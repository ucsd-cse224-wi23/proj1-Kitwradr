package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"gopkg.in/yaml.v2"
)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

type Record struct {
	Complete byte
	Key      [10]byte
	Value    [90]byte
}

var number_of_bits int = 0
var my_server_id = 0
var wg sync.WaitGroup

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

const (
	Proto          = "tcp"
	Host           = "localhost"
	Port           = "8080"
	MaxMessageSize = 101
	MaxRetries     = 3
)

func listenForClientConnections(write_only_ch chan<- Record, server_id int, address string) {

	fmt.Println("Starting " + Proto + " server on " + address)
	listener, err := net.Listen(Proto, address)
	if err != nil {
		fmt.Println("listen error")
		os.Exit(1)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(("accept error"))
			os.Exit(1)
		}

		go handleClientConnection(conn, write_only_ch, server_id)
	}
}

func handleClientConnection(conn net.Conn, write_only_ch chan<- Record, server_id int) {

	//client_msg_buf := make([]byte, MaxMessageSize)
	// bytes_read, err := conn.Read(client_msg_buf)

	// var record Record
	// err := binary.Read(conn, binary.BigEndian, &record)
	// if err != nil {
	// 	fmt.Println("read error", err.Error())
	// 	os.Exit(1)
	// }
	// fmt.Println("Record received on server ", server_id)
	// write_only_ch <- record
	// conn.Close()

	defer conn.Close()

	for {
		var record Record
		err := binary.Read(conn, binary.BigEndian, &record)
		if err != nil {
			fmt.Println("read error", err.Error())
			os.Exit(1)
		}
		//fmt.Println("Record received on server ", server_id, record)
		write_only_ch <- record
		if record.Complete == byte(1) { // Close connection if stream is complete
			break
		}

	}

}

func readFullyFromClient(ch chan Record, records []Record, wg *sync.WaitGroup) {
	for {
		record := <-ch
		//fmt.Println("Receiving on server", my_server_id)
		if record.Complete == 1 {
			wg.Done()
			break
		} else {
			records = append(records, record)
		}
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	my_server_id = serverId
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs.Servers)

	/*
		Implement Distributed Sort
	*/

	// Establishing a mesh of connections between all servers

	number_of_servers := len(scs.Servers)
	number_of_bits = int(math.Log2(float64(number_of_servers)))
	fmt.Println("Number of bits for mask", number_of_bits)

	var self_address string
	// Listening on this server
	for _, server := range scs.Servers {
		if serverId == server.ServerId {
			self_address = server.Host + ":" + server.Port
		}
	}

	ch := make(chan Record)
	defer close(ch)

	go listenForClientConnections(ch, serverId, self_address)

	// Getting file and segregating
	file, err := os.Open(os.Args[2])

	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	records := getAllRecords(file, serverId)
	fmt.Println("Output of getallrecords:", len(records))
	self_records := segregateRecords(records, serverId, false)

	// Connecting to rest of the servers
	for _, server := range scs.Servers {

		if server.ServerId != serverId {
			wg.Add(1)
			go ConnectAndSendToServer(server.Host, server.Port, server.ServerId, serverId, records)
		}
	}

	// Read from received data from clients
	clients_done := 0

	var my_records = make([]Record, 0)
	my_records = append(my_records, self_records...)
	//var wg sync.WaitGroup
	//wg.Add(number_of_servers - 1)

	// reading from channel - the data from the rest of the clients
	for clients_done < number_of_servers-1 {
		record := <-ch
		//fmt.Println("Receiving on server", my_server_id)
		if record.Complete == 1 {
			clients_done++
			fmt.Println("Number of clients completed are", clients_done)
		} else {
			my_records = append(my_records, record)
		}

	}
	wg.Wait() // waiting for send goroutines to complete
	fmt.Println("Number of records on server", serverId, "is", len(my_records), "self records num = ", len(self_records))
	fmt.Println("Server", serverId, "is shutting down and is no more")

	sortAndWrite(my_records, os.Args[3])
}

func sortAndWrite(records []Record, filename string) {

	out_file, err := os.Create(filename)

	if err != nil {
		fmt.Println("Error opening output file:", err)
		return
	}
	defer out_file.Close()

	sorted_records := sortRecords(records)

	fmt.Println("Number of sorted records on server", my_server_id, len(sorted_records))

	for _, record := range sorted_records {
		binary.Write(out_file, binary.BigEndian, record.Key)
		binary.Write(out_file, binary.BigEndian, record.Value)
	}

}

func sortRecords(records []Record) []Record {
	sort.Slice(records, func(i, j int) bool {
		return bytes.Compare(records[i].Key[:], records[j].Key[:]) < 0
	})

	return records
}

func getAllRecords(file *os.File, server_id int) []Record {

	var records []Record
	var number_of_records int = 0
	for {
		var record Record
		record.Complete = 0

		err := binary.Read(file, binary.BigEndian, &record.Key)

		if err != nil {
			if err == io.EOF {
				// End of File reached
				break
			}
			fmt.Println("Error reading key:", err)
			return nil
		}

		err = binary.Read(file, binary.BigEndian, &record.Value)

		if err != nil {
			if err == io.EOF {
				// End of File reached
				break
			}
			fmt.Println("Error reading Value:", err)
			return nil
		}

		//fmt.Println("Key:", record.Key, "Value:", record.Value, "Server", server_id)
		number_of_records++
		records = append(records, record)

	}
	fmt.Println("Number of records in input file of server", server_id, "is", number_of_records)
	return records

}

func ConnectAndSendToServer(host string, port string, target_server_id int, server_id int, records []Record) {
	address := host + ":" + port
	fmt.Println("Connecting to server", address, "from server", server_id)
	var conn net.Conn
	var err error
	for {
		conn, err = net.Dial(Proto, address)
		if err == nil {
			// connected
			fmt.Println("Connected to server ", address, "from server", server_id)
			break
		}
		if err != nil {
			fmt.Println("Error is", err.Error(), "for server", target_server_id, "from server", server_id)
		}
		fmt.Println("Waiting for a second on", server_id, "to connect to", address)
		time.Sleep(1 * time.Second)
	}

	specific_records := segregateRecords(records, target_server_id, true)
	fmt.Println("Number of records before sending to each client from server", server_id, "to server", target_server_id, "is", len(specific_records))
	sendData(conn, server_id, specific_records)

	wg.Done()

}

func segregateRecords(records []Record, target_server_id int, appendLast bool) []Record {
	fmt.Println("Input number of records for segregateRecords function = ", len(records))
	var new_records []Record
	for _, record := range records {
		if belongsToServer(record.Key, target_server_id) {
			new_records = append(new_records, record)
		}

	}
	if appendLast {
		var lastRecord Record
		lastRecord.Complete = 1 // Marking the last record to complete the stream
		new_records = append(new_records, lastRecord)
	}

	return new_records
}

func belongsToServer(key [10]byte, target_server_id int) bool {
	var a byte = key[0]
	n := number_of_bits
	msb_bits := a >> (8 - n) & (1<<n - 1)
	return msb_bits == byte(target_server_id)
}

func sendData(conn net.Conn, server_id int, records []Record) {
	defer conn.Close()

	for _, record := range records {

		err := binary.Write(conn, binary.LittleEndian, record)

		if err != nil {
			fmt.Println("Error writing struct to connection:", err)
			return
		}

	}

}

func checkError(err error) {
	if err != nil {
		log.Fatal("Fatal error: %s", err.Error())
	}
}
