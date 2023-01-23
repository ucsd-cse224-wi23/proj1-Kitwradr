package main

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
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

	var record Record
	err := binary.Read(conn, binary.BigEndian, &record)
	if err != nil {
		fmt.Println("read error")
		os.Exit(1)
	}
	fmt.Println("Record received on server ", server_id)
	write_only_ch <- record
	conn.Close()
}

func readFullyFromClient(ch chan Record, records []Record, wg *sync.WaitGroup) {
	for {
		record := <-ch
		fmt.Println("Receiving on")
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

	number_of_bits := math.Log2(float64(number_of_servers))

	fmt.Println("Number of bits for mask", number_of_bits)

	ch := make(chan Record)
	defer close(ch)

	var self_address string
	// Listening on this server
	for _, server := range scs.Servers {
		if serverId == server.ServerId {
			self_address = server.Host + ":" + server.Port
			fmt.Println("My server address is : ", self_address)
		}

	}

	go listenForClientConnections(ch, serverId, self_address)

	// Getting file and segregating
	file, err := os.Open(os.Args[2])

	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// Connecting to rest of the servers
	for _, server := range scs.Servers {
		//fmt.Println("Server configs", index, server)
		//fmt.Println(serverId, server.ServerId, serverId != server.ServerId)
		if server.ServerId != serverId {
			go ConnectToServer(server.Host, server.Port, serverId, file)
		}
	}
	messages_received := 0

	var my_records []Record
	var wg sync.WaitGroup
	wg.Add(number_of_servers - 1)

	for messages_received < number_of_servers {
		// message := <-ch
		// fmt.Println("Received message", message, "on server", serverId)

		go readFullyFromClient(ch, my_records, &wg)
		messages_received++

	}
	wg.Wait()
	fmt.Println("Number of records on server", serverId, "is", len(my_records))

}

func ConnectToServer(host string, port string, server_id int, file os.File) {
	address := host + ":" + port
	//fmt.Println("Connecting to server ", address, "from server", server_id)
	var conn net.Conn
	var err error
	for {
		conn, err = net.Dial(Proto, address)
		if err == nil {
			// connected
			fmt.Println("Connected to server ", address, "from server", server_id)
			break
		}
		fmt.Println("Waiting for a second on ", server_id, "to connect to", address)
		time.Sleep(1 * time.Second)
	}

	sendData(conn, server_id, file)

}

func sendData(conn net.Conn, server_id int, file os.File) {
	defer conn.Close()
	var message string
	if server_id == 1 {
		message = "from server 1"
	} else if server_id == 2 {
		message = "from server 2"
	} else if server_id == 3 {
		message = "from server 3"
	} else {
		message = "from server 4"
	}

	_, write_err := conn.Write([]byte(message))

	if write_err != nil {
		fmt.Println("write failed:", write_err)
		return
	}

}

func checkError(err error) {
	if err != nil {
		log.Fatal("Fatal error: %s", err.Error())
	}
}
