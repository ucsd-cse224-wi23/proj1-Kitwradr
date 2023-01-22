package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"strconv"
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
	WorldSize      = 7
	MaxMessageSize = 101
	MaxRetries     = 3
)

func listenForClientConnections(write_only_ch chan<- string, server_id int, address string) {
	//server_address := Host + ":" + Port
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

func handleClientConnection(conn net.Conn, write_only_ch chan<- string, server_id int) {

	client_msg_buf := make([]byte, MaxMessageSize)
	bytes_read, err := conn.Read(client_msg_buf)
	if err != nil {
		fmt.Println("read error")
		os.Exit(1)
	}
	message := string(client_msg_buf[0:bytes_read])
	fmt.Println("My server ID", server_id, "message received", message)
	write_only_ch <- message
	conn.Close()
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

	ch := make(chan string)
	defer close(ch)

	var self_address string
	for _, server := range scs.Servers {
		if serverId == server.ServerId {
			self_address = server.Host + ":" + server.Port
			fmt.Println("My server address is : ", self_address)
		}

	}

	go listenForClientConnections(ch, serverId, self_address)

	for _, server := range scs.Servers {
		//fmt.Println("Server configs", index, server)
		if server.ServerId != serverId {
			go ConnectToServer(server.Host, server.Port, server.ServerId)
		}
	}

}

func ConnectToServer(host string, port string, server_id int) {
	address := host + ":" + port
	fmt.Println(" Connecting to server ", address, "from server", server_id)
	var conn net.Conn
	var err error
	for {
		conn, err = net.Dial(Proto, address)
		if err == nil {
			// connected
			break
		}
		fmt.Println("Waiting for a second on server", server_id)
		time.Sleep(1 * time.Second)
	}

	sendData(conn, server_id)

}

func sendData(conn net.Conn, server_id int) {
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
