// package main

// import (
// 	"fmt"
// 	"net"
// 	"os"
// )

// const (
// 	Proto          = "tcp"
// 	Host           = "localhost"
// 	Port           = "8080"
// 	WorldSize      = 7
// 	MaxMessageSize = 100
// )

// func listenForClientConnections(write_only_ch chan<- string) {
// 	server_address := Host + ":" + Port
// 	fmt.Println("Strting " + Proto + " server on " + server_address)
// 	listener, err := net.Listen(Proto, server_address)
// 	if err != nil {
// 		fmt.Println("listen")
// 		os.Exit(1)
// 	}
// 	defer listener.Close()

// 	for {
// 		conn, err := listener.Accept()
// 		if err != nil {
// 			fmt.Println(("accept"))
// 			os.Exit(1)
// 		}

// 		go handleClientConnection(conn, write_only_ch)
// 	}
// }

// func handleClientConnection(conn net.Conn, write_only_ch chan<- string) {

// 	client_msg_buf := make([]byte, MaxMessageSize)
// 	bytes_read, err := conn.Read(client_msg_buf)
// 	if err != nil {
// 		fmt.Println("read")
// 		os.Exit(1)
// 	}
// 	message := string(client_msg_buf[0:bytes_read])
// 	write_only_ch <- message
// 	conn.Close()
// }

// func receiveDataFromChannel(read_only_ch <-chan string) []string {
// 	var client_messages []string
// 	messages_received := 0

// 	for messages_received < WorldSize {
// 		message := <-read_only_ch
// 		client_messages = append(client_messages, message)
// 		messages_received++
// 	}

// 	return client_messages

// }

// func main() {

// 	ch := make(chan string)
// 	defer close(ch)
// 	go listenForClientConnections(ch)

// 	client_messages := receiveDataFromChannel(ch)

// 	for index, message := range client_messages {
// 		fmt.Println(index, message)
// 	}
// }
