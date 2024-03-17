#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <sstream>
#include <thread>
#include <mutex>
#include <chrono>
#include <random>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

using namespace std;

const int BASE_PORT = 20000; // Base port number for server-server communication
const size_t MSG_SIZE = 1024; // Maximum message size
const int MAX_PEERS = 4;

// Message struct for storing chat messages
struct Message {
    string origin;
    int seqNo;
    string text;
};

// Global data structures
unordered_map<string, vector<Message>> chatLogs; // Chat logs separated by origin
unordered_map<string, int> maxSeqNos; // Highest sequence number seen for each origin
mutex logsMutex; // Mutex for synchronizing access to chatLogs and maxSeqNos

// Function declarations
void processProxyCommand(int sock, const string& command);
void processPeerMessage(int sock, const string& message, const string& messageType);
void forwardMessage(const Message& msg);
void respondToStatus(int sock, const string& statusMessage);
string compileStatusMessage();
void handleConnection(int sock);
void antiEntropy();
void sendMessage(int destPort, const string& msg);
void startServer(int port);


int main(int argc, char* argv[]) {
    if (argc != 4) {
        cerr << "Usage: " << argv[0] << " <processID> <nProcesses> <portNo>" << endl;
        return 1;
    }

    int portNo = stoi(argv[3]);
    // Start anti-entropy process
    thread antiEntropyThread(antiEntropy);
    antiEntropyThread.detach();
    
    // Start server
    startServer(portNo);

    return 0;
}

void startServer(int port) {
    int serverSock = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSock < 0) {
        cerr << "Error creating socket." << endl;
        exit(1);
    }

    sockaddr_in serverAddr{};
    memset(&serverAddr, 0, sizeof(serverAddr)); // Ensure struct is empty
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(port);
    serverAddr.sin_addr.s_addr = INADDR_ANY;

    // Use ::bind to specify the global namespace, avoiding ambiguity with std::bind
    if (::bind(serverSock, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        cerr << "Error binding socket." << endl;
        close(serverSock);
        exit(1);
    }

    if (listen(serverSock, 10) < 0) { // Listen for incoming connections, maximum 10 pending connections
        cerr << "Error listening on socket." << endl;
        close(serverSock);
        exit(1);
    }

    cout << "Server started on port " << port << endl;

    while (true) {
        sockaddr_in clientAddr{};
        socklen_t clientAddrSize = sizeof(clientAddr);
        int clientSock = accept(serverSock, (struct sockaddr*)&clientAddr, &clientAddrSize);
        if (clientSock < 0) {
            cerr << "Error accepting connection." << endl;
            continue;
        }
        thread(handleConnection, clientSock).detach(); // Start a new thread to handle the connection
    }
}

void handleConnection(int sock) {
    char buffer[MSG_SIZE];
    memset(buffer, 0, MSG_SIZE); // Initialize the buffer to zero

    // Attempt to receive data from the socket
    ssize_t bytesReceived = recv(sock, buffer, MSG_SIZE - 1, 0);
    if (bytesReceived <= 0) {
        cerr << "Error reading from socket or connection closed." << endl;
        close(sock); // Close the socket if an error occurs or if the connection is closed
        return;
    }

    // Convert the received data into a string for easier processing
    string receivedData(buffer, bytesReceived);

    // Inside handleConnection
    if (receivedData.find("get chatLog") == 0) {
        processProxyCommand(sock, "get chatLog");
    } else if (receivedData.find("crash") == 0) {
        processProxyCommand(sock, "crash");
    } else if (receivedData.find("RUMOR") == 0) {
        processPeerMessage(sock, receivedData, "RUMOR");
    } else if (receivedData.find("STATUS") == 0) {
        processPeerMessage(sock, receivedData, "STATUS");
    }


    close(sock); // Close the socket after processing the command or message
}

void processProxyCommand(int sock, const string& command) {
    if (command == "get chatLog") {
        lock_guard<mutex> guard(logsMutex);
        stringstream chatLogStream;
        for (const auto& [origin, messages] : chatLogs) {
            for (const auto& message : messages) {
                chatLogStream << message.origin << ":" << message.seqNo << ":" << message.text << ",";
            }
        }
        string chatLog = chatLogStream.str();
        // Send back to proxy
        send(sock, chatLog.c_str(), chatLog.length(), 0);
    } else if (command == "crash") {
        exit(0);
    }
}

void processPeerMessage(int sock, const string& message, const string& messageType) {
    // Lock mutex for thread safety
    lock_guard<mutex> guard(logsMutex);

    if (messageType == "RUMOR") {
        stringstream ss(message);
        string dummy, origin, text;
        int seqNo;
        ss >> dummy >> origin >> seqNo;
        getline(ss, text);
        text = text.substr(1); // Remove leading space

        // Check if this is a new message
        if (maxSeqNos[origin] < seqNo) {
            // Update chat log and max sequence number
            chatLogs[origin].push_back({origin, seqNo, text});
            maxSeqNos[origin] = seqNo;

            // Forward the message to another peer
            forwardMessage({origin, seqNo, text});
        }
    } else if (messageType == "STATUS") {
        // Process the status message
        // Respond with any messages the sender is missing
        respondToStatus(sock, message);
    }
}

void forwardMessage(const Message& msg) {
    // Select a random peer other than the message's origin
    int peerIndex = rand() % MAX_PEERS;
    int destPort = BASE_PORT + peerIndex;

    // Convert the message to string format
    string messageString = "RUMOR " + msg.origin + " " + to_string(msg.seqNo) + " " + msg.text;
    
    // Send the message
    sendMessage(destPort, messageString);
}

void respondToStatus(int sock, const string& statusMessage) {
    // Parse the status message to determine what the peer is missing
    // Then, compile and send those messages
    // This function would involve comparing received status with your chatLogs and maxSeqNos
}

void antiEntropy() {
    while (true) {
        this_thread::sleep_for(chrono::seconds(10)); // Adjust timing as needed

        int peerIndex = rand() % MAX_PEERS;
        int destPort = BASE_PORT + peerIndex;

        // Compile a status message
        string statusMessage = compileStatusMessage();

        // Send the status message to a randomly selected peer
        sendMessage(destPort, statusMessage);
    }
}

string compileStatusMessage() {
    lock_guard<mutex> guard(logsMutex);
    string status = "STATUS";
    for (const auto& [origin, seqNo] : maxSeqNos) {
        status += " " + origin + ":" + to_string(seqNo);
    }
    return status;
}

void sendMessage(int destPort, const string& msg) {
    // Create a socket
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        cerr << "Error creating socket for sending message." << endl;
        return;
    }

    // Set up the destination address structure
    sockaddr_in destAddr{};
    destAddr.sin_family = AF_INET;
    destAddr.sin_port = htons(destPort);
    inet_pton(AF_INET, "127.0.0.1", &destAddr.sin_addr);

    // Connect to the destination
    if (connect(sock, (sockaddr*)&destAddr, sizeof(destAddr)) < 0) {
        cerr << "Error connecting to destination port: " << destPort << endl;
        close(sock); // Close the socket if unable to connect
        return;
    }

    // Send the message
    if (send(sock, msg.c_str(), msg.length(), 0) < 0) {
        cerr << "Error sending message to port: " << destPort << endl;
    }

    // Close the socket after sending the message
    close(sock);
}