package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Rating struct {
	UserID  string  // ID del usuario
	MovieID string  // ID de la película
	Rating  float64 // Calificación
}

type ClientData struct {
	UserID string   // UserID del usuario objetivo
	Data   []Rating // Array con las calificaciones del cliente
}

var aggregatedRecommendations = make(map[string]float64)
var mu sync.Mutex

// Maneja la conexión de cada cliente
func handleClient(con net.Conn, clientData ClientData, wg *sync.WaitGroup) {
	defer wg.Done()
	defer con.Close()

	log.Printf("Cliente conectado: %s", con.RemoteAddr())

	// Enviar el UserID y el subconjunto de datos al cliente
	_, err := fmt.Fprintf(con, "UserID: %s\n", clientData.UserID)
	if err != nil {
		log.Println("Error al enviar el UserID:", err)
		return
	}

	for _, rating := range clientData.Data {
		_, err := fmt.Fprintf(con, "%s,%s,%f\n", rating.UserID, rating.MovieID, rating.Rating)
		if err != nil {
			log.Println("Error al enviar la calificación:", err)
			return
		}
	}

	// Enviar señal de fin de datos
	fmt.Fprintln(con, "END")

	// Recibir recomendaciones parciales del cliente
	scanner := bufio.NewScanner(con)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "END" {
			break
		}
		parts := strings.Split(line, " ")
		if len(parts) != 2 {
			continue
		}
		movieID := parts[0]
		score, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}

		mu.Lock()
		aggregatedRecommendations[movieID] += score
		mu.Unlock()

		log.Printf("Recibido del cliente %s: Película %s, Score %.2f", clientData.UserID, movieID, score)
	}

	if err := scanner.Err(); err != nil {
		log.Println("Error al leer datos del cliente:", err)
	}

	log.Printf("Cliente %s desconectado", clientData.UserID)
}

// Carga el conjunto de datos
func loadDataset(filename string, limit int) (map[string][]Rating, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	userRatings := make(map[string][]Rating)
	if limit <= 0 || limit > len(records)-1 {
		limit = len(records) - 1
	}

	for _, record := range records[1 : limit+1] {
		movieID := record[0]
		userID := record[1]
		rating, _ := strconv.ParseFloat(record[2], 64)

		userRatings[userID] = append(userRatings[userID], Rating{
			UserID:  userID,
			MovieID: movieID,
			Rating:  rating,
		})
	}
	return userRatings, nil
}

// Divide el dataset en partes para los clientes
func splitDataset(userRatings map[string][]Rating, targetUserRatings []Rating, numClients int) ([]ClientData, int) {
	clientData := make([]ClientData, numClients)
	targetCount := len(targetUserRatings)

	for i := 0; i < numClients; i++ {
		clientData[i].UserID = targetUserRatings[0].UserID
		clientData[i].Data = append(clientData[i].Data, targetUserRatings...)
	}

	delete(userRatings, targetUserRatings[0].UserID)

	i := 0
	for _, ratings := range userRatings {
		for _, rating := range ratings {
			clientData[i%numClients].Data = append(clientData[i%numClients].Data, rating)
			i++
		}
	}

	return clientData, targetCount
}

// Función principal
func main() {
	var numClients int
	var targetUserID string

	fmt.Print("Ingrese el número de nodos clientes: ")
	fmt.Scan(&numClients)
	fmt.Print("Ingrese el UserID del usuario objetivo: ")
	fmt.Scan(&targetUserID)

	userRatings, err := loadDataset("dataset.csv", 200000)
	if err != nil {
		log.Fatalf("Error cargando el dataset: %v", err)
	}

	targetUserRatings, exists := userRatings[targetUserID]
	if !exists {
		log.Fatalf("El UserID %s no existe en el dataset", targetUserID)
	}

	fmt.Printf("Total de registros leídos: %d\n", len(userRatings))

	clientData, targetCount := splitDataset(userRatings, targetUserRatings, numClients)

	fmt.Printf("Se encontraron %d elementos para el usuario objetivo %s\n", targetCount, targetUserID)

	// Mostrar los primeros 5 valores para cada cliente
	for i, data := range clientData {
		fmt.Printf("\nPrimeros 5 valores del cliente %d:\n", i+1)
		for j, rating := range data.Data[:5] {
			fmt.Printf("  %d. UserID: %s, MovieID: %s, Rating: %.2f\n", j+1, rating.UserID, rating.MovieID, rating.Rating)
		}
	}

	ln, err := net.Listen("tcp", "localhost:15000")
	if err != nil {
		log.Fatalf("Error iniciando el servidor: %v", err)
	}
	defer ln.Close()

	var wg sync.WaitGroup

	fmt.Printf("Servidor en espera de %d conexiones de clientes...\n", numClients)
	for i := 0; i < numClients; i++ {
		con, err := ln.Accept()
		if err != nil {
			log.Println("Error aceptando conexión:", err)
			continue
		}

		wg.Add(1)
		go handleClient(con, clientData[i], &wg)
	}

	// Esperar a que los clientes terminen
	wg.Wait()

	// Mostrar los resultados después de recibir las predicciones
	fmt.Println("\nRecibiendo respuestas de los nodos clientes...")
	for movieID, score := range aggregatedRecommendations {
		fmt.Printf("Película: %s, Recomendación parcial: %.2f\n", movieID, score)
	}

	// Mostrar las 3 recomendaciones finales
	fmt.Println("\nMostrando las 3 mejores recomendaciones:")
	type Recommendation struct {
		MovieID string
		Score   float64
	}

	var recommendations []Recommendation
	for movieID, score := range aggregatedRecommendations {
		recommendations = append(recommendations, Recommendation{MovieID: movieID, Score: score})
	}

	// Ordenar las recomendaciones por puntaje en orden descendente
	sort.Slice(recommendations, func(i, j int) bool {
		return recommendations[i].Score > recommendations[j].Score
	})

	// Mostrar las 3 recomendaciones principales
	for i := 0; i < 3 && i < len(recommendations); i++ {
		fmt.Printf("Recomendación %d: Película: %s, Score: %.2f\n", i+1, recommendations[i].MovieID, recommendations[i].Score)
	}
}
