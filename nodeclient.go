package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
)

type Rating struct {
	UserID  string
	MovieID string
	Rating  float64
}

type ClientData struct {
	TargetUserID string
	Data         []Rating
}

func createUserItemMatrix(clientData ClientData) map[string]map[string]float64 {
	matrix := make(map[string]map[string]float64)
	for _, rating := range clientData.Data {
		if _, exists := matrix[rating.UserID]; !exists {
			matrix[rating.UserID] = make(map[string]float64)
		}
		matrix[rating.UserID][rating.MovieID] = rating.Rating
	}
	return matrix
}

func matrixFactorization(matrix map[string]map[string]float64, numFactors int) (map[string][]float64, map[string][]float64) {
	userFactors := make(map[string][]float64)
	itemFactors := make(map[string][]float64)

	// Inicializar factores aleatorios
	for userID := range matrix {
		factors := make([]float64, numFactors)
		for i := 0; i < numFactors; i++ {
			factors[i] = rand.Float64()
		}
		userFactors[userID] = factors
	}

	itemSet := make(map[string]bool)
	for _, movies := range matrix {
		for movieID := range movies {
			itemSet[movieID] = true
		}
	}

	for movieID := range itemSet {
		factors := make([]float64, numFactors)
		for i := 0; i < numFactors; i++ {
			factors[i] = rand.Float64()
		}
		itemFactors[movieID] = factors
	}

	// Aquí podrías implementar un algoritmo de factorización como SGD para ajustar los factores.
	// Por simplicidad, estamos usando factores aleatorios.

	return userFactors, itemFactors
}

func predictRating(userFactors, itemFactors []float64) float64 {
	var predictedRating float64
	for i := 0; i < len(userFactors); i++ {
		predictedRating += userFactors[i] * itemFactors[i]
	}
	return predictedRating
}

func calculateRecommendations(targetUserID string, matrix map[string]map[string]float64, userFactors map[string][]float64, itemFactors map[string][]float64) map[string]float64 {
	fmt.Println("\nCalculando recomendaciones basadas en la factorización de matrices...")

	recommendations := make(map[string]float64)

	// Obtener las películas que el usuario objetivo ya ha visto
	ratedMovies := matrix[targetUserID]

	// Predecir calificaciones para todas las películas no vistas
	for movieID, itemFactor := range itemFactors {
		if _, exists := ratedMovies[movieID]; !exists {
			predictedRating := predictRating(userFactors[targetUserID], itemFactor)

			// Limitar el valor de la predicción al rango [1, 5]
			if predictedRating < 1.0 {
				predictedRating = 1.0
			} else if predictedRating > 5.0 {
				predictedRating = 5.0
			}

			recommendations[movieID] = predictedRating
		}
	}

	// Mostrar las recomendaciones calculadas
	fmt.Println("\nRecomendaciones para el usuario", targetUserID)
	for movieID, score := range recommendations {
		fmt.Printf("Película: %s, Puntuación predicha: %.2f\n", movieID, score)
	}

	return recommendations
}

func main() {
	conn, err := net.Dial("tcp", "localhost:15000")
	if err != nil {
		fmt.Println("Error de conexión:", err)
		return
	}
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	var clientData ClientData
	clientData.Data = make([]Rating, 0)

	// Recibir y procesar la data del servidor
	fmt.Println("Recibiendo datos del servidor...")
	for scanner.Scan() {
		line := scanner.Text()
		if line == "END" {
			break
		}
		if strings.HasPrefix(line, "UserID:") {
			clientData.TargetUserID = strings.TrimSpace(line[len("UserID:"):])
		} else {
			fields := strings.Split(line, ",")
			if len(fields) == 3 {
				rating, _ := strconv.ParseFloat(fields[2], 64)
				clientData.Data = append(clientData.Data, Rating{
					UserID:  fields[0],
					MovieID: fields[1],
					Rating:  rating,
				})
			}
		}
	}

	// Confirmar que la data fue recibida correctamente
	fmt.Println("\nData recibida del servidor:")
	fmt.Printf("UserID objetivo: %s\n", clientData.TargetUserID)

	// Crear la matriz usuario-item
	userItemMatrix := createUserItemMatrix(clientData)

	// Realizar la factorización de la matriz
	fmt.Println("\nRealizando la factorización de la matriz...")
	userFactors, itemFactors := matrixFactorization(userItemMatrix, 3)

	// Calcular y obtener las recomendaciones
	recommendations := calculateRecommendations(clientData.TargetUserID, userItemMatrix, userFactors, itemFactors)

	// Enviar las recomendaciones al servidor
	fmt.Println("\nEnviando las recomendaciones al servidor...")
	writer := bufio.NewWriter(conn)
	for movieID, score := range recommendations {
		_, err := fmt.Fprintf(writer, "%s %f\n", movieID, score)
		if err != nil {
			fmt.Println("Error al enviar los datos al servidor:", err)
			break
		}
	}
	// Enviar señal de fin de datos
	fmt.Fprintln(writer, "END")
	writer.Flush()

	// Confirmar que las predicciones parciales fueron enviadas al servidor
	fmt.Println("\nLas predicciones parciales fueron enviadas al servidor.")
}
