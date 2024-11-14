package com.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;

import java.sql.*;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Function {
    @FunctionName("InsertStudentData")
    public HttpResponseMessage run(
        @HttpTrigger(
            name = "req",
            methods = {HttpMethod.POST},
            authLevel = AuthorizationLevel.FUNCTION
        ) HttpRequestMessage<Optional<String>> request,  // Accepts the body as a String
        final ExecutionContext context) {

        context.getLogger().info("Processing a POST request to insert data into MySQL.");

        // Parse request body
        String requestBodyString = request.getBody().orElse("");
        if (requestBodyString.isEmpty()) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                .body("Request body is missing or invalid.")
                .build();
        }

        Map<String, Object> requestBody;
        try {
            // Convert JSON string to Map
            ObjectMapper objectMapper = new ObjectMapper();
            requestBody = objectMapper.readValue(requestBodyString, Map.class);
        } catch (Exception e) {
            context.getLogger().severe("Error parsing JSON: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                .body("Invalid JSON format.")
                .build();
        }

        // Validate fields
        if (!requestBody.containsKey("id") || !requestBody.containsKey("FirstName") ||
            !requestBody.containsKey("LastName") || !requestBody.containsKey("DateOfBirth") ||
            !requestBody.containsKey("Gender")) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                .body("Missing required fields: id, FirstName, LastName, DateOfBirth, Gender.")
                .build();
        }

        // Extract fields from request body
        int id = Integer.parseInt(requestBody.get("id").toString());
        String firstName = requestBody.get("FirstName").toString();
        String lastName = requestBody.get("LastName").toString();
        String dateOfBirth = requestBody.get("DateOfBirth").toString();
        String gender = requestBody.get("Gender").toString();

        Date dob = null;
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");  // Expected format "yyyy-MM-dd"
            java.util.Date utilDate = dateFormat.parse(dateOfBirth);
            dob = new Date(utilDate.getTime());
        } catch (ParseException e) {
            context.getLogger().severe("Invalid DateOfBirth format: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                .body("Invalid DateOfBirth format. Expected format: yyyy-MM-dd.")
                .build();
        }

        // Process further (e.g., inserting into MySQL)
        String jdbcurl = "jdbc:mysql://sql-sravan-01.mysql.database.azure.com:3306/azure_demos?useSSL=true";
        String userName = "Sravan";
        String password = "Sravan@7";
        try (Connection connection = DriverManager.getConnection(jdbcurl, userName, password)) {
            String insertQuery = "INSERT INTO students (id, first_name, last_name, dob, gender) VALUES(?,?,?,?,?);";
            try (PreparedStatement stmt = connection.prepareStatement(insertQuery)) {
                stmt.setInt(1, id);
                stmt.setString(2, firstName);
                stmt.setString(3, lastName);
                stmt.setDate(4, dob);
                stmt.setString(5, gender);
                int rowsInserted = stmt.executeUpdate();
                if (rowsInserted > 0) {
                    return request.createResponseBuilder(HttpStatus.OK)
                        .body("Data inserted successfully.")
                        .build();
                } else {
                    return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body("Failed to insert data.")
                        .build();
                }
            }
        } catch (SQLException e) {
            context.getLogger().severe("Database error: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("Database connection failed: " + e.getMessage())
                .build();
        }
    }
}
