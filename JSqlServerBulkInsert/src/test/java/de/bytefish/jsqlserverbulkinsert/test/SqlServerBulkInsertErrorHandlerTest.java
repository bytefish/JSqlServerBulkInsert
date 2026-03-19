package de.bytefish.jsqlserverbulkinsert.test;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static de.bytefish.jsqlserverbulkinsert.SqlServerBulkInsert.*;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SqlServerBulkInsertErrorHandlerTest {

    private static final String CONNECTION_STRING =
            "jdbc:sqlserver://localhost:14330;databaseName=master;user=sa;password=MyStrongPassw0rd;trustServerCertificate=true;";

    record ErrorEntity(UUID id, String name) {}

    private static final SqlServerMapper<ErrorEntity> MAPPER = SqlServerMapper.forClass(ErrorEntity.class)
            .map("Id", SqlServerTypes.UNIQUEIDENTIFIER.from(ErrorEntity::id))
            .map("Name", SqlServerTypes.NVARCHAR.from(ErrorEntity::name));

    @BeforeEach
    public void setupTable() throws Exception {
        try (Connection conn = DriverManager.getConnection(CONNECTION_STRING);
             Statement stmt = conn.createStatement()) {
            stmt.execute("IF OBJECT_ID('dbo.ErrorTestTable', 'U') IS NOT NULL DROP TABLE dbo.ErrorTestTable;");
            stmt.execute("CREATE TABLE dbo.ErrorTestTable (Id UNIQUEIDENTIFIER PRIMARY KEY, Name NVARCHAR(255));");
        }
    }

    @Test
    public void testErrorHandlerCapturesConstraintViolation() throws Exception {
        // Arrange: Prepare data with a duplicate ID to trigger a Primary Key violation
        UUID duplicateId = UUID.randomUUID();
        List<ErrorEntity> data = List.of(
                new ErrorEntity(duplicateId, "First"),
                new ErrorEntity(duplicateId, "Duplicate")
        );

        AtomicReference<Exception> capturedException = new AtomicReference<>();

        SqlServerBulkWriter<ErrorEntity> writer = new SqlServerBulkWriter<>(MAPPER)
                .withErrorHandler(capturedException::set);

        // Act
        try (Connection conn = DriverManager.getConnection(CONNECTION_STRING)) {
            BulkInsertResult result = writer.saveAll(conn, "dbo", "ErrorTestTable", data);

            // Assert
            assertFalse(result.success(), "Bulk insert should fail due to PK violation");
            assertNotNull(capturedException.get(), "Error handler should have captured the exception");
            assertTrue(capturedException.get().getMessage().contains("Violation of PRIMARY KEY constraint"),
                    "Exception message should indicate PK violation");
        }
    }

    @Test
    public void testErrorHandlerCapturesMissingTableError() throws Exception {
        // Arrange
        List<ErrorEntity> data = List.of(new ErrorEntity(UUID.randomUUID(), "Test"));
        AtomicReference<Exception> capturedException = new AtomicReference<>();

        SqlServerBulkWriter<ErrorEntity> writer = new SqlServerBulkWriter<>(MAPPER)
                .withErrorHandler(capturedException::set);

        // Act
        try (Connection conn = DriverManager.getConnection(CONNECTION_STRING)) {
            // Try to insert into a non-existing table
            BulkInsertResult result = writer.saveAll(conn, "dbo", "NonExistingTable", data);

            // Assert
            assertFalse(result.success());
            assertNotNull(capturedException.get());
            assertTrue(capturedException.get().getCause().getMessage().contains("Invalid object name"),
                    "Exception message should indicate missing table");
        }
    }
}