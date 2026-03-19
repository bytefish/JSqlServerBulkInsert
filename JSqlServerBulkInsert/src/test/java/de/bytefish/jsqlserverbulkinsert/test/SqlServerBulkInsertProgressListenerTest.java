package de.bytefish.jsqlserverbulkinsert.test;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static de.bytefish.jsqlserverbulkinsert.SqlServerBulkInsert.*;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SqlServerBulkInsertProgressListenerTest {

    private static final String CONNECTION_STRING =
            "jdbc:sqlserver://localhost:14330;databaseName=master;user=sa;password=MyStrongPassw0rd;trustServerCertificate=true;";

    record SimpleEntity(UUID id, String name) {}

    private static final SqlServerMapper<SimpleEntity> MAPPER = SqlServerMapper.forClass(SimpleEntity.class)
            .map("Id", SqlServerTypes.UNIQUEIDENTIFIER.from(SimpleEntity::id))
            .map("Name", SqlServerTypes.NVARCHAR.from(SimpleEntity::name));

    @BeforeEach
    public void setupTable() throws Exception {
        try (Connection conn = DriverManager.getConnection(CONNECTION_STRING);
             Statement stmt = conn.createStatement()) {
            stmt.execute("IF OBJECT_ID('dbo.ProgressTestTable', 'U') IS NOT NULL DROP TABLE dbo.ProgressTestTable;");
            stmt.execute("CREATE TABLE dbo.ProgressTestTable (Id UNIQUEIDENTIFIER PRIMARY KEY, Name NVARCHAR(255));");
        }
    }

    @Test
    public void testProgressListenerIsCalledCorrectly() throws Exception {
        // Arrange
        int totalRecords = 100;
        int notifyEvery = 10;
        List<SimpleEntity> data = new ArrayList<>();
        for (int i = 0; i < totalRecords; i++) {
            data.add(new SimpleEntity(UUID.randomUUID(), "Entry " + i));
        }

        AtomicInteger callCount = new AtomicInteger(0);
        AtomicLong lastProcessedCount = new AtomicLong(0);

        SqlServerBulkWriter<SimpleEntity> writer = new SqlServerBulkWriter<>(MAPPER)
                .withNotifyAfter(notifyEvery, rowsProcessed -> {
                    callCount.incrementAndGet();
                    lastProcessedCount.set(rowsProcessed);
                });

        // Act
        try (Connection conn = DriverManager.getConnection(CONNECTION_STRING)) {
            BulkInsertResult result = writer.saveAll(conn, "dbo", "ProgressTestTable", data);

            // Assert
            assertTrue(result.success());
            // Progress should be notified 10 times (100 / 10)
            assertEquals(totalRecords / notifyEvery, callCount.get(), "Listener should be called exactly 10 times");
            assertEquals(totalRecords, lastProcessedCount.get(), "Last notification should report total record count");
        }
    }
}