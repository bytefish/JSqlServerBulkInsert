package de.bytefish.jsqlserverbulkinsert.test;

import org.junit.jupiter.api.*;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static de.bytefish.jsqlserverbulkinsert.SqlServerBulkInsert.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SqlServerBulkInsertAllTypesTest {

    private static final String CONNECTION_STRING =
            "jdbc:sqlserver://localhost:1433;databaseName=master;user=sa;password=YourStrong!Passw0rd;encrypt=true;trustServerCertificate=true;";

    record AllTypesEntity(
            UUID id,

            // Primitive & Numbers
            boolean bitVal,
            short tinyIntVal,
            short smallIntVal,
            int intVal,
            long bigIntVal,
            float realVal,
            double floatVal,

            // Decimals & Money
            BigDecimal numericVal,
            BigDecimal decimalVal,
            BigDecimal moneyVal,
            BigDecimal smallMoneyVal,

            // Strings
            String charVal,
            String varCharVal,
            String nCharVal,
            String nVarCharVal,
            String varCharMaxVal,
            String nVarCharMaxVal,

            // Binaries
            byte[] varBinaryVal,
            byte[] varBinaryMaxVal,

            // Dates & Times
            LocalDate dateVal,
            LocalTime timeVal,
            LocalDateTime dateTimeVal,
            LocalDateTime dateTime2Val,
            LocalDateTime smallDateTimeVal,
            OffsetDateTime dateTimeOffsetVal
    ) {}

    // =========================================================================
    // 2. MAPPER KONFIGURATION FÜR ALLE TYPEN
    // =========================================================================
    private static final SqlServerMapper<AllTypesEntity> MAPPER = SqlServerMapper.forClass(AllTypesEntity.class)
            .map("Id", SqlServerTypes.UNIQUEIDENTIFIER.from(AllTypesEntity::id))

            // Primitives
            .map("BitVal", SqlServerTypes.BIT.primitive(AllTypesEntity::bitVal))
            .map("TinyIntVal", SqlServerTypes.TINYINT.primitive(AllTypesEntity::tinyIntVal))
            .map("SmallIntVal", SqlServerTypes.SMALLINT.primitive(AllTypesEntity::smallIntVal))
            .map("IntVal", SqlServerTypes.INT.primitive(AllTypesEntity::intVal))
            .map("BigIntVal", SqlServerTypes.BIGINT.primitive(AllTypesEntity::bigIntVal))
            .map("RealVal", SqlServerTypes.REAL.primitive(AllTypesEntity::realVal))
            .map("FloatVal", SqlServerTypes.FLOAT.primitive(AllTypesEntity::floatVal))

            // Decimals
            .map("NumericVal", SqlServerTypes.NUMERIC.numeric(10, 5).from(AllTypesEntity::numericVal))
            .map("DecimalVal", SqlServerTypes.DECIMAL.decimal(10, 5).from(AllTypesEntity::decimalVal))
            .map("MoneyVal", SqlServerTypes.MONEY.from(AllTypesEntity::moneyVal))
            .map("SmallMoneyVal", SqlServerTypes.SMALLMONEY.from(AllTypesEntity::smallMoneyVal))

            // Strings
            .map("CharVal", SqlServerTypes.CHAR.from(AllTypesEntity::charVal))
            .map("VarCharVal", SqlServerTypes.VARCHAR.from(AllTypesEntity::varCharVal))
            .map("NCharVal", SqlServerTypes.NCHAR.from(AllTypesEntity::nCharVal))
            .map("NVarCharVal", SqlServerTypes.NVARCHAR.from(AllTypesEntity::nVarCharVal))
            .map("VarCharMaxVal", SqlServerTypes.VARCHAR.max().from(AllTypesEntity::varCharMaxVal))
            .map("NVarCharMaxVal", SqlServerTypes.NVARCHAR.max().from(AllTypesEntity::nVarCharMaxVal))

            // Binaries
            .map("VarBinaryVal", SqlServerTypes.VARBINARY.from(AllTypesEntity::varBinaryVal))
            .map("VarBinaryMaxVal", SqlServerTypes.VARBINARY.max().from(AllTypesEntity::varBinaryMaxVal))

            // Dates & Times
            .map("DateVal", SqlServerTypes.DATE.localDate(AllTypesEntity::dateVal))
            .map("TimeVal", SqlServerTypes.TIME.localTime(AllTypesEntity::timeVal))
            .map("DateTimeVal", SqlServerTypes.DATETIME.localDateTime(AllTypesEntity::dateTimeVal))
            .map("DateTime2Val", SqlServerTypes.DATETIME2.localDateTime(AllTypesEntity::dateTime2Val))
            .map("SmallDateTimeVal", SqlServerTypes.SMALLDATETIME.localDateTime(AllTypesEntity::smallDateTimeVal))
            .map("DateTimeOffsetVal", SqlServerTypes.DATETIMEOFFSET.offsetDateTime(AllTypesEntity::dateTimeOffsetVal));

    @BeforeEach
    public void setupTable() throws Exception {
        try (Connection conn = DriverManager.getConnection(CONNECTION_STRING);
             Statement stmt = conn.createStatement()) {

            stmt.execute("IF OBJECT_ID('dbo.AllTypesTable', 'U') IS NOT NULL DROP TABLE dbo.AllTypesTable;");
            stmt.execute("""
                CREATE TABLE dbo.AllTypesTable (
                    Id UNIQUEIDENTIFIER PRIMARY KEY,
                    
                    BitVal BIT,
                    TinyIntVal TINYINT,
                    SmallIntVal SMALLINT,
                    IntVal INT,
                    BigIntVal BIGINT,
                    RealVal REAL,
                    FloatVal FLOAT,
                    
                    NumericVal NUMERIC(10, 5),
                    DecimalVal DECIMAL(10, 5),
                    MoneyVal MONEY,
                    SmallMoneyVal SMALLMONEY,
                    
                    CharVal CHAR(10),
                    VarCharVal VARCHAR(50),
                    NCharVal NCHAR(10),
                    NVarCharVal NVARCHAR(50),
                    VarCharMaxVal VARCHAR(MAX),
                    NVarCharMaxVal NVARCHAR(MAX),
                    
                    VarBinaryVal VARBINARY(50),
                    VarBinaryMaxVal VARBINARY(MAX),
                    
                    DateVal DATE,
                    TimeVal TIME,
                    DateTimeVal DATETIME,
                    DateTime2Val DATETIME2,
                    SmallDateTimeVal SMALLDATETIME,
                    DateTimeOffsetVal DATETIMEOFFSET
                );
            """);
        }
    }

    @AfterAll
    public void cleanup() throws Exception {
        try (Connection conn = DriverManager.getConnection(CONNECTION_STRING);
             Statement stmt = conn.createStatement()) {
            stmt.execute("IF OBJECT_ID('dbo.AllTypesTable', 'U') IS NOT NULL DROP TABLE dbo.AllTypesTable;");
        }
    }

    // =========================================================================
    // 4. DER EIGENTLICHE TEST
    // =========================================================================
    @Test
    public void testSuccessfulBulkInsertOfAllSupportedTypes() throws Exception {
        // Arrange
        List<AllTypesEntity> testData = new ArrayList<>();
        UUID testId = UUID.randomUUID();

        // Konstante Zeiten für exakte Vergleiche (Sekunden/Minuten abgeschnitten wg. SmallDateTime/DateTime Limitierungen)
        LocalDate date = LocalDate.of(2026, 3, 18);
        LocalTime time = LocalTime.of(13, 15, 0);
        LocalDateTime dateTime = LocalDateTime.of(date, time);
        OffsetDateTime offsetDateTime = OffsetDateTime.of(dateTime, ZoneOffset.ofHours(2));

        String maxString = "MAX".repeat(2000); // 6000 Zeichen, testet LOB Verhalten
        byte[] binaryData = new byte[] { 0x01, 0x02, 0x03 };

        testData.add(new AllTypesEntity(
                testId,
                true,
                (short) 255,
                (short) 32767,
                2147483647,
                9223372036854775807L,
                3.14f,
                2.718281828459,
                new BigDecimal("12345.67890"),
                new BigDecimal("98765.43210"),
                new BigDecimal("123.4567"), // Money max 4 decimals
                new BigDecimal("12.3456"),  // SmallMoney max 4 decimals
                "ABC       ", // Char füllt mit Spaces auf
                "Varchar Test",
                "XYZ       ", // NChar füllt auf
                "NVarchar Unicode: 🚀",
                maxString,
                maxString,
                binaryData,
                binaryData,
                date,
                time,
                dateTime,
                dateTime,
                dateTime,
                offsetDateTime
        ));

        SqlServerBulkWriter<AllTypesEntity> writer = new SqlServerBulkWriter<>(MAPPER)
                .withBatchSize(1000)
                .withTableLock(true);

        // Act
        try (Connection conn = DriverManager.getConnection(CONNECTION_STRING)) {

            BulkInsertResult result = writer.saveAll(conn, "dbo", "AllTypesTable", testData);

            // Assert
            assertTrue(result.success(), "Bulk Insert sollte fehlerfrei durchlaufen.");
            assertEquals(1, result.rowsAffected(), "Es sollte exakt eine Zeile geschrieben werden.");

            // Verify Data via Standard JDBC
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM dbo.AllTypesTable WHERE Id = '" + testId + "'")) {

                assertTrue(rs.next(), "Der Datensatz muss in der DB vorhanden sein.");

                assertEquals(true, rs.getBoolean("BitVal"));
                assertEquals((short) 255, rs.getShort("TinyIntVal"));
                assertEquals((short) 32767, rs.getShort("SmallIntVal"));
                assertEquals(2147483647, rs.getInt("IntVal"));
                assertEquals(9223372036854775807L, rs.getLong("BigIntVal"));
                assertEquals(3.14f, rs.getFloat("RealVal"), 0.001);
                assertEquals(2.718281828459, rs.getDouble("FloatVal"), 0.000001);

                assertEquals(new BigDecimal("12345.67890"), rs.getBigDecimal("NumericVal"));
                assertEquals(new BigDecimal("98765.43210"), rs.getBigDecimal("DecimalVal"));
                assertEquals(new BigDecimal("123.4567"), rs.getBigDecimal("MoneyVal"));

                assertEquals("ABC       ", rs.getString("CharVal"));
                assertEquals("Varchar Test", rs.getString("VarCharVal"));
                assertEquals("XYZ       ", rs.getString("NCharVal"));
                assertEquals("NVarchar Unicode: 🚀", rs.getString("NVarCharVal"));
                assertEquals(maxString, rs.getString("VarCharMaxVal"));
                assertEquals(maxString, rs.getString("NVarCharMaxVal"));

                assertArrayEquals(binaryData, rs.getBytes("VarBinaryVal"));
                assertArrayEquals(binaryData, rs.getBytes("VarBinaryMaxVal"));

                assertEquals(java.sql.Date.valueOf(date), rs.getDate("DateVal"));
                assertEquals(java.sql.Time.valueOf(time), rs.getTime("TimeVal"));
                assertEquals(java.sql.Timestamp.valueOf(dateTime), rs.getTimestamp("DateTime2Val"));

                // OffsetDateTime ist im JDBC-Treiber von SQL Server eine eigene Klasse "microsoft.sql.DateTimeOffset"
                // Die String Repräsentation reicht uns hier oft zur Bestätigung.
                assertNotNull(rs.getObject("DateTimeOffsetVal"));
                assertTrue(rs.getString("DateTimeOffsetVal").startsWith(date.toString()));
            }
        }
    }
}
