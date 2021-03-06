// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.test.mapping;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import de.bytefish.jsqlserverbulkinsert.SqlServerBulkInsert;
import de.bytefish.jsqlserverbulkinsert.mapping.AbstractMapping;
import de.bytefish.jsqlserverbulkinsert.test.base.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class BigIntegerMappingTest extends TransactionalTestBase {

    private class BigIntegerEntity extends SampleEntity<BigInteger> {

        public BigIntegerEntity(BigInteger value) {
            super(value);
        }
    }

    private class BigIntegerMapping extends AbstractMapping<BigIntegerEntity> {

        public BigIntegerMapping() {
            super("dbo", "UnitTest");

            mapBigInt("BigIntegerValue", BigIntegerEntity::getValue);
        }

    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTestTable();
    }

    @Test
    public void bulkBigIntDataTest() throws SQLException {
        BigInteger BigIntegerValue = new BigInteger("47878778228484");
        // Create the Value:
        List<BigIntegerEntity> entities = Arrays.asList(new BigIntegerEntity(BigIntegerValue));
        // Create the BulkInserter:
        BigIntegerMapping mapping = new BigIntegerMapping();
        // Create the Bulk Inserter:
        SqlServerBulkInsert<BigIntegerEntity> inserter = new SqlServerBulkInsert<>(mapping);
        // Now save all entities of a given stream:
        inserter.saveAll(connection, entities.stream());
        // And assert all have been written to the database:
        ResultSet rs = getAll();
        // We have a Value:
        Assert.assertEquals(true, rs.next());
        // Get the Date we have written:
        long resultBigIntegerValue = rs.getLong("BigIntegerValue");
        // Assert both are equal:
        Assert.assertEquals(BigIntegerValue.longValueExact(), resultBigIntegerValue);
        // Assert only one record was read:
        Assert.assertEquals(false, rs.next());
    }

    @Test
    public void bulkNullDataTest() throws SQLException {
        BigInteger BigIntegerValue = null;
        // Create the Value:
        List<BigIntegerEntity> entities = Arrays.asList(new BigIntegerEntity(BigIntegerValue));
        // Create the BulkInserter:
        BigIntegerMapping mapping = new BigIntegerMapping();
        // Create the Bulk Inserter:
        SqlServerBulkInsert<BigIntegerEntity> inserter = new SqlServerBulkInsert<>(mapping);
        // Now save all entities of a given stream:
        inserter.saveAll(connection, entities.stream());
        // And assert all have been written to the database:
        ResultSet rs = getAll();
        // We have a Value:
        Assert.assertEquals(true, rs.next());
        // Get the Date we have written:
        Long resultBigIntegerValue = rs.getLong("BigIntegerValue");
        // Assert both are equal:
        Assert.assertTrue(rs.wasNull());
        // Assert only one record was read:
        Assert.assertEquals(false, rs.next());
    }

    private ResultSet getAll() throws SQLException {

        String sqlStatement = "SELECT * FROM dbo.UnitTest";

        Statement statement = connection.createStatement();

        return statement.executeQuery(sqlStatement);
    }

    private void createTestTable() throws SQLException {
        String sqlStatement = "CREATE TABLE [dbo].[UnitTest]\n" +
                "            (\n" +
                "                BigIntegerValue bigint null\n" +
                "            );";

        Statement statement = connection.createStatement();

        statement.execute(sqlStatement);
    }

}
