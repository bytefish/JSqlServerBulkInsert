// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;

import java.sql.Connection;
import java.sql.Types;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.*;

public class SqlServerBulkInsert {

    public record BulkInsertResult(boolean success, long rowsAffected, Exception error) {
        public static BulkInsertResult ok(long rows) {
            return new BulkInsertResult(true, rows, null);
        }

        public static BulkInsertResult error(Exception e) {
            return new BulkInsertResult(false, 0, e);
        }
    }

    @FunctionalInterface
    public interface ToShortFunction<T> {
        short applyAsShort(T value);
    }

    @FunctionalInterface
    public interface ToFloatFunction<T> {
        float applyAsFloat(T value);
    }

    @FunctionalInterface
    public interface BulkProgressListener {
        void onProgress(long rowsProcessed);
    }

    @FunctionalInterface
    public interface BulkErrorHandler {
        void handle(Exception e);
    }

    public static class ColumnBinding<TEntity> {
        private final int jdbcType;
        private final int precision;
        private final int scale;
        private final Function<TEntity, Object> extractor;

        public ColumnBinding(int jdbcType, int precision, int scale, Function<TEntity, Object> extractor) {
            this.jdbcType = jdbcType;
            this.precision = precision;
            this.scale = scale;
            this.extractor = extractor;
        }

        public int getJdbcType() {
            return jdbcType;
        }

        public int getPrecision() {
            return precision;
        }

        public int getScale() {
            return scale;
        }

        public Function<TEntity, Object> getExtractor() {
            return extractor;
        }
    }

    public static class SqlServerType<TValue> {
        protected final int jdbcType;
        protected final int precision;
        protected final int scale;

        public SqlServerType(int jdbcType, int precision, int scale) {
            this.jdbcType = jdbcType;
            this.precision = precision;
            this.scale = scale;
        }

        public SqlServerType(int jdbcType) {
            this(jdbcType, 0, 0);
        }

        public <TEntity> ColumnBinding<TEntity> from(Function<TEntity, TValue> extractor) {
            return new ColumnBinding<>(jdbcType, precision, scale, entity -> extractor.apply(entity));
        }
    }

    public static class SqlServerShortType extends SqlServerType<Short> {
        public SqlServerShortType(int jdbcType) { super(jdbcType); }
        public <TEntity> ColumnBinding<TEntity> primitive(ToShortFunction<TEntity> extractor) {
            return new ColumnBinding<>(jdbcType, precision, scale, entity -> extractor.applyAsShort(entity));
        }
    }

    public static class SqlServerIntType extends SqlServerType<Integer> {
        public SqlServerIntType(int jdbcType) {
            super(jdbcType);
        }

        public <TEntity> ColumnBinding<TEntity> primitive(ToIntFunction<TEntity> extractor) {
            return new ColumnBinding<>(jdbcType, precision, scale, entity -> extractor.applyAsInt(entity));
        }
    }

    public static class SqlServerLongType extends SqlServerType<Long> {
        public SqlServerLongType(int jdbcType) {
            super(jdbcType);
        }

        public <TEntity> ColumnBinding<TEntity> primitive(ToLongFunction<TEntity> extractor) {
            return new ColumnBinding<>(jdbcType, precision, scale, entity -> extractor.applyAsLong(entity));
        }
    }


    public static class SqlServerFloatType extends SqlServerType<Float> {
        public SqlServerFloatType(int jdbcType) { super(jdbcType); }
        public <TEntity> ColumnBinding<TEntity> primitive(ToFloatFunction<TEntity> extractor) {
            return new ColumnBinding<>(jdbcType, precision, scale, entity -> extractor.applyAsFloat(entity));
        }
    }

    public static class SqlServerDoubleType extends SqlServerType<Double> {
        public SqlServerDoubleType(int jdbcType) { super(jdbcType); }
        public <TEntity> ColumnBinding<TEntity> primitive(ToDoubleFunction<TEntity> extractor) {
            return new ColumnBinding<>(jdbcType, precision, scale, entity -> extractor.applyAsDouble(entity));
        }
    }

    public static class SqlServerBigDecimalType extends SqlServerType<java.math.BigDecimal> {
        public SqlServerBigDecimalType(int jdbcType) { super(jdbcType); }
        private SqlServerBigDecimalType(int jdbcType, int precision, int scale) { super(jdbcType, precision, scale); }

        public SqlServerBigDecimalType numeric(int precision, int scale) {
            return new SqlServerBigDecimalType(this.jdbcType, precision, scale);
        }

        public SqlServerBigDecimalType decimal(int precision, int scale) {
            return new SqlServerBigDecimalType(this.jdbcType, precision, scale);
        }
    }


    public static class SqlServerStringType extends SqlServerType<String> {
        public SqlServerStringType(int jdbcType) {
            super(jdbcType);
        }

        private SqlServerStringType(int jdbcType, int precision, int scale) {
            super(jdbcType, precision, scale);
        }

        public SqlServerStringType max() {
            return new SqlServerStringType(this.jdbcType, SqlServerTypes.MAX, this.scale);
        }
    }

    public static class SqlServerBinaryType extends SqlServerType<byte[]> {
        public SqlServerBinaryType(int jdbcType) {
            super(jdbcType);
        }

        private SqlServerBinaryType(int jdbcType, int precision, int scale) {
            super(jdbcType, precision, scale);
        }

        public SqlServerBinaryType max() {
            return new SqlServerBinaryType(this.jdbcType, SqlServerTypes.MAX, this.scale);
        }
    }

    public static class SqlServerDateType extends SqlServerType<LocalDate> {
        public SqlServerDateType(int jdbcType) { super(jdbcType); }

        public <TEntity> ColumnBinding<TEntity> localDate(Function<TEntity, LocalDate> extractor) {
            return from(extractor);
        }
    }

    public static class SqlServerTimeType extends SqlServerType<LocalTime> {
        public SqlServerTimeType(int jdbcType) { super(jdbcType); }

        public <TEntity> ColumnBinding<TEntity> localTime(Function<TEntity, LocalTime> extractor) {
            return from(extractor);
        }
    }

    public static class SqlServerDateTimeType extends SqlServerType<LocalDateTime> {
        public SqlServerDateTimeType(int jdbcType) {
            super(jdbcType);
        }

        public <TEntity> ColumnBinding<TEntity> localDateTime(Function<TEntity, LocalDateTime> extractor) {
            return from(extractor);
        }

        public <TEntity> ColumnBinding<TEntity> instant(Function<TEntity, Instant> extractor) {
            return new ColumnBinding<>(jdbcType, precision, scale, e -> {
                Instant val = extractor.apply(e);
                return val == null ? null : LocalDateTime.ofInstant(val, ZoneOffset.UTC);
            });
        }
    }

    public static class SqlServerDateTimeOffsetType extends SqlServerType<OffsetDateTime> {
        public SqlServerDateTimeOffsetType(int jdbcType) {
            super(jdbcType);
        }

        public <TEntity> ColumnBinding<TEntity> offsetDateTime(Function<TEntity, OffsetDateTime> extractor) {
            return from(extractor);
        }

        public <TEntity> ColumnBinding<TEntity> instant(Function<TEntity, Instant> extractor) {
            return new ColumnBinding<>(jdbcType, precision, scale, e -> {
                Instant val = extractor.apply(e);
                return val == null ? null : val.atOffset(ZoneOffset.UTC);
            });
        }
    }

    public static class SqlServerBooleanType extends SqlServerType<Boolean> {
        public SqlServerBooleanType(int jdbcType) { super(jdbcType); }
        public <TEntity> ColumnBinding<TEntity> primitive(Predicate<TEntity> extractor) {
            return new ColumnBinding<>(jdbcType, precision, scale, entity -> extractor.test(entity));
        }
    }

    public static class SqlServerTypes {
        private SqlServerTypes() {
        }

        public static final int MAX = -1;

        // Primitives & Numbers
        public static final SqlServerBooleanType BIT = new SqlServerBooleanType(Types.BIT);
        public static final SqlServerShortType TINYINT = new SqlServerShortType(Types.TINYINT);
        public static final SqlServerShortType SMALLINT = new SqlServerShortType(Types.SMALLINT);
        public static final SqlServerIntType INT = new SqlServerIntType(Types.INTEGER);
        public static final SqlServerLongType BIGINT = new SqlServerLongType(Types.BIGINT);
        public static final SqlServerFloatType REAL = new SqlServerFloatType(Types.REAL);
        public static final SqlServerDoubleType FLOAT = new SqlServerDoubleType(Types.FLOAT); // SQL Server FLOAT is Double

        // Decimals & Money
        public static final SqlServerBigDecimalType NUMERIC = new SqlServerBigDecimalType(Types.NUMERIC);
        public static final SqlServerBigDecimalType DECIMAL = new SqlServerBigDecimalType(Types.DECIMAL);
        public static final SqlServerBigDecimalType MONEY = new SqlServerBigDecimalType(Types.DECIMAL);
        public static final SqlServerBigDecimalType SMALLMONEY = new SqlServerBigDecimalType(Types.DECIMAL);

        // Strings
        public static final SqlServerStringType CHAR = new SqlServerStringType(Types.CHAR);
        public static final SqlServerStringType VARCHAR = new SqlServerStringType(Types.VARCHAR);
        public static final SqlServerStringType NCHAR = new SqlServerStringType(Types.NCHAR);
        public static final SqlServerStringType NVARCHAR = new SqlServerStringType(Types.NVARCHAR);

        // Binaries
        public static final SqlServerBinaryType VARBINARY = new SqlServerBinaryType(Types.VARBINARY);

        // Unique Identifier
        public static final SqlServerType<UUID> UNIQUEIDENTIFIER = new SqlServerType<>(microsoft.sql.Types.GUID);

        // Dates & Times
        public static final SqlServerDateType DATE = new SqlServerDateType(Types.DATE);
        public static final SqlServerTimeType TIME = new SqlServerTimeType(Types.TIME);
        public static final SqlServerDateTimeType DATETIME = new SqlServerDateTimeType(Types.TIMESTAMP);
        public static final SqlServerDateTimeType DATETIME2 = new SqlServerDateTimeType(Types.TIMESTAMP);
        public static final SqlServerDateTimeType SMALLDATETIME = new SqlServerDateTimeType(Types.TIMESTAMP);
        public static final SqlServerDateTimeOffsetType DATETIMEOFFSET = new SqlServerDateTimeOffsetType(microsoft.sql.Types.DATETIMEOFFSET);
    }

// =========================================================================
// 5. THE MAPPER
// =========================================================================

    public static class SqlServerMapper<TEntity> {

        static class MappedColumn<T> {
            final String columnName;
            final ColumnBinding<T> binding;

            MappedColumn(String n, ColumnBinding<T> b) {
                this.columnName = n;
                this.binding = b;
            }
        }

        private final List<MappedColumn<TEntity>> columns = new ArrayList<>();

        public static <T> SqlServerMapper<T> forClass(Class<T> clazz) {
            return new SqlServerMapper<>();
        }

        public SqlServerMapper() {
        }

        public SqlServerMapper<TEntity> map(String name, ColumnBinding<TEntity> binding) {
            columns.add(new MappedColumn<>(name, binding));
            return this;
        }

        List<MappedColumn<TEntity>> getColumns() {
            return Collections.unmodifiableList(columns);
        }
    }

    public static class SqlServerBulkRecordAdapter<TEntity> implements ISQLServerBulkRecord {
        private final List<SqlServerMapper.MappedColumn<TEntity>> columns;
        private final Iterator<TEntity> iterator;
        private final Set<Integer> ordinals;
        private final BulkProgressListener progressListener;
        private final int notifyAfter;

        private TEntity current;
        private long counter = 0;

        SqlServerBulkRecordAdapter(SqlServerMapper<TEntity> mapper, Iterator<TEntity> iterator, int notifyAfter, BulkProgressListener listener) {
            this.columns = mapper.getColumns();
            this.iterator = iterator;
            this.notifyAfter = notifyAfter;
            this.progressListener = listener;
            this.ordinals = new HashSet<>();
            for (int i = 1; i <= columns.size(); i++) ordinals.add(i);
        }

        @Override
        public Set<Integer> getColumnOrdinals() {
            return ordinals;
        }

        @Override
        public String getColumnName(int c) {
            return columns.get(c - 1).columnName;
        }

        @Override
        public int getColumnType(int c) {
            return columns.get(c - 1).binding.getJdbcType();
        }

        @Override
        public int getPrecision(int c) {
            return columns.get(c - 1).binding.getPrecision();
        }

        @Override
        public int getScale(int c) {
            return columns.get(c - 1).binding.getScale();
        }

        @Override
        public boolean isAutoIncrement(int c) {
            return false;
        }

        @Override
        public Object[] getRowData() throws SQLServerException {
            Object[] data = new Object[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                data[i] = columns.get(i).binding.getExtractor().apply(current);
            }
            return data;
        }

        @Override
        public void addColumnMetadata(int positionInSource, String name, int jdbcType, int precision, int scale, DateTimeFormatter dateTimeFormatter) {
            // Doesn't need to be implemented
        }

        @Override
        public void addColumnMetadata(int positionInSource, String name, int jdbcType, int precision, int scale) {
            // Doesn't need to be implemented
        }

        @Override
        public void setTimestampWithTimezoneFormat(String s) {

        }

        @Override
        public void setTimestampWithTimezoneFormat(DateTimeFormatter dateTimeFormatter) {

        }

        @Override
        public void setTimeWithTimezoneFormat(String s) {

        }

        @Override
        public void setTimeWithTimezoneFormat(DateTimeFormatter dateTimeFormatter) {

        }

        @Override
        public DateTimeFormatter getColumnDateTimeFormatter(int i) {
            return null;
        }

        @Override
        public boolean next() throws SQLServerException {
            if (iterator.hasNext()) {
                current = iterator.next();
                counter++;
                if (progressListener != null && notifyAfter > 0 && counter % notifyAfter == 0) {
                    progressListener.onProgress(counter);
                }
                return true;
            }
            return false;
        }

        public long getRowsProcessed() {
            return counter;
        }
    }

    public static class SqlServerBulkWriter<TEntity> {
        private final SqlServerMapper<TEntity> mapper;

        private int batchSize = 0;
        private boolean tableLock = true;
        private boolean checkConstraints = true;
        private int notifyAfter = 0;
        private BulkProgressListener progressListener = null;
        private BulkErrorHandler errorHandler = e -> {
        };

        public SqlServerBulkWriter(SqlServerMapper<TEntity> mapper) {
            this.mapper = mapper;
        }

        public SqlServerBulkWriter<TEntity> withBatchSize(int s) {
            this.batchSize = s;
            return this;
        }

        public SqlServerBulkWriter<TEntity> withTableLock(boolean l) {
            this.tableLock = l;
            return this;
        }

        public SqlServerBulkWriter<TEntity> withCheckConstraints(boolean c) {
            this.checkConstraints = c;
            return this;
        }

        public SqlServerBulkWriter<TEntity> withNotifyAfter(int rows, BulkProgressListener listener) {
            this.notifyAfter = rows;
            this.progressListener = listener;
            return this;
        }

        public SqlServerBulkWriter<TEntity> withErrorHandler(BulkErrorHandler h) {
            this.errorHandler = h;
            return this;
        }

        public BulkInsertResult saveAll(Connection connection, String schemaName, String tableName, Iterable<TEntity> entities) {
            try {
                SQLServerConnection sqlServerConnection = connection.unwrap(SQLServerConnection.class);
                try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(sqlServerConnection)) {
                    SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                    options.setBatchSize(batchSize);
                    options.setTableLock(tableLock);
                    options.setCheckConstraints(checkConstraints);

                    bulkCopy.setBulkCopyOptions(options);
                    bulkCopy.setDestinationTableName("[" + schemaName + "].[" + tableName + "]");

                    for (var column : mapper.getColumns()) {
                        bulkCopy.addColumnMapping(column.columnName, column.columnName);
                    }

                    SqlServerBulkRecordAdapter<TEntity> adapter = new SqlServerBulkRecordAdapter<>(
                            mapper, entities.iterator(), notifyAfter, progressListener);

                    bulkCopy.writeToServer(adapter);
                    return BulkInsertResult.ok(adapter.getRowsProcessed());
                }
            } catch (Exception e) {
                errorHandler.handle(e);
                return BulkInsertResult.error(e);
            }
        }
    }
}