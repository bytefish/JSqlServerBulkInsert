package de.bytefish.jsqlserverbulkinsert.converters;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

public class LocalDateTimeConverter extends BaseConverter<LocalDateTime> {
    @Override
    public Object internalConvert(LocalDateTime value) {

        long epochSeconds = value.toEpochSecond(OffsetDateTime.now().getOffset());

        // Convert to milliseconds to create the Timestamp:
        Timestamp castedResult = new Timestamp(epochSeconds * 1000);

        // Round to 100 nanoseconds (precision that SQL server can handle):
        castedResult.setNanos((value.getNano()/100)*100);

        return castedResult;
    }
}
