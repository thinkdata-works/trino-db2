/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.db2;

import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcErrorCode;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.StandardColumnMappings;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import com.google.inject.Inject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultCharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromLongTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.toLongTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.toTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class DB2Client
        extends BaseJdbcClient
{
    private final int varcharMaxLength;
    private static final int DB2_MAX_SUPPORTED_TIMESTAMP_PRECISION = 12;
    // java.util.LocalDateTime supports up to nanosecond precision
    private static final int MAX_LOCAL_DATE_TIME_PRECISION = 9;
    private static final String VARCHAR_FORMAT = "VARCHAR(%d)";

    @Inject
    public DB2Client(
            BaseJdbcConfig config,
            DB2Config db2config,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier remoteQueryModifier)
            throws SQLException
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, remoteQueryModifier, true);
        this.varcharMaxLength = db2config.getVarcharMaxLength();

        // http://stackoverflow.com/questions/16910791/getting-error-code-4220-with-null-sql-state
        System.setProperty("db2.jcc.charsetDecoderEncoder", "3");
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcSplit split, JdbcTableHandle jdbcTableHandle)
            throws SQLException
    {
        Connection connection = super.getConnection(session, split, jdbcTableHandle);
        try {
            // TRANSACTION_READ_UNCOMMITTED = Uncommitted read
            // http://www.ibm.com/developerworks/data/library/techarticle/dm-0509schuetz/
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        switch (typeHandle.getJdbcType()) {
            case Types.BIT, Types.BOOLEAN -> {
                return Optional.of(booleanColumnMapping());
            }
            case Types.TINYINT -> {
                return Optional.of(tinyintColumnMapping());
            }
            case Types.SMALLINT -> {
                return Optional.of(smallintColumnMapping());
            }
            case Types.INTEGER -> {
                return Optional.of(integerColumnMapping());
            }
            case Types.BIGINT -> {
                return Optional.of(bigintColumnMapping());
            }
            case Types.REAL -> {
                return Optional.of(realColumnMapping());
            }
            case Types.FLOAT, Types.DOUBLE -> {
                return Optional.of(doubleColumnMapping());
            }
            case Types.NUMERIC, Types.DECIMAL -> {
                int decimalDigits = typeHandle.getRequiredDecimalDigits();
                int precision = typeHandle.getRequiredColumnSize() + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0))));
            }
            case Types.CHAR, Types.NCHAR -> {
                return Optional.of(defaultCharColumnMapping(typeHandle.getRequiredColumnSize(), false));
            }
            case Types.VARCHAR -> {
                int columnSize = typeHandle.getRequiredColumnSize();
                if (columnSize == -1) {
                    return Optional.of(varcharColumnMapping(createUnboundedVarcharType(), true));
                }
                return Optional.of(defaultVarcharColumnMapping(columnSize, true));
            }
            case Types.NVARCHAR, Types.LONGVARCHAR, Types.LONGNVARCHAR -> {
                return Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize(), false));
            }
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> {
                return Optional.of(varbinaryColumnMapping());
            }
            case Types.DATE -> {
                return Optional.of(dateColumnMappingUsingLocalDate());
            }
            case Types.TIME -> {
                return Optional.of(timeColumnMapping(TimeType.TIME_MILLIS));
            }
            case Types.TIMESTAMP -> {
                TimestampType timestampType = typeHandle.getDecimalDigits()
                        .map(TimestampType::createTimestampType)
                        .orElse(TIMESTAMP_MILLIS);
                return Optional.of(timestampColumnMapping(timestampType));
            }
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    public static ColumnMapping dateColumnMappingUsingLocalDate()
    {
        return ColumnMapping.longMapping(DateType.DATE, dateReadFunctionUsingLocalDate(), dateWriteFunctionUsingLocalDate());
    }

    public static ColumnMapping timestampColumnMapping(TimestampType timestampType)
    {
        if (timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(
                    timestampType,
                    timestampReadFunction(timestampType),
                    timestampWriteFunction(timestampType));
        }
        checkArgument(timestampType.getPrecision() <= MAX_LOCAL_DATE_TIME_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return ColumnMapping.objectMapping(
                timestampType,
                longtimestampReadFunction(timestampType),
                longTimestampWriteFunction(timestampType));
    }

    public static LongReadFunction timestampReadFunction(TimestampType timestampType)
    {
        checkArgument(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return (resultSet, columnIndex) -> toTrinoTimestamp(timestampType, resultSet.getTimestamp(columnIndex).toLocalDateTime());
    }

    /**
     * Customized timestampReadFunction to convert timestamp type to LocalDateTime type.
     * Notice that it's because Db2 JDBC driver doesn't support ResetSet#getObject(index, Class).
     *
     * @param timestampType type of timestamp to read
     * @return function to read the new timestamp
     */
    private static ObjectReadFunction longtimestampReadFunction(TimestampType timestampType)
    {
        checkArgument(timestampType.getPrecision() <= MAX_LOCAL_DATE_TIME_PRECISION,
                "Precision is out of range: %s", timestampType.getPrecision());
        return ObjectReadFunction.of(
                LongTimestamp.class,
                (resultSet, columnIndex) -> toLongTrinoTimestamp(timestampType, resultSet.getTimestamp(columnIndex).toLocalDateTime()));
    }

    /**
     * Customized timestampWriteFunction.
     * Notice that it's because Db2 JDBC driver doesn't support PreparedStatement#setObject(index, LocalDateTime).
     *
     * @param timestampType type of timestamp to write to
     * @return functino to write the new timestamp
     */
    private static ObjectWriteFunction longTimestampWriteFunction(TimestampType timestampType)
    {
        checkArgument(timestampType.getPrecision() > TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return ObjectWriteFunction.of(
                LongTimestamp.class,
                (statement, index, value) -> statement.setTimestamp(index, Timestamp.valueOf(fromLongTrinoTimestamp(value, timestampType.getPrecision()))));
    }

    /*
        According to https://github.com/IBM/trino-db2/issues/75 the db2 jdbc connector's implement of `resultSet.getObject` can not be used with a class type as the second param
        as it is implemented in the base class

        This overrides to use the `setDate` and `getDate` objects
     */
    public static LongReadFunction dateReadFunctionUsingLocalDate()
    {
        return new LongReadFunction()
        {
            public boolean isNull(ResultSet resultSet, int columnIndex) throws SQLException
            {
                resultSet.getDate(columnIndex);
                return resultSet.wasNull();
            }

            public long readLong(ResultSet resultSet, int columnIndex) throws SQLException
            {
                java.sql.Date value = resultSet.getDate(columnIndex);
                if (value == null) {
                    throw new TrinoException(JdbcErrorCode.JDBC_ERROR, "Driver returned null LocalDate for a non-null value");
                }
                else {
                    return value.toLocalDate().toEpochDay();
                }
            }
        };
    }

    public static LongWriteFunction dateWriteFunctionUsingLocalDate()
    {
        return LongWriteFunction.of(91, (statement, index, value) ->
        {
            statement.setDate(index, java.sql.Date.valueOf(LocalDate.ofEpochDay(value)));
        });
    }

    public static ColumnMapping timeColumnMapping(TimeType timeType)
    {
        return ColumnMapping.longMapping(timeType, timeReadFunction(timeType), timeWriteFunction(timeType.getPrecision()));
    }

    public static LongReadFunction timeReadFunction(TimeType timeType)
    {
        requireNonNull(timeType, "timeType is null");
        checkArgument(timeType.getPrecision() <= 9, "Unsupported type precision: %s", timeType);
        return (resultSet, columnIndex) ->
        {
            java.sql.Time time = resultSet.getTime(columnIndex);
            long nanosOfDay = time.toLocalTime().toNanoOfDay();
            verify(nanosOfDay < 86400000000000L, "Invalid value of nanosOfDay: %s", nanosOfDay);
            long picosOfDay = nanosOfDay * 1000L;
            long rounded = Timestamps.round(picosOfDay, 12 - timeType.getPrecision());
            if (rounded == 86400000000000000L) {
                rounded = 0L;
            }

            return rounded;
        };
    }

    public static LongWriteFunction timeWriteFunction(int precision)
    {
        checkArgument(precision <= 9, "Unsupported precision: %s", precision);
        return LongWriteFunction.of(92, (statement, index, picosOfDay) ->
        {
            picosOfDay = Timestamps.round(picosOfDay, 12 - precision);
            if (picosOfDay == 86400000000000000L) {
                picosOfDay = 0L;
            }
            LocalTime localtime = StandardColumnMappings.fromTrinoTime(picosOfDay);
            // Copied from private method in superclass
            java.sql.Time sqltime = new Time(Time.valueOf(localtime).getTime() + TimeUnit.NANOSECONDS.toMillis(localtime.getNano()));
            statement.setTime(index, sqltime);
        });
    }

    /**
     * To map data types when generating SQL.
     */
    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type instanceof VarcharType varcharType) {
            String dataType;

            if (varcharType.isUnbounded()) {
                dataType = format(VARCHAR_FORMAT, this.varcharMaxLength);
            }
            else if (varcharType.getBoundedLength() > this.varcharMaxLength) {
                dataType = format("CLOB(%d)", varcharType.getBoundedLength());
            }
            else if (varcharType.getBoundedLength() < this.varcharMaxLength) {
                dataType = format(VARCHAR_FORMAT, varcharType.getBoundedLength());
            }
            else {
                dataType = format(VARCHAR_FORMAT, this.varcharMaxLength);
            }

            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        if (type instanceof TimestampType timestampType) {
            verify(timestampType.getPrecision() <= DB2_MAX_SUPPORTED_TIMESTAMP_PRECISION);
            return WriteMapping.longMapping(format("TIMESTAMP(%s)", timestampType.getPrecision()), timestampWriteFunction(timestampType));
        }

        return this.legacyToWriteMapping(type);
    }

    public static LongWriteFunction timestampWriteFunction(TimestampType timestampType)
    {
        checkArgument(timestampType.getPrecision() <= 6, "Precision is out of range: %s", timestampType.getPrecision());
        return LongWriteFunction.of(93, (statement, index, value) -> {
            statement.setTimestamp(index, Timestamp.valueOf(StandardColumnMappings.fromTrinoTimestamp(value)));
        });
    }

    protected WriteMapping legacyToWriteMapping(Type type)
    {
        if (type instanceof VarcharType varcharType) {
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "varchar";
            }
            else {
                dataType = "varchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (type instanceof CharType) {
            return WriteMapping.sliceMapping("char(" + ((CharType) type).getLength() + ")", charWriteFunction());
        }
        if (type instanceof DecimalType decimalType) {
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }
        if (type == TINYINT) {
            return WriteMapping.longMapping("tinyint", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }
        if (type == REAL) {
            return WriteMapping.longMapping("real", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }
        if (type == VARBINARY) {
            return WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction());
        }
        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate());
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    protected void renameTable(ConnectorSession session, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            String newTableName = newTable.getTableName();
            if (connection.getMetaData().storesUpperCaseIdentifiers()) {
                newTableName = newTableName.toUpperCase(ENGLISH);
            }
            // Specifies the new name for the table without a schema name
            String sql = format(
                    "RENAME TABLE %s TO %s",
                    quoted(catalogName, schemaName, tableName),
                    quoted(newTableName));
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        String sql = format(
                "CREATE TABLE %s AS (SELECT %s FROM %s) WITH NO DATA",
                quoted(catalogName, schemaName, newTableName),
                columnNames.stream()
                        .map(this::quoted)
                        .collect(joining(", ")),
                quoted(catalogName, schemaName, tableName));
        try {
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }
}
