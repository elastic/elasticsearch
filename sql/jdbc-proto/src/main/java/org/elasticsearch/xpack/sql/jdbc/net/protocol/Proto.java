/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import java.sql.SQLClientInfoException;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

import javax.sql.rowset.serial.SerialException;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;

//
// Basic tabular messaging for the JDBC driver
//
// Note this message is transmitted through HTTP and thus things like transport error codes
// are handled through that.

// The proto is based around a simple, single request-response model.
// Note the field order is _important_.
// To simplify things, the protocol is not meant to be backwards compatible.
//
public interface Proto {

    // All requests start with 
    // magic_number - int    - just because
    // version      - int    - the version the client understands
    // action       - int    - action to perform
    // <action-param> (see below)

    int MAGIC_NUMBER = 0x0C0DE1DBC;
    int VERSION = 000_000_001;

    public interface Header {
        int value();
    }

    // The response start with a similar pattern
    // magic_number
    // version
    // action reply (status)
    // payload

    enum Status implements Header {
        // If successful, each method has its own params (describe for each method)
        SUCCESS  (0x5000000),

        // Expected exceptions contain
        // message      - string    - exception message
        // exception    - string    - exception class
        // sql exception - int      - to what SqlException type this maps to (see below)
        EXCEPTION(0x3000000),

        // Unexpected error contains the following fields

        // message     - string     - exception message
        // exception   - string     - exception class
        // stacktrace  - string     - exception stacktrace (should be massaged)
        ERROR  (0xF000000);

        private static final Map<Integer, Status> MAP = Arrays.stream(Status.class.getEnumConstants())
                .collect(toMap(Status::value, Function.identity()));

        private final int value;
        
        Status(int value) {
            this.value = value;
        }
        
        @Override
        public int value() {
            return value;
        }

        public static Status from(int value) {
            return MAP.get(value & 0xF000000);
        }

        public static int toSuccess(Action action) {
            return action.value() | SUCCESS.value();
        }

        public static int toException(Action action) {
            return action.value() | EXCEPTION.value();
        }

        public static int toError(Action action) {
            return action.value() | ERROR.value();
        }
    }

    enum SqlExceptionType {
        UNKNOWN    (0x001),
        SERIAL     (0x010),
        CLIENT_INFO(0x020),
        DATA       (0x100),
        SYNTAX     (0x200),
        
        RECOVERABLE(0x300),
        TIMEOUT    (0x400);
        

        private static final Map<Integer, SqlExceptionType> MAP = Arrays.stream(SqlExceptionType.class.getEnumConstants())
                .collect(toMap(SqlExceptionType::value, Function.identity()));

        private final int value;

        SqlExceptionType(int value) {
            this.value = value;
        }

        public int value() {
            return value;
        }

        public static SqlExceptionType from(int value) {
            return MAP.get(value);
        }

        public static SQLException asException(SqlExceptionType type, String message) {
            if (message == null) {
                message = "";
            }
            if (type == SERIAL) {
                return new SerialException(message);
            }
            if (type == CLIENT_INFO) {
                return new SQLClientInfoException(message, emptyMap());
            }
            if (type == DATA) {
                return new SQLDataException(message);
            }
            if (type == SYNTAX) {
                return new SQLSyntaxErrorException(message);
            }
            if (type == RECOVERABLE) {
                return new SQLRecoverableException(message);
            }
            if (type == TIMEOUT) {
                return new SQLTimeoutException(message);
            }

            return new SQLException("Unexpected 'expected' exception " + type);
        }
    }

    // 
    // RPC
    //

    enum Action implements Header {

        //
        // Retrieves information about the server    
        //    
        // 
        // java.version     - string
        // java.vendor      - string
        // java.class.path  - string
        // os.name          - string
        // os.version       - string
        // 
        // 
        // node.name        - string
        // cluster.name     - string
        // version.major    - byte
        // version.minor    - byte
        // version.number   - string
        // version.hash     - string
        // version.build    - string
        // # nodes          - fall back nodes to connect to
        // for each node
        // node.name        - string
        // node.address     - string
        // 

        INFO(0x01),
    
    
        // 
        // Retrieves metadata about tables
        //
        // Request:
        // 
        // name pattern     - string
        // 
        // Response:
        // 
        // # tables         - int    - index.type
        // for each table
        // name             - string - table name
        // 
        
        META_TABLE(0x04),
    
        //
        // Retrieves metadata about columns 
        //    
        // Request:
        // 
        // table pattern    - string
        // column pattern   - string
        // 
        // Response:
        // 
        // # columns        - int    - columns that match
        // for each column (MetaColumnInfo):
        // table.name       - string - index.type
        // column.name      - string - column name
        // data.type        - int    - data type
        // column.size      - int    
        // ordinal.position - int    - position inside table
    
        META_COLUMN(0x05),
    
    
        // Request (QueryInfo):
        // Contains several _header_ fields
        //
        // fetch-size      - int    - the number of results returned in a response by the server
        // (TimeoutInfo)
        // client_time     - long   - milliseconds since the epoch (in UTC)
        // timeout         - long   - how much time (in ms) the server has to deliver an answer.
        // request_timeout - long   - how much time (in ms) a scroll/cursor needs to be kept alive between requests
    
        // And the actual payload.
        //
        // query        - string - the actual SQL query
        //
    
        // Response:
        // Header fields (ResultInfo):
        //
        // time_received - long   - (in UTC)
        // time_sent     - long   - (in UTC)
        // request_id     - string - id for this page; if it's null it means there are no more results
        // # columns     - int    - number of columns
        // row schema
        //   for each column (ColumnInfo): 
        //      name       - string - name of the column
        //      alias      - string - if the column has an alias or label
        //      table      - string - index.type
        //      schema     - string - TBD (could be user)
        //      catalog    - string - TBD (could be cluster/node id)
        //      type       - int    - JDBC type
        // # rows          - int    - number of rows
        //   for each row, the actual values (the schema is not sent anymore)
    
        QUERY_INIT(0x10),
    
        QUERY_PAGE(0x15),
    
        // Request (PageInfo):

        // request_id      - string  - the request/scroll id
        // (TimeoutInfo):
        // client_time     - long    - ms since the epoch (in UTC)
        // timeout         - long    - how much time (in ms) the server has to deliver an answer.
        // request_timeout - long    - how much time (in ms) the request needs to be kept alive until the next request

    
        // Returns (ResultPageInfo):
        // request_id      - string - id for this page; if it's null it means there are no more results
        // # rows         - int    - number of rows
        // for each row, the actual values (the schema is not sent anymore)

        // TODO: needs implementing
        QUERY_CLOSE(0x19);


        private static final Map<Integer, Action> MAP = Arrays.stream(Action.class.getEnumConstants())
                .collect(toMap(Action::value, Function.identity()));

        private final int value;

        Action(int value) {
            this.value = value;
        }

        @Override
        public int value() {
            return value;
        }

        public static Action from(int value) {
            return MAP.get(value & 0x00000FF);
        }
    }
}