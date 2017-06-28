/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

//
// Basic tabular messaging for the CLI
//
// The protocol is very similar (a subset) to the JDBC driver
//
// To simplify things, the protocol is NOT meant to be backwards compatible.
//
public interface Proto {

    // All requests start with 
    // magic_number - int    - just because
    // version      - int    - the version the client understands
    // action       - int    - action to perform
    // <action-param> (see below)

    int MAGIC_NUMBER = 0x0C0DEC110;
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
        
        COMMAND(0x10);
    
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