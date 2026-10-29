/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.common;

public enum EncodingType {
    TEXT('A', "text"),
    INTEGER('I', "integer"),
    DOUBLE('F', "double"),
    HEX('H', "hexadecimal"),
    IPV4('4', "IPv4"),
    IPV4_ADDRESS('V', "IPv4 and port"),
    UUID('U', "UUID"),
    TIMESTAMP('T', "timestamp");

    private final char symbol;
    private final String description;

    EncodingType(char symbol, String description) {
        this.symbol = symbol;
        this.description = description;
    }

    public char getSymbol() {
        return symbol;
    }

    public String getDescription() {
        return description;
    }

    public static EncodingType fromSymbol(char symbol) {
        for (EncodingType type : values()) {
            if (type.symbol == symbol) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown token encoding type symbol: " + symbol);
    }
}
