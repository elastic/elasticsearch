/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Parses the {@code tables} request body parameter.
 */
public class ParseTables {
    public static final Set<DataType> SUPPORTED_TYPES = Set.of(DataType.INTEGER, DataType.KEYWORD, DataType.LONG);
    private static final int MAX_LENGTH = (int) ByteSizeValue.ofMb(1).getBytes();

    private final BlockFactory blockFactory;
    private final EsqlQueryRequest request;
    private final XContentParser p;
    private int length;

    ParseTables(EsqlQueryRequest request, XContentParser p) {
        // TODO use a real block factory
        this.blockFactory = new BlockFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST), BigArrays.NON_RECYCLING_INSTANCE);
        this.request = request;
        this.p = p;
    }

    void parseTables() throws IOException {
        if (p.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new XContentParseException(p.getTokenLocation(), "expected " + XContentParser.Token.START_OBJECT);
        }
        while (true) {
            switch (p.nextToken()) {
                case END_OBJECT -> {
                    return;
                }
                case FIELD_NAME -> {
                    String name = p.currentName();
                    p.nextToken();
                    request.addTable(name, parseTable());
                }
            }
        }
    }

    /**
     * Parse a table from the request. Object keys are in the format {@code name:type}
     * so we can be sure we'll always have a type.
     */
    private Map<String, Column> parseTable() throws IOException {
        Map<String, Column> columns = new LinkedHashMap<>();
        boolean success = false;
        try {
            if (p.currentToken() != XContentParser.Token.START_OBJECT) {
                throw new XContentParseException(p.getTokenLocation(), "expected " + XContentParser.Token.START_OBJECT);
            }
            while (true) {
                switch (p.nextToken()) {
                    case END_OBJECT -> {
                        success = true;
                        return columns;
                    }
                    case FIELD_NAME -> {
                        String name = p.currentName();
                        if (columns.containsKey(name)) {
                            throw new XContentParseException(p.getTokenLocation(), "duplicate column name [" + name + "]");
                        }
                        columns.put(name, parseColumn());
                    }
                    default -> throw new XContentParseException(
                        p.getTokenLocation(),
                        "expected " + XContentParser.Token.END_OBJECT + " or " + XContentParser.Token.FIELD_NAME
                    );
                }
            }
        } finally {
            if (success == false) {
                Releasables.close(columns.values());
            }
        }
    }

    private Column parseColumn() throws IOException {
        if (p.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new XContentParseException(p.getTokenLocation(), "expected " + XContentParser.Token.START_OBJECT);
        }
        if (p.nextToken() != XContentParser.Token.FIELD_NAME) {
            throw new XContentParseException(p.getTokenLocation(), "expected " + XContentParser.Token.FIELD_NAME);
        }
        String type = p.currentName();
        Column result = switch (type) {
            case "integer" -> parseIntColumn();
            case "keyword" -> parseKeywordColumn();
            case "long" -> parseLongColumn();
            case "double" -> parseDoubleColumn();
            default -> throw new XContentParseException(p.getTokenLocation(), "unsupported type [" + type + "]");
        };
        if (p.nextToken() != XContentParser.Token.END_OBJECT) {
            result.close();
            throw new XContentParseException(p.getTokenLocation(), "expected " + XContentParser.Token.END_OBJECT);
        }
        return result;
    }

    private Column parseKeywordColumn() throws IOException {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(100)) { // TODO 100?!
            XContentParser.Token token = p.nextToken();
            if (token != XContentParser.Token.START_ARRAY) {
                throw new XContentParseException(p.getTokenLocation(), "expected " + XContentParser.Token.START_ARRAY);
            }
            BytesRefBuilder scratch = new BytesRefBuilder();
            while (true) {
                switch (p.nextToken()) {
                    case END_ARRAY -> {
                        return new Column(DataType.KEYWORD, builder.build());
                    }
                    case START_ARRAY -> parseTextArray(builder, scratch);
                    case VALUE_NULL -> builder.appendNull();
                    case VALUE_STRING, VALUE_NUMBER, VALUE_BOOLEAN -> appendText(builder, scratch);
                    default -> throw new XContentParseException(p.getTokenLocation(), "expected string, array of strings, or null");
                }
            }
        }
    }

    private void parseTextArray(BytesRefBlock.Builder builder, BytesRefBuilder scratch) throws IOException {
        builder.beginPositionEntry();
        while (true) {
            switch (p.nextToken()) {
                case END_ARRAY -> {
                    builder.endPositionEntry();
                    return;
                }
                case VALUE_STRING -> appendText(builder, scratch);
                default -> throw new XContentParseException(p.getTokenLocation(), "expected string");
            }
        }
    }

    private void appendText(BytesRefBlock.Builder builder, BytesRefBuilder scratch) throws IOException {
        scratch.clear();
        String v = p.text();
        scratch.copyChars(v, 0, v.length());
        length += scratch.length();
        if (length > MAX_LENGTH) {
            throw new XContentParseException(p.getTokenLocation(), "tables too big");
        }
        builder.appendBytesRef(scratch.get());
    }

    private Column parseIntColumn() throws IOException {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(100)) { // TODO 100?!
            XContentParser.Token token = p.nextToken();
            if (token != XContentParser.Token.START_ARRAY) {
                throw new XContentParseException(p.getTokenLocation(), "expected " + XContentParser.Token.START_ARRAY);
            }
            while (true) {
                switch (p.nextToken()) {
                    case END_ARRAY -> {
                        return new Column(DataType.INTEGER, builder.build());
                    }
                    case START_ARRAY -> parseIntArray(builder);
                    case VALUE_NULL -> builder.appendNull();
                    case VALUE_NUMBER, VALUE_STRING -> appendInt(builder);
                    default -> throw new XContentParseException(p.getTokenLocation(), "expected number, array of numbers, or null");
                }
            }
        }
    }

    private void parseIntArray(IntBlock.Builder builder) throws IOException {
        builder.beginPositionEntry();
        while (true) {
            switch (p.nextToken()) {
                case END_ARRAY -> {
                    builder.endPositionEntry();
                    return;
                }
                case VALUE_NUMBER, VALUE_STRING -> appendInt(builder);
                default -> throw new XContentParseException(p.getTokenLocation(), "expected number");
            }
        }
    }

    private void appendInt(IntBlock.Builder builder) throws IOException {
        length += Integer.BYTES;
        if (length > MAX_LENGTH) {
            throw new XContentParseException(p.getTokenLocation(), "tables too big");
        }
        builder.appendInt(p.intValue());
    }

    private Column parseLongColumn() throws IOException {
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(100)) { // TODO 100?!
            XContentParser.Token token = p.nextToken();
            if (token != XContentParser.Token.START_ARRAY) {
                throw new XContentParseException(p.getTokenLocation(), "expected " + XContentParser.Token.START_ARRAY);
            }
            while (true) {
                switch (p.nextToken()) {
                    case END_ARRAY -> {
                        return new Column(DataType.LONG, builder.build());
                    }
                    case START_ARRAY -> parseLongArray(builder);
                    case VALUE_NULL -> builder.appendNull();
                    case VALUE_NUMBER, VALUE_STRING -> appendLong(builder);
                    default -> throw new XContentParseException(p.getTokenLocation(), "expected number, array of numbers, or null");
                }
            }
        }
    }

    private void parseLongArray(LongBlock.Builder builder) throws IOException {
        builder.beginPositionEntry();
        while (true) {
            switch (p.nextToken()) {
                case END_ARRAY -> {
                    builder.endPositionEntry();
                    return;
                }
                case VALUE_NUMBER, VALUE_STRING -> appendLong(builder);
                default -> throw new XContentParseException(p.getTokenLocation(), "expected number");
            }
        }
    }

    private void appendLong(LongBlock.Builder builder) throws IOException {
        length += Long.BYTES;
        if (length > MAX_LENGTH) {
            throw new XContentParseException(p.getTokenLocation(), "tables too big");
        }
        builder.appendLong(p.longValue());
    }

    private Column parseDoubleColumn() throws IOException {
        try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(100)) { // TODO 100?!
            XContentParser.Token token = p.nextToken();
            if (token != XContentParser.Token.START_ARRAY) {
                throw new XContentParseException(p.getTokenLocation(), "expected " + XContentParser.Token.START_ARRAY);
            }
            while (true) {
                switch (p.nextToken()) {
                    case END_ARRAY -> {
                        return new Column(DataType.DOUBLE, builder.build());
                    }
                    case START_ARRAY -> parseDoubleArray(builder);
                    case VALUE_NULL -> builder.appendNull();
                    case VALUE_NUMBER, VALUE_STRING -> appendDouble(builder);
                    default -> throw new XContentParseException(p.getTokenLocation(), "expected number, array of numbers, or null");
                }
            }
        }
    }

    private void parseDoubleArray(DoubleBlock.Builder builder) throws IOException {
        builder.beginPositionEntry();
        while (true) {
            switch (p.nextToken()) {
                case END_ARRAY -> {
                    builder.endPositionEntry();
                    return;
                }
                case VALUE_NUMBER, VALUE_STRING -> appendDouble(builder);
                default -> throw new XContentParseException(p.getTokenLocation(), "expected number");
            }
        }
    }

    private void appendDouble(DoubleBlock.Builder builder) throws IOException {
        length += Double.BYTES;
        if (length > MAX_LENGTH) {
            throw new XContentParseException(p.getTokenLocation(), "tables too big");
        }
        builder.appendDouble(p.doubleValue());
    }
}
