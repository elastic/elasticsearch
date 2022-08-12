/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.support;

import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xcontent.XContentParser.Token.END_ARRAY;
import static org.elasticsearch.xcontent.XContentParser.Token.END_OBJECT;
import static org.elasticsearch.xcontent.XContentParser.Token.START_OBJECT;

public final class XContentFilterKeysUtils {

    private XContentFilterKeysUtils() {}

    public static Map<String, Object> filterMapOrdered(Set<String> keys, XContentParser parser) throws IOException {
        try {
            if (parser.currentToken() != null) {
                throw new IllegalArgumentException("Parser already started");
            }
            if (parser.nextToken() != START_OBJECT) {
                throw new IllegalArgumentException("Content should start with START_OBJECT");
            }
            State state = new State(new ArrayList<>(keys));
            return parse(parser, state);
        } catch (IOException e) {
            throw new IOException("could not build a filtered payload out of xcontent", e);
        }
    }

    private static Map<String, Object> parse(XContentParser parser, State state) throws IOException {
        return parse(parser, state, true);
    }

    private static Map<String, Object> parse(XContentParser parser, State state, boolean isOutsideOfArray) throws IOException {
        if (state.includeLeaf) {
            return parser.map();
        }

        Map<String, Object> data = new HashMap<>();
        for (XContentParser.Token token = parser.nextToken(); token != END_OBJECT; token = parser.nextToken()) {
            switch (token) {
                case FIELD_NAME -> state.nextField(parser.currentName());
                case START_OBJECT -> {
                    if (state.includeKey) {
                        String fieldName = state.currentFieldName();
                        Map<String, Object> nestedData = parse(parser, state, isOutsideOfArray);
                        data.put(fieldName, nestedData);
                    } else {
                        parser.skipChildren();
                    }
                    if (isOutsideOfArray) {
                        state.previousField();
                    }
                }
                case START_ARRAY -> {
                    if (state.includeKey) {
                        String fieldName = state.currentFieldName();
                        List<Object> arrayData = arrayParsing(parser, state);
                        data.put(fieldName, arrayData);
                    } else {
                        parser.skipChildren();
                    }
                    state.previousField();
                }
                case VALUE_STRING -> {
                    if (state.includeKey) {
                        data.put(state.currentFieldName(), parser.text());
                    }
                    if (isOutsideOfArray) {
                        state.previousField();
                    }
                }
                case VALUE_NUMBER -> {
                    if (state.includeKey) {
                        data.put(state.currentFieldName(), parser.numberValue());
                    }
                    if (isOutsideOfArray) {
                        state.previousField();
                    }
                }
                case VALUE_BOOLEAN -> {
                    if (state.includeKey) {
                        data.put(state.currentFieldName(), parser.booleanValue());
                    }
                    if (isOutsideOfArray) {
                        state.previousField();
                    }
                }
            }
        }
        return data;
    }

    private static List<Object> arrayParsing(XContentParser parser, State state) throws IOException {
        List<Object> values = new ArrayList<>();
        for (XContentParser.Token token = parser.nextToken(); token != END_ARRAY; token = parser.nextToken()) {
            switch (token) {
                case START_OBJECT -> values.add(parse(parser, state, false));
                case VALUE_STRING -> values.add(parser.text());
                case VALUE_NUMBER -> values.add(parser.numberValue());
                case VALUE_BOOLEAN -> values.add(parser.booleanValue());
            }
        }
        return values;
    }

    private static final class State {

        final List<String> extractPaths;
        StringBuilder currentPath = new StringBuilder();

        boolean includeLeaf;
        boolean includeKey;
        String currentFieldName;

        private State(List<String> extractPaths) {
            this.extractPaths = extractPaths;
        }

        void nextField(String fieldName) {
            currentFieldName = fieldName;
            if (currentPath.length() != 0) {
                currentPath.append('.');
            }
            currentPath = currentPath.append(fieldName);
            final String path = currentPath.toString();
            for (String extractPath : extractPaths) {
                if (path.equals(extractPath)) {
                    includeKey = true;
                    includeLeaf = true;
                    return;
                } else if (extractPath.startsWith(path)) {
                    includeKey = true;
                    return;
                }
            }
            includeKey = false;
            includeLeaf = false;
        }

        String currentFieldName() {
            return currentFieldName;
        }

        void previousField() {
            int start = currentPath.lastIndexOf(currentFieldName);
            currentPath = currentPath.delete(start, currentPath.length());
            if (currentPath.length() > 0 && currentPath.charAt(currentPath.length() - 1) == '.') {
                currentPath = currentPath.deleteCharAt(currentPath.length() - 1);
            }
            currentFieldName = currentPath.toString();
            includeKey = false;
            includeLeaf = false;
        }

    }

}
