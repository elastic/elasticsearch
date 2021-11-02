/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Returns a stream of json log lines.
 * This is intended to be used for easy and readable assertions for logger tests
 */
public class JsonLogsStream {
    private final XContentParser parser;
    private final BufferedReader reader;
    private final ObjectParser<JsonLogLine, Void> logLineParser;

    private JsonLogsStream(BufferedReader reader, ObjectParser<JsonLogLine, Void> logLineParser) throws IOException {
        this.reader = reader;
        this.parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, reader);
        this.logLineParser = logLineParser;
    }

    public static Stream<JsonLogLine> from(BufferedReader reader, ObjectParser<JsonLogLine, Void> logLineParser) throws IOException {
        return new JsonLogsStream(reader, logLineParser).stream();
    }

    public static Stream<JsonLogLine> from(BufferedReader reader) throws IOException {
        return new JsonLogsStream(reader, JsonLogLine.ECS_LOG_LINE).stream();
    }

    public static Stream<JsonLogLine> from(Path path) throws IOException {
        return from(Files.newBufferedReader(path));
    }

    public static Stream<Map<String, String>> mapStreamFrom(Path path) throws IOException {
        return new JsonLogsStream(Files.newBufferedReader(path), JsonLogLine.ECS_LOG_LINE).streamMap();
    }

    private Stream<JsonLogLine> stream() {
        Spliterator<JsonLogLine> spliterator = Spliterators.spliteratorUnknownSize(new JsonIterator(), Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, false).onClose(this::close);
    }

    private Stream<Map<String, String>> streamMap() {
        Spliterator<Map<String, String>> spliterator = Spliterators.spliteratorUnknownSize(new MapIterator(), Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, false).onClose(this::close);
    }

    private void close() {
        try {
            parser.close();
            reader.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private class MapIterator implements Iterator<Map<String, String>> {

        @Override
        public boolean hasNext() {
            return parser.isClosed() == false;
        }

        @Override
        public Map<String, String> next() {
            Map<String, String> map;
            try {
                map = parser.map(LinkedHashMap::new, XContentParser::text);
                parser.nextToken();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return map;
        }
    }

    private class JsonIterator implements Iterator<JsonLogLine> {

        @Override
        public boolean hasNext() {
            return parser.isClosed() == false;
        }

        @Override
        public JsonLogLine next() {
            JsonLogLine apply = logLineParser.apply(parser, null);
            nextToken();
            return apply;
        }

        private void nextToken() {
            try {
                parser.nextToken();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
