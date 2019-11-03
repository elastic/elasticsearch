/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.logging;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

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

    private JsonLogsStream(BufferedReader reader) throws IOException {
        this.reader = reader;
        this.parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            reader);
    }

    public static Stream<JsonLogLine> from(BufferedReader reader) throws IOException {
        return new JsonLogsStream(reader).stream();
    }

    public static Stream<JsonLogLine> from(Path path) throws IOException {
        return from(Files.newBufferedReader(path));
    }

    public static Stream<Map<String, String>> mapStreamFrom(Path path) throws IOException {
        return new JsonLogsStream(Files.newBufferedReader(path)).streamMap();
    }

    private Stream<JsonLogLine> stream() {
        Spliterator<JsonLogLine> spliterator = Spliterators.spliteratorUnknownSize(new JsonIterator(), Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, false)
            .onClose(this::close);
    }

    private Stream<Map<String, String>> streamMap() {
        Spliterator<Map<String, String>> spliterator = Spliterators.spliteratorUnknownSize(new MapIterator(), Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, false)
            .onClose(this::close);
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
            JsonLogLine apply = JsonLogLine.PARSER.apply(parser, null);
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
