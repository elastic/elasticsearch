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
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class JsonLogs implements Iterable<JsonLogLine>, Closeable {

    private final XContentParser parser;
    private final BufferedReader reader;

    public JsonLogs(BufferedReader reader) throws IOException {
        this.reader = reader;
        this.parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            reader);
    }

    public JsonLogs(Path path) throws IOException {
        this(Files.newBufferedReader(path));
    }

    @Override
    public Iterator<JsonLogLine> iterator() {
        return new JsonIterator();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private class JsonIterator implements Iterator<JsonLogLine> {

        @Override
        public boolean hasNext() {
            return !parser.isClosed();
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
                throw new RuntimeException(e);
            }
        }
    }
}
