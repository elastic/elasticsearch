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

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;

/**
 * A generic abstraction on top of handling content, inspired by JSON and pull parsing.
 */
public interface XContent {

    /**
     * The type this content handles and produces.
     */
    XContentType type();

    byte streamSeparator();

    /**
     * Creates a new generator using the provided output stream.
     */
    default XContentGenerator createGenerator(OutputStream os) throws IOException {
        return createGenerator(os, null, true);
    }

    /**
     * Creates a new generator using the provided output stream and some
     * inclusive filters. Same as createGenerator(os, filters, true).
     */
    default XContentGenerator createGenerator(OutputStream os, String[] filters) throws IOException {
        return createGenerator(os, filters, true);
    }

    /**
     * Creates a new generator using the provided output stream and some
     * filters.
     *
     * @param inclusive
     *            If true only paths matching a filter will be included in
     *            output. If false no path matching a filter will be included in
     *            output
     */
    XContentGenerator createGenerator(OutputStream os, String[] filters, boolean inclusive) throws IOException;
    /**
     * Creates a parser over the provided string content.
     */
    XContentParser createParser(String content) throws IOException;

    /**
     * Creates a parser over the provided input stream.
     */
    XContentParser createParser(InputStream is) throws IOException;

    /**
     * Creates a parser over the provided bytes.
     */
    XContentParser createParser(byte[] data) throws IOException;

    /**
     * Creates a parser over the provided bytes.
     */
    XContentParser createParser(byte[] data, int offset, int length) throws IOException;

    /**
     * Creates a parser over the provided bytes.
     */
    XContentParser createParser(BytesReference bytes) throws IOException;

    /**
     * Creates a parser over the provided reader.
     */
    XContentParser createParser(Reader reader) throws IOException;

}
