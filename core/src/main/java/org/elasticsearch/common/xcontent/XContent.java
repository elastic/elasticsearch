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

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.Set;

/**
 * A generic abstraction on top of handling content, inspired by JSON and pull parsing.
 */
public interface XContent {

    /*
     * NOTE: This comment is only meant for maintainers of the Elasticsearch code base and is intentionally not a Javadoc comment as it
     *       describes an undocumented system property.
     *
     *
     * Determines whether the XContent parser will always check for duplicate keys. This behavior is enabled by default but
     * can be disabled by setting the otherwise undocumented system property "es.xcontent.strict_duplicate_detection to "false".
     *
     * Before we've enabled this mode, we had custom duplicate checks in various parts of the code base. As the user can still disable this
     * mode and fall back to the legacy duplicate checks, we still need to keep the custom duplicate checks around and we also need to keep
     * the tests around.
     *
     * If this fallback via system property is removed one day in the future you can remove all tests that call this method and also remove
     * the corresponding custom duplicate check code.
     *
     */
    static boolean isStrictDuplicateDetectionEnabled() {
        // Don't allow duplicate keys in JSON content by default but let the user opt out
        return Booleans.parseBoolean(System.getProperty("es.xcontent.strict_duplicate_detection", "true"));
    }

    /**
     * The type this content handles and produces.
     */
    XContentType type();

    byte streamSeparator();

    /**
     * Creates a new generator using the provided output stream.
     */
    default XContentGenerator createGenerator(OutputStream os) throws IOException {
        return createGenerator(os, Collections.emptySet(), Collections.emptySet());
    }

    /**
     * Creates a new generator using the provided output stream and some inclusive and/or exclusive filters. When both exclusive and
     * inclusive filters are provided, the underlying generator will first use exclusion filters to remove fields and then will check the
     * remaining fields against the inclusive filters.
     *
     * @param os       the output stream
     * @param includes the inclusive filters: only fields and objects that match the inclusive filters will be written to the output.
     * @param excludes the exclusive filters: only fields and objects that don't match the exclusive filters will be written to the output.
     */
    XContentGenerator createGenerator(OutputStream os, Set<String> includes, Set<String> excludes) throws IOException;

    /**
     * Creates a parser over the provided string content.
     */
    XContentParser createParser(NamedXContentRegistry xContentRegistry, String content) throws IOException;

    /**
     * Creates a parser over the provided input stream.
     */
    XContentParser createParser(NamedXContentRegistry xContentRegistry, InputStream is) throws IOException;

    /**
     * Creates a parser over the provided bytes.
     */
    XContentParser createParser(NamedXContentRegistry xContentRegistry, byte[] data) throws IOException;

    /**
     * Creates a parser over the provided bytes.
     */
    XContentParser createParser(NamedXContentRegistry xContentRegistry, byte[] data, int offset, int length) throws IOException;

    /**
     * Creates a parser over the provided bytes.
     */
    XContentParser createParser(NamedXContentRegistry xContentRegistry, BytesReference bytes) throws IOException;

    /**
     * Creates a parser over the provided reader.
     */
    XContentParser createParser(NamedXContentRegistry xContentRegistry, Reader reader) throws IOException;

}
