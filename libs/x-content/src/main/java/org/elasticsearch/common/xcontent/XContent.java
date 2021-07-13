/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.core.RestApiVersion;

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
    XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, String content)
        throws IOException;

    /**
     * Creates a parser over the provided input stream.
     */
    XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, InputStream is)
        throws IOException;

    /**
     * Creates a parser over the provided bytes.
     */
    XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, byte[] data)
        throws IOException;

    /**
     * Creates a parser over the provided bytes.
     */
    XContentParser createParser(NamedXContentRegistry xContentRegistry,
                                DeprecationHandler deprecationHandler, byte[] data, int offset, int length) throws IOException;

    /**
     * Creates a parser over the provided reader.
     */
    XContentParser createParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, Reader reader)
        throws IOException;

    /**
     * Creates a parser over the provided input stream and with the indication that a request is using REST compatible API.
     *
     * @param restApiVersion - indicates if the N-1 or N compatible XContent parsing logic will be used.
     */
    XContentParser createParserForCompatibility(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler,
                                                InputStream is, RestApiVersion restApiVersion) throws IOException;

    XContentParser createParserForCompatibility(NamedXContentRegistry xContentRegistry,
                                DeprecationHandler deprecationHandler, byte[] data, int offset, int length,
                                                RestApiVersion restApiVersion) throws IOException;

}
