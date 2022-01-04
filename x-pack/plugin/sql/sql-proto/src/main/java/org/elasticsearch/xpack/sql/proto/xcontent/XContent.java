/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.xcontent;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;

/**
 * NB: Light-clone from XContent library to keep JDBC driver independent.
 *
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
    XContentGenerator createGenerator(OutputStream os) throws IOException;

    /**
     * Creates a parser over the provided string content.
     */
    XContentParser createParser(XContentParserConfiguration config, String content) throws IOException;

    /**
     * Creates a parser over the provided string content.
     * @deprecated Use {@link #createParser(XContentParserConfiguration, InputStream)}
     */
    @Deprecated
    default XContentParser createParser(NamedXContentRegistry registry, DeprecationHandler deprecationHandler, String content)
        throws IOException {
        return createParser(XContentParserConfiguration.EMPTY.withRegistry(registry).withDeprecationHandler(deprecationHandler), content);
    }

    /**
     * Creates a parser over the provided input stream.
     */
    XContentParser createParser(XContentParserConfiguration config, InputStream is) throws IOException;

    /**
     * Creates a parser over the provided input stream.
     * @deprecated Use {@link #createParser(XContentParserConfiguration, InputStream)}
     */
    @Deprecated
    default XContentParser createParser(NamedXContentRegistry registry, DeprecationHandler deprecationHandler, InputStream is)
        throws IOException {
        return createParser(XContentParserConfiguration.EMPTY.withRegistry(registry).withDeprecationHandler(deprecationHandler), is);
    }

    /**
     * Creates a parser over the provided bytes.
     */
    default XContentParser createParser(XContentParserConfiguration config, byte[] data) throws IOException {
        return createParser(config, data, 0, data.length);
    }

    /**
     * Creates a parser over the provided bytes.
     * @deprecated Use {@link #createParser(XContentParserConfiguration, byte[])}
     */
    @Deprecated
    default XContentParser createParser(NamedXContentRegistry registry, DeprecationHandler deprecationHandler, byte[] data)
        throws IOException {
        return createParser(XContentParserConfiguration.EMPTY.withRegistry(registry).withDeprecationHandler(deprecationHandler), data);
    }

    /**
     * Creates a parser over the provided bytes.
     */
    XContentParser createParser(XContentParserConfiguration config, byte[] data, int offset, int length) throws IOException;

    /**
     * Creates a parser over the provided reader.
     */
    XContentParser createParser(XContentParserConfiguration config, Reader reader) throws IOException;
}
