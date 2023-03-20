/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.streamsmile;

import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.spi.XContentProvider;

import java.io.IOException;

/**
 * Smile based XContent that escapes the stream separator so it can be safely
 * used for streaming smile documents.
 */
public final class StreamSmileXContent {

    private static final XContentProvider.FormatProvider provider = XContentProvider.provider().getStreamSmileXContent();

    private StreamSmileXContent() {}

    /**
     * Returns an {@link XContentBuilder} for building Smile XContent that is safe for streaming documents.
     */
    public static XContentBuilder contentBuilder() throws IOException {
        return provider.getContentBuilder();
    }

    /**
     * A Smile based XContent that can be used safely for streaming documents.
     */
    public static final XContent streamSmileXContent = provider.XContent();

}
