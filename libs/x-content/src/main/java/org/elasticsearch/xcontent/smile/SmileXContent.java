/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.smile;

import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.spi.XContentProvider;

import java.io.IOException;

/**
 * Smile based XContent.
 */
public final class SmileXContent {

    private static final XContentProvider.FormatProvider provider = XContentProvider.provider().getSmileXContent();

    private SmileXContent() {}

    /**
     * Returns an {@link XContentBuilder} for building Smile XContent.
     */
    public static XContentBuilder contentBuilder() throws IOException {
        return provider.getContentBuilder();
    }

    /**
     * A Smile based XContent.
     */
    public static final XContent smileXContent = provider.XContent();

}
