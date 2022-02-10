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
public abstract class SmileXContent implements XContent {

    private static final XContentProvider.FormatProvider<SmileXContent> provider = XContentProvider.provider().getSmileXContent();

    /**
     * Returns a {@link XContentBuilder} for building Smile XContent.
     */
    public static final XContentBuilder contentBuilder() throws IOException {
        return provider.getContentBuilder();
    }

    /**
     * A Smile based XContent.
     */
    public static final SmileXContent smileXContent = provider.XContent();

}
