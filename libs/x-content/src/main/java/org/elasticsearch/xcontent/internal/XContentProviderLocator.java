/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.internal;

import org.elasticsearch.xcontent.spi.XContentProvider;

/**
 * A provider locator for finding the {@link XContentProvider}.
 */
public class XContentProviderLocator {

    /**
     * Returns the provider instance.
     */
    public static final XContentProvider INSTANCE = provider();

    static final String PROVIDER_NAME = "x-content";

    static final String PROVIDER_MODULE_NAME = "org.elasticsearch.xcontent.impl";

    private static XContentProvider provider() {
        return (new ProviderLocator<>(PROVIDER_NAME, XContentProvider.class, PROVIDER_MODULE_NAME)).get();
    }
}
