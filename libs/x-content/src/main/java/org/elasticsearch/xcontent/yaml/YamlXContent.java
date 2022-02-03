/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.yaml;

import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.spi.XContentProvider;

import java.io.IOException;

/**
 * A YAML based content implementation using Jackson.
 */
public abstract class YamlXContent implements XContent {

    private static final XContentProvider.FormatProvider<YamlXContent> provider = XContentProvider.provider().getYamlXContent();

    public static final XContentBuilder contentBuilder() throws IOException {
        return provider.getContentBuilder();
    }

    public static final YamlXContent yamlXContent = provider.XContent();

}
