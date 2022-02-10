/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.spi;

import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.internal.ProviderLocator;
import org.elasticsearch.xcontent.json.JsonStringEncoder;

import java.io.IOException;

/**
 * A provider for the XContent API.
 *
 * A provider supports all the XContent formats, JSON, CBOR, SMILE, and YAML.
 */
public interface XContentProvider {

    /**
     * A provider of a specific content format, e.g. JSON
     */
    interface FormatProvider {
        /**
         * Returns a {@link XContentBuilder} for building the format specific content.
         */
        XContentBuilder getContentBuilder() throws IOException;

        /**
         * Returns an instance of the format specific content.
         */
        XContent XContent();
    }

    /**
     * Returns the CBOR format provider.
     */
    FormatProvider getCborXContent();

    /**
     * Returns the JSON format provider.
     */
    FormatProvider getJsonXContent();

    /**
     * Returns the SMILE format provider.
     */
    FormatProvider getSmileXContent();

    /**
     * Returns the YAML format provider.
     */
    FormatProvider getYamlXContent();

    /**
     * Returns an empty XContentParserConfiguration.
     */
    XContentParserConfiguration empty();

    /**
     * Returns a JsonStringEncoder.
     */
    JsonStringEncoder getJsonStringEncoder();

    /**
     * Returns the located provider instance.
     */
    static XContentProvider provider() {
        return ProviderLocator.INSTANCE;
    }
}
