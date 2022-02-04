/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.spi;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.cbor.CborXContent;
import org.elasticsearch.xcontent.internal.ProviderLocator;
import org.elasticsearch.xcontent.json.JsonStringEncoder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xcontent.smile.SmileXContent;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.io.IOException;

public interface XContentProvider {

    interface FormatProvider<T> {
        XContentBuilder getContentBuilder() throws IOException;

        T XContent();
    }

    FormatProvider<CborXContent> getCborXContent();

    FormatProvider<JsonXContent> getJsonXContent();

    FormatProvider<SmileXContent> getSmileXContent();

    FormatProvider<YamlXContent> getYamlXContent();

    XContentParserConfiguration empty();

    JsonStringEncoder getJsonStringEncoder();

    static XContentProvider provider() {
        return ProviderLocator.INSTANCE;
    }
}
