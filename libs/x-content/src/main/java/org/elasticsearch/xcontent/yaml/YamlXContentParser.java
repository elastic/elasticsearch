/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.yaml;

import com.fasterxml.jackson.core.JsonParser;

import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContentParser;
import org.elasticsearch.xcontent.support.filtering.FilterPath;

public class YamlXContentParser extends JsonXContentParser {

    public YamlXContentParser(NamedXContentRegistry xContentRegistry, DeprecationHandler deprecationHandler, JsonParser parser) {
        super(xContentRegistry, deprecationHandler, parser);
    }

    public YamlXContentParser(
        NamedXContentRegistry xContentRegistry,
        DeprecationHandler deprecationHandler,
        JsonParser parser,
        FilterPath[] includes,
        FilterPath[] excludes
    ) {
        super(xContentRegistry, deprecationHandler, parser, includes, excludes);
    }

    @Override
    public XContentType contentType() {
        return XContentType.YAML;
    }
}
