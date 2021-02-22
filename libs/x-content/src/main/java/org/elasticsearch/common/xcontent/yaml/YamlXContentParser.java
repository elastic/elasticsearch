/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent.yaml;

import com.fasterxml.jackson.core.JsonParser;
import org.elasticsearch.common.compatibility.RestApiCompatibleVersion;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContentParser;

public class YamlXContentParser extends JsonXContentParser {

    public YamlXContentParser(NamedXContentRegistry xContentRegistry,
            DeprecationHandler deprecationHandler, JsonParser parser) {
        super(xContentRegistry, deprecationHandler, parser);
    }

    public YamlXContentParser(NamedXContentRegistry xContentRegistry,
                              DeprecationHandler deprecationHandler, JsonParser parser,
                              RestApiCompatibleVersion restApiCompatibleVersion) {
        super(xContentRegistry, deprecationHandler, parser, restApiCompatibleVersion);
    }

    @Override
    public XContentType contentType() {
        return XContentType.YAML;
    }
}
