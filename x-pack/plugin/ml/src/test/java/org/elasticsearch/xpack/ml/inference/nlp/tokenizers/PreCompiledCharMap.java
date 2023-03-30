/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

record PreCompiledCharMap(String charMapStr) {
    static ParseField PRECOMPILED_CHARSMAP = new ParseField("precompiled_charsmap");
    static ConstructingObjectParser<PreCompiledCharMap, Void> PARSER = new ConstructingObjectParser<>(
        "precompiled_charsmap_config",
        true,
        a -> new PreCompiledCharMap((String) a[0])
    );
    static {
        PARSER.declareString(constructorArg(), PRECOMPILED_CHARSMAP);
    }

    static PreCompiledCharMap fromResource(String resourcePath) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, PreCompiledCharMap.class.getResourceAsStream(resourcePath))
        ) {
            return PreCompiledCharMap.PARSER.apply(parser, null);
        }
    }
}
