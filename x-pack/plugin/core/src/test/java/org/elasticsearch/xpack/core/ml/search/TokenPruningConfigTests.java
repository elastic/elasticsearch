/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.search;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.mapper.vectors.TokenPruningConfig;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class TokenPruningConfigTests extends AbstractXContentSerializingTestCase<TokenPruningConfig> {

    public static TokenPruningConfig testInstance() {
        return new TokenPruningConfig(randomIntBetween(1, 100), randomFloat(), randomBoolean());
    }

    @Override
    protected Writeable.Reader<TokenPruningConfig> instanceReader() {
        return TokenPruningConfig::new;
    }

    @Override
    protected TokenPruningConfig createTestInstance() {
        return testInstance();
    }

    @Override
    protected TokenPruningConfig mutateInstance(TokenPruningConfig instance) throws IOException {
        return null;
    }

    @Override
    protected TokenPruningConfig doParseInstance(XContentParser parser) throws IOException {
        return TokenPruningConfig.fromXContent(parser);
    }
}
