/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference.preprocessing;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class NGramTests extends AbstractXContentTestCase<NGram> {

    @Override
    protected NGram doParseInstance(XContentParser parser) throws IOException {
        return NGram.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected NGram createTestInstance() {
        return createRandom();
    }

    public static NGram createRandom() {
        int length = randomIntBetween(1, 10);
        return new NGram(randomAlphaOfLength(10),
            IntStream.range(1, Math.min(5, length + 1)).limit(5).boxed().collect(Collectors.toList()),
            randomBoolean() ? null : randomIntBetween(0, 10),
            randomBoolean() ? null : length,
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomAlphaOfLength(10));
    }

}
