/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.termsenum.action.TermCount;

import java.io.IOException;

public class TermCountTests extends AbstractSerializingTestCase<TermCount> {

    static TermCount createRandomQueryExplanation(boolean isValid) {
        int docCount = randomInt(100);
        String term = randomAlphaOfLength(randomIntBetween(10, 100));
        return new TermCount(term, docCount);
    }

    static TermCount createRandomQueryExplanation() {
        return createRandomQueryExplanation(randomBoolean());
    }

    @Override
    protected TermCount doParseInstance(XContentParser parser) throws IOException {
        return TermCount.fromXContent(parser);
    }

    @Override
    protected TermCount createTestInstance() {
        return createRandomQueryExplanation();
    }

    @Override
    protected Writeable.Reader<TermCount> instanceReader() {
        return TermCount::new;
    }
}
