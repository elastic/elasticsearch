/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class AucRocTests extends AbstractXContentSerializingTestCase<AucRoc> {

    @Override
    protected AucRoc doParseInstance(XContentParser parser) throws IOException {
        return AucRoc.PARSER.apply(parser, null);
    }

    @Override
    protected AucRoc createTestInstance() {
        return createRandom();
    }

    @Override
    protected AucRoc mutateInstance(AucRoc instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<AucRoc> instanceReader() {
        return AucRoc::new;
    }

    public static AucRoc createRandom() {
        return new AucRoc(randomBoolean() ? randomBoolean() : null, randomAlphaOfLength(randomIntBetween(2, 10)));
    }
}
