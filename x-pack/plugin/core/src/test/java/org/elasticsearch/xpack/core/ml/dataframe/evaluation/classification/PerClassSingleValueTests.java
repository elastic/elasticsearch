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

public class PerClassSingleValueTests extends AbstractXContentSerializingTestCase<PerClassSingleValue> {

    @Override
    protected PerClassSingleValue doParseInstance(XContentParser parser) throws IOException {
        return PerClassSingleValue.PARSER.apply(parser, null);
    }

    @Override
    protected Writeable.Reader<PerClassSingleValue> instanceReader() {
        return PerClassSingleValue::new;
    }

    @Override
    protected PerClassSingleValue createTestInstance() {
        return new PerClassSingleValue(randomAlphaOfLength(10), randomDouble());
    }

    @Override
    protected PerClassSingleValue mutateInstance(PerClassSingleValue instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
