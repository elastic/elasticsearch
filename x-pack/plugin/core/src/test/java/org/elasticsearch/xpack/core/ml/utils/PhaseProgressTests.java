/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class PhaseProgressTests extends AbstractXContentSerializingTestCase<PhaseProgress> {

    @Override
    protected PhaseProgress createTestInstance() {
        return createRandom();
    }

    @Override
    protected PhaseProgress mutateInstance(PhaseProgress instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static PhaseProgress createRandom() {
        return new PhaseProgress(randomAlphaOfLength(10), randomIntBetween(0, 100));
    }

    @Override
    protected PhaseProgress doParseInstance(XContentParser parser) throws IOException {
        return PhaseProgress.PARSER.apply(parser, null);
    }

    @Override
    protected Writeable.Reader<PhaseProgress> instanceReader() {
        return PhaseProgress::new;
    }
}
