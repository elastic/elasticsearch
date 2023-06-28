/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Before;

import java.io.IOException;

public class NamedQueryExtractorBuilderTests extends AbstractXContentSerializingTestCase<NamedQueryExtractorBuilder> {

    protected boolean lenient;

    public static NamedQueryExtractorBuilder randomInstance() {
        return new NamedQueryExtractorBuilder(randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected Writeable.Reader<NamedQueryExtractorBuilder> instanceReader() {
        return NamedQueryExtractorBuilder::new;
    }

    @Override
    protected NamedQueryExtractorBuilder createTestInstance() {
        return randomInstance();
    }

    @Override
    protected NamedQueryExtractorBuilder mutateInstance(NamedQueryExtractorBuilder instance) throws IOException {
        int i = randomInt(1);
        return switch (i) {
            case 0 -> new NamedQueryExtractorBuilder(randomAlphaOfLength(10), instance.featureName());
            case 1 -> new NamedQueryExtractorBuilder(instance.queryName(), randomAlphaOfLength(10));
            default -> throw new AssertionError("unknown random case for instance mutation");
        };
    }

    @Override
    protected NamedQueryExtractorBuilder doParseInstance(XContentParser parser) throws IOException {
        return NamedQueryExtractorBuilder.fromXContent(parser, lenient);
    }
}
