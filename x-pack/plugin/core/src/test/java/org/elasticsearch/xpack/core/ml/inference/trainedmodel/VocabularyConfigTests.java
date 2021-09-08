/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.junit.Before;

import java.io.IOException;

public class VocabularyConfigTests extends AbstractBWCSerializationTestCase<VocabularyConfig> {

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected VocabularyConfig doParseInstance(XContentParser parser) throws IOException {
        return VocabularyConfig.createParser(lenient).apply(parser, null);
    }

    @Override
    protected Writeable.Reader<VocabularyConfig> instanceReader() {
        return VocabularyConfig::new;
    }

    @Override
    protected VocabularyConfig createTestInstance() {
        return createRandom();
    }

    @Override
    protected VocabularyConfig mutateInstanceForVersion(VocabularyConfig instance, Version version) {
        return instance;
    }

    public static VocabularyConfig createRandom() {
        return new VocabularyConfig(randomAlphaOfLength(10), randomAlphaOfLength(10));
    }
}
