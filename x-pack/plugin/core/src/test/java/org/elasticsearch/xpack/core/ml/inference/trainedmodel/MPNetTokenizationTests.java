/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.junit.Before;

import java.io.IOException;

public class MPNetTokenizationTests extends AbstractBWCSerializationTestCase<MPNetTokenization> {

    private boolean lenient;

    static MPNetTokenization mutateForVersion(MPNetTokenization instance, Version version) {
        if (version.before(Version.V_8_2_0)) {
            return new MPNetTokenization(
                instance.doLowerCase,
                instance.withSpecialTokens,
                instance.maxSequenceLength,
                instance.truncate,
                null
            );
        }
        return instance;
    }

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected MPNetTokenization doParseInstance(XContentParser parser) throws IOException {
        return MPNetTokenization.createParser(lenient).apply(parser, null);
    }

    @Override
    protected Writeable.Reader<MPNetTokenization> instanceReader() {
        return MPNetTokenization::new;
    }

    @Override
    protected MPNetTokenization createTestInstance() {
        return createRandom();
    }

    @Override
    protected MPNetTokenization mutateInstanceForVersion(MPNetTokenization instance, Version version) {
        return mutateForVersion(instance, version);
    }

    public static MPNetTokenization createRandom() {
        return new MPNetTokenization(
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomIntBetween(1, 1024),
            randomBoolean() ? null : randomFrom(Tokenization.Truncate.values()),
            null
        );
    }

    public static MPNetTokenization createRandomWithSpan() {
        Tokenization.Truncate truncate = randomBoolean() ? null : randomFrom(Tokenization.Truncate.values());
        Integer maxSeq = randomBoolean() ? null : randomIntBetween(1, 1024);
        return new MPNetTokenization(
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomBoolean(),
            maxSeq,
            truncate,
            Tokenization.Truncate.NONE.equals(truncate) && randomBoolean() ? randomIntBetween(0, maxSeq != null ? maxSeq - 1 : 100) : null
        );
    }
}
