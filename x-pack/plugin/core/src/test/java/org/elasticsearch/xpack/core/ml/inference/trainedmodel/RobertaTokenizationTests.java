/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.junit.Before;

import java.io.IOException;

public class RobertaTokenizationTests extends AbstractBWCSerializationTestCase<RobertaTokenization> {

    private boolean lenient;

    public static RobertaTokenization mutateForVersion(RobertaTokenization instance, TransportVersion version) {
        if (version.before(TransportVersion.V_8_2_0)) {
            return new RobertaTokenization(
                instance.withSpecialTokens,
                instance.isAddPrefixSpace(),
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
    protected RobertaTokenization doParseInstance(XContentParser parser) throws IOException {
        return RobertaTokenization.createParser(lenient).apply(parser, null);
    }

    @Override
    protected Writeable.Reader<RobertaTokenization> instanceReader() {
        return RobertaTokenization::new;
    }

    @Override
    protected RobertaTokenization createTestInstance() {
        return createRandom();
    }

    @Override
    protected RobertaTokenization mutateInstance(RobertaTokenization instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected RobertaTokenization mutateInstanceForVersion(RobertaTokenization instance, TransportVersion version) {
        return mutateForVersion(instance, version);
    }

    public static RobertaTokenization createRandom() {
        return new RobertaTokenization(
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomIntBetween(1, 1024),
            randomBoolean() ? null : randomFrom(Tokenization.Truncate.values()),
            null
        );
    }

    public static RobertaTokenization createRandomWithSpan() {
        Tokenization.Truncate truncate = randomBoolean() ? null : randomFrom(Tokenization.Truncate.values());
        Integer maxSeq = randomBoolean() ? null : randomIntBetween(1, 1024);
        return new RobertaTokenization(
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomBoolean(),
            maxSeq,
            truncate,
            Tokenization.Truncate.NONE.equals(truncate) && randomBoolean() ? randomIntBetween(0, maxSeq != null ? maxSeq - 1 : 100) : null
        );
    }
}
