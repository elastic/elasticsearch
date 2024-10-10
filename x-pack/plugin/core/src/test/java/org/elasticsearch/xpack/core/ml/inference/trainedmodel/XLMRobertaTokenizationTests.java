/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Before;

import java.io.IOException;

public class XLMRobertaTokenizationTests extends AbstractBWCSerializationTestCase<XLMRobertaTokenization> {

    private boolean lenient;

    public static XLMRobertaTokenization mutateForVersion(XLMRobertaTokenization instance, TransportVersion version) {
        if (version.before(TransportVersions.V_8_2_0)) {
            return new XLMRobertaTokenization(instance.withSpecialTokens, instance.maxSequenceLength, instance.truncate, null);
        }
        return instance;
    }

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected XLMRobertaTokenization doParseInstance(XContentParser parser) throws IOException {
        return XLMRobertaTokenization.createParser(lenient).apply(parser, null);
    }

    @Override
    protected Writeable.Reader<XLMRobertaTokenization> instanceReader() {
        return XLMRobertaTokenization::new;
    }

    @Override
    protected XLMRobertaTokenization createTestInstance() {
        return createRandom();
    }

    @Override
    protected XLMRobertaTokenization mutateInstance(XLMRobertaTokenization instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected XLMRobertaTokenization mutateInstanceForVersion(XLMRobertaTokenization instance, TransportVersion version) {
        return mutateForVersion(instance, version);
    }

    public static XLMRobertaTokenization createRandom() {
        return new XLMRobertaTokenization(
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomIntBetween(1, 1024),
            randomBoolean() ? null : randomFrom(Tokenization.Truncate.values()),
            null
        );
    }

    public static XLMRobertaTokenization createRandomWithSpan() {
        Tokenization.Truncate truncate = randomBoolean() ? null : randomFrom(Tokenization.Truncate.values());
        Integer maxSeq = randomBoolean() ? null : randomIntBetween(1, 1024);
        return new XLMRobertaTokenization(
            randomBoolean() ? null : randomBoolean(),
            maxSeq,
            truncate,
            Tokenization.Truncate.NONE.equals(truncate) && randomBoolean() ? randomIntBetween(0, maxSeq != null ? maxSeq - 1 : 100) : null
        );
    }
}
