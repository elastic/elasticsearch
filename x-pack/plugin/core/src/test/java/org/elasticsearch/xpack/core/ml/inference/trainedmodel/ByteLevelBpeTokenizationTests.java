/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.ByteLevelBpeTokenization.ML_BYTE_LEVEL_BPE_TOKENIZATION_ADDED;

public class ByteLevelBpeTokenizationTests extends AbstractBWCSerializationTestCase<ByteLevelBpeTokenization> {

    private boolean lenient;

    public static ByteLevelBpeTokenization mutateForVersion(ByteLevelBpeTokenization instance, TransportVersion version) {
        return instance;
    }

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    /**
     * Versions before {@link ByteLevelBpeTokenization#ML_BYTE_LEVEL_BPE_TOKENIZATION_ADDED} reject serialization; filtering avoids
     * spurious BWC failures. {@link #testByteLevelBpeTokenizationIsNotBackwardsCompatible} covers the explicit failure path.
     */
    @Override
    protected Collection<TransportVersion> bwcVersions() {
        return super.bwcVersions().stream().filter(version -> version.supports(ML_BYTE_LEVEL_BPE_TOKENIZATION_ADDED)).toList();
    }

    public void testByteLevelBpeTokenizationIsNotBackwardsCompatible() throws IOException {
        testSerializationIsNotBackwardsCompatible(ML_BYTE_LEVEL_BPE_TOKENIZATION_ADDED, instance -> true, """
            Cannot send byte_level_bpe tokenization to an older node. \
            Please wait until all nodes are upgraded before using byte_level_bpe tokenization""");
    }

    @Override
    protected ByteLevelBpeTokenization doParseInstance(XContentParser parser) throws IOException {
        return ByteLevelBpeTokenization.fromXContent(parser, lenient);
    }

    @Override
    protected Writeable.Reader<ByteLevelBpeTokenization> instanceReader() {
        return ByteLevelBpeTokenization::new;
    }

    @Override
    protected ByteLevelBpeTokenization createTestInstance() {
        return createRandom();
    }

    @Override
    protected ByteLevelBpeTokenization mutateInstance(ByteLevelBpeTokenization instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected ByteLevelBpeTokenization mutateInstanceForVersion(ByteLevelBpeTokenization instance, TransportVersion version) {
        return mutateForVersion(instance, version);
    }

    public void testBuildUpdatedTokenization() {
        var update = new ByteLevelBpeTokenization(false, true, 100, Tokenization.Truncate.FIRST, -1, false, null, null, null, null, null)
            .buildWindowingTokenization(50, 20);
        assertEquals(Tokenization.Truncate.NONE, update.getTruncate());
        assertEquals(50, update.maxSequenceLength());
        assertEquals(20, update.getSpan());
    }

    public void testDoLowerCaseTrueRejected() {
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> new ByteLevelBpeTokenization(true, true, 512, Tokenization.Truncate.FIRST, -1, false, null, null, null, null, null)
        );
        assertEquals("unable to set [do_lower_case] to [true] for byte_level_bpe tokenizer", e.getMessage());
    }

    public static ByteLevelBpeTokenization createRandom() {
        return new ByteLevelBpeTokenization(
            randomBoolean() ? null : false,
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomIntBetween(1, 1024),
            randomBoolean() ? null : randomFrom(Tokenization.Truncate.values()),
            null,
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(2, 8),
            randomBoolean() ? null : randomAlphaOfLengthBetween(2, 8),
            randomBoolean() ? null : randomAlphaOfLengthBetween(2, 8),
            randomBoolean() ? null : randomAlphaOfLengthBetween(2, 8),
            randomBoolean() ? null : randomAlphaOfLengthBetween(2, 8)
        );
    }

    public static ByteLevelBpeTokenization createRandomWithSpan() {
        Tokenization.Truncate truncate = randomBoolean() ? null : randomFrom(Tokenization.Truncate.values());
        Integer maxSeq = randomBoolean() ? null : randomIntBetween(1, 1024);
        return new ByteLevelBpeTokenization(
            randomBoolean() ? null : false,
            randomBoolean() ? null : randomBoolean(),
            maxSeq,
            truncate,
            Tokenization.Truncate.NONE.equals(truncate) && randomBoolean() ? randomIntBetween(0, maxSeq != null ? maxSeq - 1 : 100) : null,
            randomBoolean() ? null : randomBoolean(),
            null,
            null,
            null,
            null,
            null
        );
    }
}
