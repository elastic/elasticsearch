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

import static org.hamcrest.Matchers.containsString;

public class BertTokenizationTests extends AbstractBWCSerializationTestCase<BertTokenization> {

    private boolean lenient;

    public static BertTokenization mutateForVersion(BertTokenization instance, TransportVersion version) {
        if (version.before(TransportVersions.V_8_2_0)) {
            return new BertTokenization(
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
    protected BertTokenization doParseInstance(XContentParser parser) throws IOException {
        return BertTokenization.createParser(lenient).apply(parser, null);
    }

    @Override
    protected Writeable.Reader<BertTokenization> instanceReader() {
        return BertTokenization::new;
    }

    @Override
    protected BertTokenization createTestInstance() {
        return createRandom();
    }

    @Override
    protected BertTokenization mutateInstance(BertTokenization instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected BertTokenization mutateInstanceForVersion(BertTokenization instance, TransportVersion version) {
        return mutateForVersion(instance, version);
    }

    public void testsBuildUpdatedTokenization() {
        var update = new BertTokenization(true, true, 100, Tokenization.Truncate.FIRST, -1).buildWindowingTokenization(50, 20);
        assertEquals(Tokenization.Truncate.NONE, update.getTruncate());
        assertEquals(50, update.maxSequenceLength());
        assertEquals(20, update.getSpan());
    }

    public void testUpdateWindowSettings() {
        var tokenization = new BertTokenization(true, true, 100, Tokenization.Truncate.FIRST, -1);
        {
            var update = tokenization.updateWindowSettings(new Tokenization.SpanSettings((Integer) null));
            // settings not changed
            assertEquals(tokenization.getMaxSequenceLength(), update.getMaxSequenceLength());
            assertEquals(tokenization.getSpan(), update.getSpan());
        }
        {
            var update = tokenization.updateWindowSettings(new Tokenization.SpanSettings(20));
            assertEquals(20, update.getMaxSequenceLength());
            assertEquals(tokenization.getSpan(), update.getSpan());
        }
        {
            var update = tokenization.updateWindowSettings(new Tokenization.SpanSettings(null, 10));
            assertEquals(tokenization.getMaxSequenceLength(), update.getMaxSequenceLength());
            assertEquals(10, update.getSpan());
        }
        {
            var update = tokenization.updateWindowSettings(new Tokenization.SpanSettings(20, 10));
            assertEquals(20, update.getMaxSequenceLength());
            assertEquals(10, update.getSpan());
        }
    }

    public void testUpdateWindowSettings_InvalidSpan() {
        var tokenization = new BertTokenization(true, true, 100, Tokenization.Truncate.FIRST, -1);
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> tokenization.updateWindowSettings(new Tokenization.SpanSettings(32, 64))
        );
        assertThat(e.getMessage(), containsString("[span] provided [64] must not be greater than [max_sequence_length] provided [32]"));
    }

    public void testUpdateWindowSettings_InvalidWindowSize() {
        var tokenization = new BertTokenization(true, true, 100, Tokenization.Truncate.FIRST, -1);
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> tokenization.updateWindowSettings(new Tokenization.SpanSettings(32, 64))
        );
        assertThat(e.getMessage(), containsString("[span] provided [64] must not be greater than [max_sequence_length] provided [32]"));
    }

    public static BertTokenization createRandom() {
        return new BertTokenization(
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomIntBetween(1, 1024),
            randomBoolean() ? null : randomFrom(Tokenization.Truncate.values()),
            null
        );
    }

    public static BertTokenization createRandomWithSpan() {
        Tokenization.Truncate truncate = randomBoolean() ? null : randomFrom(Tokenization.Truncate.values());
        Integer maxSeq = randomBoolean() ? null : randomIntBetween(1, 1024);
        return new BertTokenization(
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomBoolean(),
            maxSeq,
            truncate,
            Tokenization.Truncate.NONE.equals(truncate) && randomBoolean() ? randomIntBetween(0, maxSeq != null ? maxSeq - 1 : 100) : null
        );
    }
}
