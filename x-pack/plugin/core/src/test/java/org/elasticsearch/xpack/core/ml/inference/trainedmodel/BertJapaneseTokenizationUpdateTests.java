/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import static org.hamcrest.Matchers.sameInstance;

public class BertJapaneseTokenizationUpdateTests extends AbstractBWCWireSerializationTestCase<BertJapaneseTokenizationUpdate> {

    public static BertJapaneseTokenizationUpdate randomInstance() {
        Integer span = randomBoolean() ? null : randomIntBetween(8, 128);
        Tokenization.Truncate truncate = randomBoolean() ? null : randomFrom(Tokenization.Truncate.values());

        if (truncate != Tokenization.Truncate.NONE) {
            span = null;
        }
        return new BertJapaneseTokenizationUpdate(truncate, span);
    }

    public void testApply() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new BertJapaneseTokenizationUpdate(Tokenization.Truncate.SECOND, 100).apply(BertJapaneseTokenizationTests.createRandom())
        );

        var updatedSpan = new BertJapaneseTokenizationUpdate(null, 100).apply(
            new BertJapaneseTokenization(false, false, 512, Tokenization.Truncate.NONE, 50)
        );
        assertEquals(new BertJapaneseTokenization(false, false, 512, Tokenization.Truncate.NONE, 100), updatedSpan);

        var updatedTruncate = new BertJapaneseTokenizationUpdate(Tokenization.Truncate.FIRST, null).apply(
            new BertJapaneseTokenization(true, true, 512, Tokenization.Truncate.SECOND, null)
        );
        assertEquals(new BertJapaneseTokenization(true, true, 512, Tokenization.Truncate.FIRST, null), updatedTruncate);

        var updatedNone = new BertJapaneseTokenizationUpdate(Tokenization.Truncate.NONE, null).apply(
            new BertJapaneseTokenization(true, true, 512, Tokenization.Truncate.SECOND, null)
        );
        assertEquals(new BertJapaneseTokenization(true, true, 512, Tokenization.Truncate.NONE, null), updatedNone);

        var unmodified = new BertJapaneseTokenization(true, true, 512, Tokenization.Truncate.NONE, null);
        assertThat(new BertJapaneseTokenizationUpdate(null, null).apply(unmodified), sameInstance(unmodified));
    }

    public void testNoop() {
        assertTrue(new BertJapaneseTokenizationUpdate(null, null).isNoop());
        assertFalse(new BertJapaneseTokenizationUpdate(Tokenization.Truncate.SECOND, null).isNoop());
        assertFalse(new BertJapaneseTokenizationUpdate(null, 10).isNoop());
        assertFalse(new BertJapaneseTokenizationUpdate(Tokenization.Truncate.NONE, 10).isNoop());
    }

    @Override
    protected Writeable.Reader<BertJapaneseTokenizationUpdate> instanceReader() {
        return BertJapaneseTokenizationUpdate::new;
    }

    @Override
    protected BertJapaneseTokenizationUpdate createTestInstance() {
        return randomInstance();
    }

    @Override
    protected BertJapaneseTokenizationUpdate mutateInstance(BertJapaneseTokenizationUpdate instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected BertJapaneseTokenizationUpdate mutateInstanceForVersion(BertJapaneseTokenizationUpdate instance, TransportVersion version) {
        if (version.before(TransportVersion.V_8_2_0)) {
            return new BertJapaneseTokenizationUpdate(instance.getTruncate(), null);
        }

        return instance;
    }
}
