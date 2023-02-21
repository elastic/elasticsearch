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

public class BertTokenizationUpdateTests extends AbstractBWCWireSerializationTestCase<BertTokenizationUpdate> {

    public static BertTokenizationUpdate randomInstance() {
        Integer span = randomBoolean() ? null : randomIntBetween(8, 128);
        Tokenization.Truncate truncate = randomBoolean() ? null : randomFrom(Tokenization.Truncate.values());

        if (truncate != Tokenization.Truncate.NONE) {
            span = null;
        }
        return new BertTokenizationUpdate(truncate, span);
    }

    public void testApply() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new BertTokenizationUpdate(Tokenization.Truncate.SECOND, 100).apply(BertTokenizationTests.createRandom())
        );

        var updatedSpan = new BertTokenizationUpdate(null, 100).apply(
            new BertTokenization(false, false, 512, Tokenization.Truncate.NONE, 50)
        );
        assertEquals(new BertTokenization(false, false, 512, Tokenization.Truncate.NONE, 100), updatedSpan);

        var updatedTruncate = new BertTokenizationUpdate(Tokenization.Truncate.FIRST, null).apply(
            new BertTokenization(true, true, 512, Tokenization.Truncate.SECOND, null)
        );
        assertEquals(new BertTokenization(true, true, 512, Tokenization.Truncate.FIRST, null), updatedTruncate);

        var updatedNone = new BertTokenizationUpdate(Tokenization.Truncate.NONE, null).apply(
            new BertTokenization(true, true, 512, Tokenization.Truncate.SECOND, null)
        );
        assertEquals(new BertTokenization(true, true, 512, Tokenization.Truncate.NONE, null), updatedNone);

        var unmodified = new BertTokenization(true, true, 512, Tokenization.Truncate.NONE, null);
        assertThat(new BertTokenizationUpdate(null, null).apply(unmodified), sameInstance(unmodified));
    }

    public void testNoop() {
        assertTrue(new BertTokenizationUpdate(null, null).isNoop());
        assertFalse(new BertTokenizationUpdate(Tokenization.Truncate.SECOND, null).isNoop());
        assertFalse(new BertTokenizationUpdate(null, 10).isNoop());
        assertFalse(new BertTokenizationUpdate(Tokenization.Truncate.NONE, 10).isNoop());
    }

    @Override
    protected Writeable.Reader<BertTokenizationUpdate> instanceReader() {
        return BertTokenizationUpdate::new;
    }

    @Override
    protected BertTokenizationUpdate createTestInstance() {
        return randomInstance();
    }

    @Override
    protected BertTokenizationUpdate mutateInstance(BertTokenizationUpdate instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected BertTokenizationUpdate mutateInstanceForVersion(BertTokenizationUpdate instance, TransportVersion version) {
        if (version.before(TransportVersion.V_8_2_0)) {
            return new BertTokenizationUpdate(instance.getTruncate(), null);
        }

        return instance;
    }
}
