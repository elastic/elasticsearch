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

public class ByteLevelBpeTokenizationUpdateTests extends AbstractBWCWireSerializationTestCase<ByteLevelBpeTokenizationUpdate> {

    public static ByteLevelBpeTokenizationUpdate randomInstance() {
        Integer span = randomBoolean() ? null : randomIntBetween(8, 128);
        Tokenization.Truncate truncate = randomBoolean() ? null : randomFrom(Tokenization.Truncate.values());

        if (truncate != null && truncate != Tokenization.Truncate.NONE) {
            span = null;
        }
        return new ByteLevelBpeTokenizationUpdate(truncate, span);
    }

    public void testApply() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new ByteLevelBpeTokenizationUpdate(Tokenization.Truncate.SECOND, 100).apply(ByteLevelBpeTokenizationTests.createRandom())
        );

        var updatedSpan = new ByteLevelBpeTokenizationUpdate(null, 100).apply(
            new ByteLevelBpeTokenization(false, false, 512, Tokenization.Truncate.NONE, 50, false, null, null, null, null, null)
        );
        assertEquals(
            new ByteLevelBpeTokenization(false, false, 512, Tokenization.Truncate.NONE, 100, false, null, null, null, null, null),
            updatedSpan
        );

        var updatedTruncate = new ByteLevelBpeTokenizationUpdate(Tokenization.Truncate.FIRST, null).apply(
            new ByteLevelBpeTokenization(false, true, 512, Tokenization.Truncate.SECOND, null, true, null, null, null, null, null)
        );
        assertEquals(
            new ByteLevelBpeTokenization(false, true, 512, Tokenization.Truncate.FIRST, null, true, null, null, null, null, null),
            updatedTruncate
        );

        var updatedNone = new ByteLevelBpeTokenizationUpdate(Tokenization.Truncate.NONE, null).apply(
            new ByteLevelBpeTokenization(false, true, 512, Tokenization.Truncate.SECOND, null, false, null, null, null, null, null)
        );
        assertEquals(
            new ByteLevelBpeTokenization(false, true, 512, Tokenization.Truncate.NONE, null, false, null, null, null, null, null),
            updatedNone
        );

        var unmodified = new ByteLevelBpeTokenization(
            false,
            true,
            512,
            Tokenization.Truncate.NONE,
            null,
            false,
            null,
            null,
            null,
            null,
            null
        );
        assertThat(new ByteLevelBpeTokenizationUpdate(null, null).apply(unmodified), sameInstance(unmodified));
    }

    /**
     * {@link Tokenization.Truncate#NONE} with a span plus an update to span-incompatible {@code truncate} and omitted
     * {@code span} merges the old span and fails {@link Tokenization#validateSpanAndTruncate}.
     */
    public void testApplyIncompatibleTruncateWithInheritedSpanThrows() {
        var windowing = new ByteLevelBpeTokenization(
            false,
            false,
            512,
            Tokenization.Truncate.NONE,
            50,
            false,
            null,
            null,
            null,
            null,
            null
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new ByteLevelBpeTokenizationUpdate(Tokenization.Truncate.FIRST, null).apply(windowing)
        );
    }

    @Override
    protected Writeable.Reader<ByteLevelBpeTokenizationUpdate> instanceReader() {
        return ByteLevelBpeTokenizationUpdate::new;
    }

    @Override
    protected ByteLevelBpeTokenizationUpdate createTestInstance() {
        return randomInstance();
    }

    @Override
    protected ByteLevelBpeTokenizationUpdate mutateInstance(ByteLevelBpeTokenizationUpdate instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected ByteLevelBpeTokenizationUpdate mutateInstanceForVersion(ByteLevelBpeTokenizationUpdate instance, TransportVersion version) {
        return instance;
    }
}
