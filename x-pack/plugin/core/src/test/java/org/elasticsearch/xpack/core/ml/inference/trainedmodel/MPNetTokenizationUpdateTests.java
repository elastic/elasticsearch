/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import static org.hamcrest.Matchers.sameInstance;

public class MPNetTokenizationUpdateTests extends AbstractBWCWireSerializationTestCase<MPNetTokenizationUpdate> {

    public static MPNetTokenizationUpdate randomInstance() {
        Integer span = randomBoolean() ? null : randomIntBetween(8, 128);
        Tokenization.Truncate truncate = randomBoolean() ? null : randomFrom(Tokenization.Truncate.values());

        if (truncate != Tokenization.Truncate.NONE) {
            span = null;
        }
        return new MPNetTokenizationUpdate(truncate, span);
    }

    public void testApply() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new MPNetTokenizationUpdate(Tokenization.Truncate.SECOND, 100).apply(MPNetTokenizationTests.createRandom())
        );

        var updatedSpan = new MPNetTokenizationUpdate(null, 100).apply(
            new MPNetTokenization(false, false, 512, Tokenization.Truncate.NONE, 50)
        );
        assertEquals(new MPNetTokenization(false, false, 512, Tokenization.Truncate.NONE, 100), updatedSpan);

        var updatedTruncate = new MPNetTokenizationUpdate(Tokenization.Truncate.FIRST, null).apply(
            new MPNetTokenization(true, true, 512, Tokenization.Truncate.SECOND, null)
        );
        assertEquals(new MPNetTokenization(true, true, 512, Tokenization.Truncate.FIRST, null), updatedTruncate);

        var updatedNone = new MPNetTokenizationUpdate(Tokenization.Truncate.NONE, null).apply(
            new MPNetTokenization(true, true, 512, Tokenization.Truncate.SECOND, null)
        );
        assertEquals(new MPNetTokenization(true, true, 512, Tokenization.Truncate.NONE, null), updatedNone);

        var unmodified = new MPNetTokenization(true, true, 512, Tokenization.Truncate.NONE, null);
        assertThat(new MPNetTokenizationUpdate(null, null).apply(unmodified), sameInstance(unmodified));
    }

    @Override
    protected Writeable.Reader<MPNetTokenizationUpdate> instanceReader() {
        return MPNetTokenizationUpdate::new;
    }

    @Override
    protected MPNetTokenizationUpdate createTestInstance() {
        return randomInstance();
    }

    @Override
    protected MPNetTokenizationUpdate mutateInstance(MPNetTokenizationUpdate instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected MPNetTokenizationUpdate mutateInstanceForVersion(MPNetTokenizationUpdate instance, Version version) {
        if (version.before(Version.V_8_2_0)) {
            return new MPNetTokenizationUpdate(instance.getTruncate(), null);
        }

        return instance;
    }
}
