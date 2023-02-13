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

public class RobertaTokenizationUpdateTests extends AbstractBWCWireSerializationTestCase<RobertaTokenizationUpdate> {

    public static RobertaTokenizationUpdate randomInstance() {
        Integer span = randomBoolean() ? null : randomIntBetween(8, 128);
        Tokenization.Truncate truncate = randomBoolean() ? null : randomFrom(Tokenization.Truncate.values());

        if (truncate != Tokenization.Truncate.NONE) {
            span = null;
        }
        return new RobertaTokenizationUpdate(truncate, span);
    }

    public void testApply() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new RobertaTokenizationUpdate(Tokenization.Truncate.SECOND, 100).apply(RobertaTokenizationTests.createRandom())
        );

        var updatedSpan = new RobertaTokenizationUpdate(null, 100).apply(
            new RobertaTokenization(false, false, 512, Tokenization.Truncate.NONE, 50)
        );
        assertEquals(new RobertaTokenization(false, false, 512, Tokenization.Truncate.NONE, 100), updatedSpan);

        var updatedTruncate = new RobertaTokenizationUpdate(Tokenization.Truncate.FIRST, null).apply(
            new RobertaTokenization(true, true, 512, Tokenization.Truncate.SECOND, null)
        );
        assertEquals(new RobertaTokenization(true, true, 512, Tokenization.Truncate.FIRST, null), updatedTruncate);

        var updatedNone = new RobertaTokenizationUpdate(Tokenization.Truncate.NONE, null).apply(
            new RobertaTokenization(true, true, 512, Tokenization.Truncate.SECOND, null)
        );
        assertEquals(new RobertaTokenization(true, true, 512, Tokenization.Truncate.NONE, null), updatedNone);

        var unmodified = new RobertaTokenization(true, true, 512, Tokenization.Truncate.NONE, null);
        assertThat(new RobertaTokenizationUpdate(null, null).apply(unmodified), sameInstance(unmodified));
    }

    @Override
    protected Writeable.Reader<RobertaTokenizationUpdate> instanceReader() {
        return RobertaTokenizationUpdate::new;
    }

    @Override
    protected RobertaTokenizationUpdate createTestInstance() {
        return randomInstance();
    }

    @Override
    protected RobertaTokenizationUpdate mutateInstance(RobertaTokenizationUpdate instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected RobertaTokenizationUpdate mutateInstanceForVersion(RobertaTokenizationUpdate instance, Version version) {
        if (version.before(Version.V_8_2_0)) {
            return new RobertaTokenizationUpdate(instance.getTruncate(), null);
        }

        return instance;
    }
}
