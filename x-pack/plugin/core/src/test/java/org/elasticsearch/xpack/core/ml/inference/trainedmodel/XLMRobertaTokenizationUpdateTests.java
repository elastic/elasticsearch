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

public class XLMRobertaTokenizationUpdateTests extends AbstractBWCWireSerializationTestCase<XLMRobertaTokenizationUpdate> {

    public static XLMRobertaTokenizationUpdate randomInstance() {
        Integer span = randomBoolean() ? null : randomIntBetween(8, 128);
        Tokenization.Truncate truncate = randomBoolean() ? null : randomFrom(Tokenization.Truncate.values());

        if (truncate != Tokenization.Truncate.NONE) {
            span = null;
        }
        return new XLMRobertaTokenizationUpdate(truncate, span);
    }

    public void testApply() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new XLMRobertaTokenizationUpdate(Tokenization.Truncate.SECOND, 100).apply(XLMRobertaTokenizationTests.createRandom())
        );

        var updatedSpan = new XLMRobertaTokenizationUpdate(null, 100).apply(
            new XLMRobertaTokenization(false, 512, Tokenization.Truncate.NONE, 50)
        );
        assertEquals(new XLMRobertaTokenization(false, 512, Tokenization.Truncate.NONE, 100), updatedSpan);

        var updatedTruncate = new XLMRobertaTokenizationUpdate(Tokenization.Truncate.FIRST, null).apply(
            new XLMRobertaTokenization(true, 512, Tokenization.Truncate.SECOND, null)
        );
        assertEquals(new XLMRobertaTokenization(true, 512, Tokenization.Truncate.FIRST, null), updatedTruncate);

        var updatedNone = new XLMRobertaTokenizationUpdate(Tokenization.Truncate.NONE, null).apply(
            new XLMRobertaTokenization(true, 512, Tokenization.Truncate.SECOND, null)
        );
        assertEquals(new XLMRobertaTokenization(true, 512, Tokenization.Truncate.NONE, null), updatedNone);

        var unmodified = new XLMRobertaTokenization(true, 512, Tokenization.Truncate.NONE, null);
        assertThat(new XLMRobertaTokenizationUpdate(null, null).apply(unmodified), sameInstance(unmodified));
    }

    @Override
    protected Writeable.Reader<XLMRobertaTokenizationUpdate> instanceReader() {
        return XLMRobertaTokenizationUpdate::new;
    }

    @Override
    protected XLMRobertaTokenizationUpdate createTestInstance() {
        return randomInstance();
    }

    @Override
    protected XLMRobertaTokenizationUpdate mutateInstance(XLMRobertaTokenizationUpdate instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected XLMRobertaTokenizationUpdate mutateInstanceForVersion(XLMRobertaTokenizationUpdate instance, TransportVersion version) {
        if (version.before(TransportVersion.V_8_2_0)) {
            return new XLMRobertaTokenizationUpdate(instance.getTruncate(), null);
        }

        return instance;
    }
}
