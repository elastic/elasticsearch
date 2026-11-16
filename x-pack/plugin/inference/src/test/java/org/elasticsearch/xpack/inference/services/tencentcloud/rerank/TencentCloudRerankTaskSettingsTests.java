/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.rerank;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class TencentCloudRerankTaskSettingsTests extends AbstractWireSerializingTestCase<TencentCloudRerankTaskSettings> {

    public static TencentCloudRerankTaskSettings createRandom() {
        Integer topN = randomBoolean() ? randomIntBetween(1, 20) : null;
        Boolean returnDocuments = randomBoolean() ? randomBoolean() : null;
        return new TencentCloudRerankTaskSettings(topN, returnDocuments);
    }

    public void testFromMap_Empty_ReturnsEmptySettings() {
        assertThat(TencentCloudRerankTaskSettings.fromMap(new HashMap<>()), is(TencentCloudRerankTaskSettings.EMPTY_SETTINGS));
    }

    public void testFromMap_NullMap_ReturnsEmptySettings() {
        assertThat(TencentCloudRerankTaskSettings.fromMap(null), is(TencentCloudRerankTaskSettings.EMPTY_SETTINGS));
    }

    public void testFromMap_WithFields() {
        var settings = TencentCloudRerankTaskSettings.fromMap(
            new HashMap<>(Map.of(TencentCloudRerankTaskSettings.TOP_N, 3, TencentCloudRerankTaskSettings.RETURN_DOCUMENTS, true))
        );

        assertThat(settings.getTopN(), is(3));
        assertThat(settings.getReturnDocuments(), is(true));
    }

    public void testOf_PrefersRequestSettings() {
        var original = new TencentCloudRerankTaskSettings(1, false);
        var request = new TencentCloudRerankTaskSettings(5, true);
        var merged = TencentCloudRerankTaskSettings.of(original, request);

        assertThat(merged.getTopN(), is(5));
        assertThat(merged.getReturnDocuments(), is(true));
    }

    public void testOf_KeepsOriginalWhenRequestIsEmpty() {
        var original = new TencentCloudRerankTaskSettings(1, false);
        var request = TencentCloudRerankTaskSettings.EMPTY_SETTINGS;
        var merged = TencentCloudRerankTaskSettings.of(original, request);

        assertThat(merged.getTopN(), is(1));
        assertThat(merged.getReturnDocuments(), is(false));
    }

    public void testUpdatedTaskSettings() {
        var original = new TencentCloudRerankTaskSettings(1, false);
        var merged = (TencentCloudRerankTaskSettings) original.updatedTaskSettings(
            new HashMap<>(Map.of(TencentCloudRerankTaskSettings.TOP_N, 8))
        );
        assertThat(merged.getTopN(), is(8));
        assertThat(merged.getReturnDocuments(), is(false));
    }

    public void testIsEmpty() {
        assertTrue(TencentCloudRerankTaskSettings.EMPTY_SETTINGS.isEmpty());
        assertFalse(new TencentCloudRerankTaskSettings(1, null).isEmpty());
        assertFalse(new TencentCloudRerankTaskSettings(null, true).isEmpty());
    }

    @Override
    protected Writeable.Reader<TencentCloudRerankTaskSettings> instanceReader() {
        return TencentCloudRerankTaskSettings::new;
    }

    @Override
    protected TencentCloudRerankTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected TencentCloudRerankTaskSettings mutateInstance(TencentCloudRerankTaskSettings instance) throws IOException {
        var topN = instance.getTopN();
        var returnDocuments = instance.getReturnDocuments();
        if (randomBoolean()) {
            topN = topN == null ? randomIntBetween(1, 100) : null;
        } else {
            returnDocuments = returnDocuments == null ? randomBoolean() : null;
        }
        return new TencentCloudRerankTaskSettings(topN, returnDocuments);
    }
}
