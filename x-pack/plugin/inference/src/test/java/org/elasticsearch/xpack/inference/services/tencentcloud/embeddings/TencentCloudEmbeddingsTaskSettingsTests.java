/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.embeddings;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;

import static org.hamcrest.Matchers.is;

public class TencentCloudEmbeddingsTaskSettingsTests extends AbstractWireSerializingTestCase<TencentCloudEmbeddingsTaskSettings> {

    public void testFromMap_ReturnsEmptySettings() {
        assertThat(TencentCloudEmbeddingsTaskSettings.fromMap(new HashMap<>()), is(TencentCloudEmbeddingsTaskSettings.EMPTY_SETTINGS));
    }

    public void testFromMap_NullMap_ReturnsEmptySettings() {
        assertThat(TencentCloudEmbeddingsTaskSettings.fromMap(null), is(TencentCloudEmbeddingsTaskSettings.EMPTY_SETTINGS));
    }

    public void testIsEmpty_AlwaysTrue() {
        assertTrue(new TencentCloudEmbeddingsTaskSettings().isEmpty());
    }

    public void testUpdatedTaskSettings_ReturnsSameInstance() {
        var settings = new TencentCloudEmbeddingsTaskSettings();
        assertSame(settings, settings.updatedTaskSettings(new HashMap<>()));
    }

    @Override
    protected Writeable.Reader<TencentCloudEmbeddingsTaskSettings> instanceReader() {
        return TencentCloudEmbeddingsTaskSettings::new;
    }

    @Override
    protected TencentCloudEmbeddingsTaskSettings createTestInstance() {
        return new TencentCloudEmbeddingsTaskSettings();
    }

    @Override
    protected TencentCloudEmbeddingsTaskSettings mutateInstance(TencentCloudEmbeddingsTaskSettings instance) throws IOException {
        // No mutable fields available, return null to keep the wire-serialization framework happy.
        return null;
    }
}
