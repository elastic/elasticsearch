/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.completion;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;

import static org.hamcrest.Matchers.is;

public class TencentCloudChatCompletionTaskSettingsTests extends AbstractWireSerializingTestCase<TencentCloudChatCompletionTaskSettings> {

    public void testFromMap_ReturnsEmptySettings() {
        assertThat(
            TencentCloudChatCompletionTaskSettings.fromMap(new HashMap<>()),
            is(TencentCloudChatCompletionTaskSettings.EMPTY_SETTINGS)
        );
    }

    public void testFromMap_NullMap_ReturnsEmptySettings() {
        assertThat(TencentCloudChatCompletionTaskSettings.fromMap(null), is(TencentCloudChatCompletionTaskSettings.EMPTY_SETTINGS));
    }

    public void testIsEmpty_AlwaysTrue() {
        assertTrue(new TencentCloudChatCompletionTaskSettings().isEmpty());
    }

    public void testUpdatedTaskSettings_ReturnsSameInstance() {
        var settings = new TencentCloudChatCompletionTaskSettings();
        assertSame(settings, settings.updatedTaskSettings(new HashMap<>()));
    }

    @Override
    protected Writeable.Reader<TencentCloudChatCompletionTaskSettings> instanceReader() {
        return TencentCloudChatCompletionTaskSettings::new;
    }

    @Override
    protected TencentCloudChatCompletionTaskSettings createTestInstance() {
        return new TencentCloudChatCompletionTaskSettings();
    }

    @Override
    protected TencentCloudChatCompletionTaskSettings mutateInstance(TencentCloudChatCompletionTaskSettings instance) throws IOException {
        // No mutable fields available, return null to keep the wire-serialization framework happy.
        return null;
    }
}
