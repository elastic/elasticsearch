/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class OciGenAiChatCompletionTaskSettingsTests extends AbstractWireSerializingTestCase<OciGenAiChatCompletionTaskSettings> {

    public void testFromMap_ReturnsEmptySettings() {
        assertThat(
            OciGenAiChatCompletionTaskSettings.fromMap(new HashMap<>(Map.of("some_key", "value"))),
            is(OciGenAiChatCompletionTaskSettings.EMPTY_SETTINGS)
        );
    }

    public void testFromMap_NullMap_ReturnsEmptySettings() {
        assertThat(OciGenAiChatCompletionTaskSettings.fromMap(null), is(OciGenAiChatCompletionTaskSettings.EMPTY_SETTINGS));
    }

    public void testIsEmpty_AlwaysTrue() {
        assertTrue(new OciGenAiChatCompletionTaskSettings().isEmpty());
    }

    public void testUpdatedTaskSettings_ReturnsSameInstance() {
        var settings = new OciGenAiChatCompletionTaskSettings();
        assertSame(settings, settings.updatedTaskSettings(new HashMap<>(Map.of("some_key", "value"))));
    }

    public void testToXContent_WritesAnEmptyObject() throws IOException {
        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        OciGenAiChatCompletionTaskSettings.EMPTY_SETTINGS.toXContent(builder, null);

        assertThat(Strings.toString(builder), is("{}"));
    }

    @Override
    protected Writeable.Reader<OciGenAiChatCompletionTaskSettings> instanceReader() {
        return OciGenAiChatCompletionTaskSettings::new;
    }

    @Override
    protected OciGenAiChatCompletionTaskSettings createTestInstance() {
        return new OciGenAiChatCompletionTaskSettings();
    }

    @Override
    protected OciGenAiChatCompletionTaskSettings mutateInstance(OciGenAiChatCompletionTaskSettings instance) throws IOException {
        // There are no fields to mutate; returning null tells the wire serialization framework to skip the inequality check.
        return null;
    }
}
