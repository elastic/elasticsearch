/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.common.model.Truncation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.INPUT_TYPE;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.TRUNCATE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class OciGenAiEmbeddingsTaskSettingsTests extends AbstractBWCWireSerializationTestCase<OciGenAiEmbeddingsTaskSettings> {

    public static OciGenAiEmbeddingsTaskSettings createRandom() {
        return new OciGenAiEmbeddingsTaskSettings(
            randomBoolean() ? null : randomFrom(OciGenAiEmbeddingsTaskSettings.VALID_REQUEST_INPUT_TYPES),
            randomBoolean() ? null : randomFrom(Truncation.values())
        );
    }

    public void testFromMap_ParsesInputTypeAndTruncate() {
        var settings = OciGenAiEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(INPUT_TYPE, "ingest", TRUNCATE, "end")));

        assertThat(settings.getInputType(), is(InputType.INGEST));
        assertThat(settings.getTruncation(), is(Truncation.END));
        assertFalse(settings.isEmpty());
    }

    public void testFromMap_EmptyOrNull_ReturnsEmptySettings() {
        assertThat(OciGenAiEmbeddingsTaskSettings.fromMap(null), sameInstance(OciGenAiEmbeddingsTaskSettings.EMPTY_SETTINGS));
        assertThat(OciGenAiEmbeddingsTaskSettings.fromMap(new HashMap<>()), sameInstance(OciGenAiEmbeddingsTaskSettings.EMPTY_SETTINGS));
        assertTrue(OciGenAiEmbeddingsTaskSettings.EMPTY_SETTINGS.isEmpty());
    }

    public void testFromMap_ThrowsForInvalidInputType() {
        var exception = expectThrows(
            ValidationException.class,
            () -> OciGenAiEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(INPUT_TYPE, "internal_ingest")))
        );

        assertThat(
            exception.getMessage(),
            containsString("[task_settings] Invalid value [internal_ingest] received. [input_type] must be one of")
        );
    }

    public void testFromMap_ThrowsForInvalidTruncate() {
        var exception = expectThrows(
            ValidationException.class,
            () -> OciGenAiEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(TRUNCATE, "middle")))
        );

        assertThat(exception.getMessage(), containsString("[truncate] must be one of"));
    }

    public void testOf_PrefersRequestSettings() {
        var original = new OciGenAiEmbeddingsTaskSettings(InputType.INGEST, Truncation.END);
        var request = new OciGenAiEmbeddingsTaskSettings(InputType.SEARCH, null);

        var merged = OciGenAiEmbeddingsTaskSettings.of(original, request);

        assertThat(merged.getInputType(), is(InputType.SEARCH));
        assertThat(merged.getTruncation(), is(Truncation.END));
    }

    public void testUpdatedTaskSettings() {
        var original = new OciGenAiEmbeddingsTaskSettings(InputType.INGEST, null);

        var updated = (OciGenAiEmbeddingsTaskSettings) original.updatedTaskSettings(new HashMap<>(Map.of(TRUNCATE, "start")));

        assertThat(updated.getInputType(), is(InputType.INGEST));
        assertThat(updated.getTruncation(), is(Truncation.START));
    }

    public void testToXContent() throws IOException {
        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        new OciGenAiEmbeddingsTaskSettings(InputType.CLASSIFICATION, Truncation.NONE).toXContent(builder, null);

        assertThat(Strings.toString(builder), is(XContentHelper.stripWhitespace("""
            { "input_type": "classification", "truncate": "none" }
            """)));
    }

    public void testToXContent_Empty() throws IOException {
        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        OciGenAiEmbeddingsTaskSettings.EMPTY_SETTINGS.toXContent(builder, null);

        assertThat(Strings.toString(builder), is("{}"));
        assertThat(OciGenAiEmbeddingsTaskSettings.EMPTY_SETTINGS.getInputType(), nullValue());
    }

    @Override
    protected Writeable.Reader<OciGenAiEmbeddingsTaskSettings> instanceReader() {
        return OciGenAiEmbeddingsTaskSettings::new;
    }

    @Override
    protected OciGenAiEmbeddingsTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OciGenAiEmbeddingsTaskSettings mutateInstance(OciGenAiEmbeddingsTaskSettings instance) throws IOException {
        return randomValueOtherThan(instance, OciGenAiEmbeddingsTaskSettingsTests::createRandom);
    }

    @Override
    protected OciGenAiEmbeddingsTaskSettings mutateInstanceForVersion(OciGenAiEmbeddingsTaskSettings instance, TransportVersion version) {
        return instance;
    }
}
