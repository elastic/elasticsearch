/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.inference.common.model.Truncation;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceFields;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.InputTypeTests.randomWithoutUnspecified;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class CohereEmbeddingsTaskSettingsTests extends AbstractBWCSerializationTestCase<CohereEmbeddingsTaskSettings> {

    private final boolean supportsUnknownFields = randomBoolean();

    public static CohereEmbeddingsTaskSettings createRandom() {
        var inputType = randomBoolean() ? randomWithoutUnspecified() : null;
        var truncation = randomBoolean() ? randomFrom(Truncation.values()) : null;

        return new CohereEmbeddingsTaskSettings(inputType, truncation);
    }

    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testUpdatedTaskSettings() {
        var initialSettings = createRandom();
        var newSettings = createRandom();
        Map<String, Object> newSettingsMap = new HashMap<>();
        if (newSettings.getInputType() != null) {
            newSettingsMap.put(CohereEmbeddingsTaskSettings.INPUT_TYPE, newSettings.getInputType().toString());
        }
        if (newSettings.getTruncation() != null) {
            newSettingsMap.put(CohereServiceFields.TRUNCATE, newSettings.getTruncation().toString());
        }
        CohereEmbeddingsTaskSettings updatedSettings = (CohereEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(newSettingsMap);
        if (newSettings.getInputType() == null) {
            assertEquals(initialSettings.getInputType(), updatedSettings.getInputType());
        } else {
            assertEquals(newSettings.getInputType(), updatedSettings.getInputType());
        }
        if (newSettings.getTruncation() == null) {
            assertEquals(initialSettings.getTruncation(), updatedSettings.getTruncation());
        } else {
            assertEquals(newSettings.getTruncation(), updatedSettings.getTruncation());
        }
    }

    public void testFromMap_CreatesEmptySettings_WhenAllFieldsAreNull() {
        assertThat(
            CohereEmbeddingsTaskSettings.fromMap(new HashMap<>(), ConfigurationParseContext.REQUEST),
            is(new CohereEmbeddingsTaskSettings(null, null))
        );
    }

    public void testFromMap_CreatesEmptySettings_WhenMapIsNull() {
        assertThat(
            CohereEmbeddingsTaskSettings.fromMap(null, ConfigurationParseContext.REQUEST),
            is(new CohereEmbeddingsTaskSettings(null, null))
        );
    }

    public void testFromMap_CreatesSettings_WhenAllFieldsOfSettingsArePresent() {
        assertThat(
            CohereEmbeddingsTaskSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        CohereEmbeddingsTaskSettings.INPUT_TYPE,
                        InputType.INGEST.toString(),
                        CohereServiceFields.TRUNCATE,
                        Truncation.END.toString()
                    )
                ),
                ConfigurationParseContext.REQUEST
            ),
            is(new CohereEmbeddingsTaskSettings(InputType.INGEST, Truncation.END))
        );
    }

    public void testFromMap_ReturnsFailure_WhenInputTypeIsInvalid() {
        var exception = expectThrows(
            XContentParseException.class,
            () -> CohereEmbeddingsTaskSettings.fromMap(
                new HashMap<>(Map.of(CohereEmbeddingsTaskSettings.INPUT_TYPE, "abc")),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(
            exception.getCause().getMessage(),
            equalTo("Invalid value [abc]; expected one of [ingest, search, classification, clustering]")
        );
    }

    public void testFromMap_ReturnsFailure_WhenInputTypeIsUnspecified() {
        var exception = expectThrows(
            XContentParseException.class,
            () -> CohereEmbeddingsTaskSettings.fromMap(
                new HashMap<>(Map.of(CohereEmbeddingsTaskSettings.INPUT_TYPE, InputType.UNSPECIFIED.toString())),
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(
            exception.getCause().getMessage(),
            equalTo("Invalid value [unspecified]; expected one of [ingest, search, classification, clustering]")
        );
    }

    public void testXContent_ThrowsAssertionFailure_WhenInputTypeIsUnspecified() {
        var thrownException = expectThrows(AssertionError.class, () -> new CohereEmbeddingsTaskSettings(InputType.UNSPECIFIED, null));
        assertThat(thrownException.getMessage(), is("received invalid input type value [unspecified]"));
    }

    public void testOf_UsesRequestTaskSettings() {
        var taskSettings = new CohereEmbeddingsTaskSettings(null, Truncation.NONE);
        var overriddenTaskSettings = CohereEmbeddingsTaskSettings.of(
            taskSettings,
            new CohereEmbeddingsTaskSettings(InputType.INGEST, Truncation.END)
        );

        assertThat(overriddenTaskSettings, is(new CohereEmbeddingsTaskSettings(InputType.INGEST, Truncation.END)));
    }

    @Override
    protected Writeable.Reader<CohereEmbeddingsTaskSettings> instanceReader() {
        return CohereEmbeddingsTaskSettings::new;
    }

    @Override
    protected CohereEmbeddingsTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected CohereEmbeddingsTaskSettings mutateInstance(CohereEmbeddingsTaskSettings instance) throws IOException {
        if (randomBoolean()) {
            InputType inputType = randomValueOtherThan(instance.getInputType(), () -> randomFrom(randomWithoutUnspecified(), null));
            return new CohereEmbeddingsTaskSettings(inputType, instance.getTruncation());
        } else {
            Truncation truncation = randomValueOtherThan(instance.getTruncation(), () -> randomFrom(randomFrom(Truncation.values()), null));
            return new CohereEmbeddingsTaskSettings(instance.getInputType(), truncation);
        }
    }

    @Override
    protected CohereEmbeddingsTaskSettings doParseInstance(XContentParser parser) throws IOException {
        return CohereEmbeddingsTaskSettings.createParser(supportsUnknownFields).apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return supportsUnknownFields;
    }

    public static Map<String, Object> getTaskSettingsMapEmpty() {
        return new HashMap<>();
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable InputType inputType, @Nullable Truncation truncation) {
        var map = new HashMap<String, Object>();

        if (inputType != null) {
            map.put(CohereEmbeddingsTaskSettings.INPUT_TYPE, inputType.toString());
        }

        if (truncation != null) {
            map.put(CohereServiceFields.TRUNCATE, truncation.toString());
        }

        return map;
    }

    @Override
    protected CohereEmbeddingsTaskSettings mutateInstanceForVersion(CohereEmbeddingsTaskSettings instance, TransportVersion version) {
        return instance;
    }
}
