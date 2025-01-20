/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankTaskSettings.TOP_N;
import static org.hamcrest.Matchers.is;

public class GoogleVertexAiRerankTaskSettingsTests extends AbstractBWCWireSerializationTestCase<GoogleVertexAiRerankTaskSettings> {

    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testUpdatedTaskSettings() {
        var initialSettings = createRandom();
        var newSettings = createRandom();
        Map<String, Object> newSettingsMap = new HashMap<>();
        if (newSettings.topN() != null) {
            newSettingsMap.put(GoogleVertexAiRerankTaskSettings.TOP_N, newSettings.topN());
        }
        GoogleVertexAiRerankTaskSettings updatedSettings = (GoogleVertexAiRerankTaskSettings) initialSettings.updatedTaskSettings(
            Collections.unmodifiableMap(newSettingsMap)
        );
        if (newSettings.topN() == null) {
            assertEquals(initialSettings.topN(), updatedSettings.topN());
        } else {
            assertEquals(newSettings.topN(), updatedSettings.topN());
        }
    }

    public void testFromMap_TopNIsSet() {
        var topN = 1;
        var taskSettingsMap = getTaskSettingsMap(topN);
        var taskSettings = GoogleVertexAiRerankTaskSettings.fromMap(taskSettingsMap);

        assertThat(taskSettings, is(new GoogleVertexAiRerankTaskSettings(topN)));
    }

    public void testFromMap_ThrowsValidationException_IfTopNIsInvalidValue() {
        var taskSettingsMap = getTaskSettingsMap("invalid");

        expectThrows(ValidationException.class, () -> GoogleVertexAiRerankTaskSettings.fromMap(taskSettingsMap));
    }

    public void testFromMap_TopNIsNull() {
        var taskSettingsMap = getTaskSettingsMap(null);
        var taskSettings = GoogleVertexAiRerankTaskSettings.fromMap(taskSettingsMap);
        // needed, because of constructors being ambiguous otherwise
        Integer nullInt = null;

        assertThat(taskSettings, is(new GoogleVertexAiRerankTaskSettings(nullInt)));
    }

    public void testFromMap_DoesNotThrow_WithEmptyMap() {
        assertNull(GoogleVertexAiRerankTaskSettings.fromMap(new HashMap<>()).topN());
    }

    public void testOf_UseRequestSettings() {
        var originalTopN = 1;
        var originalSettings = new GoogleVertexAiRerankTaskSettings(originalTopN);

        var requestTopN = originalTopN + 1;
        var requestTaskSettings = new GoogleVertexAiRerankRequestTaskSettings(requestTopN);

        assertThat(GoogleVertexAiRerankTaskSettings.of(originalSettings, requestTaskSettings).topN(), is(requestTopN));
    }

    public void testOf_UseOriginalSettings() {
        var originalTopN = 1;
        var originalSettings = new GoogleVertexAiRerankTaskSettings(originalTopN);

        var requestTaskSettings = new GoogleVertexAiRerankRequestTaskSettings(null);

        assertThat(GoogleVertexAiRerankTaskSettings.of(originalSettings, requestTaskSettings).topN(), is(originalTopN));
    }

    public void testToXContent_WritesTopNIfNotNull() throws IOException {
        var settings = GoogleVertexAiRerankTaskSettings.fromMap(getTaskSettingsMap(1));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "top_n":1
            }
            """));
    }

    public void testToXContent_DoesNotWriteTopNIfNull() throws IOException {
        var settings = GoogleVertexAiRerankTaskSettings.fromMap(getTaskSettingsMap(null));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {}"""));
    }

    @Override
    protected Writeable.Reader<GoogleVertexAiRerankTaskSettings> instanceReader() {
        return GoogleVertexAiRerankTaskSettings::new;
    }

    @Override
    protected GoogleVertexAiRerankTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected GoogleVertexAiRerankTaskSettings mutateInstance(GoogleVertexAiRerankTaskSettings instance) throws IOException {
        return randomValueOtherThan(instance, GoogleVertexAiRerankTaskSettingsTests::createRandom);
    }

    @Override
    protected GoogleVertexAiRerankTaskSettings mutateInstanceForVersion(
        GoogleVertexAiRerankTaskSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static GoogleVertexAiRerankTaskSettings createRandom() {
        return new GoogleVertexAiRerankTaskSettings(randomFrom(new Integer[] { null, randomNonNegativeInt() }));
    }

    private static Map<String, Object> getTaskSettingsMap(@Nullable Object topN) {
        var map = new HashMap<String, Object>();

        if (topN != null) {
            map.put(TOP_N, topN);
        }

        return map;
    }
}
