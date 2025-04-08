/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.rerank;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankTaskSettings.TOP_N_DOCS_ONLY;
import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceRerankTaskSettingsTests extends AbstractWireSerializingTestCase<
    ElasticInferenceServiceRerankTaskSettings> {
    @Override
    protected Writeable.Reader<ElasticInferenceServiceRerankTaskSettings> instanceReader() {
        return ElasticInferenceServiceRerankTaskSettings::new;
    }

    @Override
    protected ElasticInferenceServiceRerankTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected ElasticInferenceServiceRerankTaskSettings mutateInstance(ElasticInferenceServiceRerankTaskSettings instance)
        throws IOException {
        return randomValueOtherThan(instance, ElasticInferenceServiceRerankTaskSettingsTests::createRandom);
    }

    public static ElasticInferenceServiceRerankTaskSettings createRandom() {
        return new ElasticInferenceServiceRerankTaskSettings(randomNonNegativeInt());
    }

    public void testIsEmpty() {
        ElasticInferenceServiceRerankTaskSettings settingsWithNull = new ElasticInferenceServiceRerankTaskSettings((Integer) null);
        assertTrue(settingsWithNull.isEmpty());

        ElasticInferenceServiceRerankTaskSettings settingsWithValue = new ElasticInferenceServiceRerankTaskSettings(5);
        assertFalse(settingsWithValue.isEmpty());
    }

    public void testUpdatedTaskSettings() {
        ElasticInferenceServiceRerankTaskSettings initialSettings = createRandom();
        ElasticInferenceServiceRerankTaskSettings newSettings = randomValueOtherThan(
            initialSettings,
            ElasticInferenceServiceRerankTaskSettingsTests::createRandom
        );
        Map<String, Object> newSettingsMap = new HashMap<>();

        if (newSettings.getTopNDocumentsOnly() != null) {
            newSettingsMap.put(TOP_N_DOCS_ONLY, newSettings.getTopNDocumentsOnly());
        }
        ElasticInferenceServiceRerankTaskSettings updatedSettings = (ElasticInferenceServiceRerankTaskSettings) initialSettings
            .updatedTaskSettings(Collections.unmodifiableMap(newSettingsMap));
        if (newSettings.getTopNDocumentsOnly() == null) {
            assertEquals(initialSettings.getTopNDocumentsOnly(), updatedSettings.getTopNDocumentsOnly());
        } else {
            assertEquals(newSettings.getTopNDocumentsOnly(), updatedSettings.getTopNDocumentsOnly());
        }
    }

    public void testFromMap_TopNIsSet() {
        int topN = 1;
        Map<String, Object> taskSettingsMap = getTaskSettingsMap(topN);
        ElasticInferenceServiceRerankTaskSettings taskSettings = ElasticInferenceServiceRerankTaskSettings.fromMap(taskSettingsMap);
        assertThat(taskSettings, is(new ElasticInferenceServiceRerankTaskSettings(topN)));
    }

    public void testFromMap_ThrowsValidationException_IfTopNIsInvalidValue() {
        Map<String, Object> taskSettingsMap = getTaskSettingsMap("invalid");
        expectThrows(ValidationException.class, () -> ElasticInferenceServiceRerankTaskSettings.fromMap(taskSettingsMap));
    }

    public void testFromMap_TopNIsNull() {
        Map<String, Object> taskSettingsMap = getTaskSettingsMap(null);
        ElasticInferenceServiceRerankTaskSettings taskSettings = ElasticInferenceServiceRerankTaskSettings.fromMap(taskSettingsMap);
        Integer nullInt = null;
        assertThat(taskSettings, is(new ElasticInferenceServiceRerankTaskSettings(nullInt)));
    }

    public void testFromMap_DoesNotThrow_WithEmptyMap() {
        assertNull(ElasticInferenceServiceRerankTaskSettings.fromMap(new HashMap<>()).getTopNDocumentsOnly());
    }

    public void testOf_UseRequestSettings() {
        int originalTopN = 1;
        ElasticInferenceServiceRerankTaskSettings originalSettings = new ElasticInferenceServiceRerankTaskSettings(originalTopN);

        int requestTopN = originalTopN + 1;
        ElasticInferenceServiceRerankTaskSettings requestTaskSettings = new ElasticInferenceServiceRerankTaskSettings(requestTopN);

        ElasticInferenceServiceRerankTaskSettings updated = ElasticInferenceServiceRerankTaskSettings.of(
            originalSettings,
            requestTaskSettings
        );
        assertThat(updated.getTopNDocumentsOnly(), is(requestTopN));
    }

    public void testOf_UseOriginalSettings() {
        int originalTopN = 1;
        ElasticInferenceServiceRerankTaskSettings originalSettings = new ElasticInferenceServiceRerankTaskSettings(originalTopN);
        ElasticInferenceServiceRerankTaskSettings requestTaskSettings = new ElasticInferenceServiceRerankTaskSettings((Integer) null);
        ElasticInferenceServiceRerankTaskSettings updated = ElasticInferenceServiceRerankTaskSettings.of(
            originalSettings,
            requestTaskSettings
        );
        assertThat(updated.getTopNDocumentsOnly(), is(originalTopN));
    }

    public void testToXContent_WritesTopNIfNotNull() throws IOException {
        ElasticInferenceServiceRerankTaskSettings settings = ElasticInferenceServiceRerankTaskSettings.fromMap(getTaskSettingsMap(1));
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("{ \"top_n\": 1 }"));
    }

    public void testToXContent_DoesNotWriteTopNIfNull() throws IOException {
        ElasticInferenceServiceRerankTaskSettings settings = ElasticInferenceServiceRerankTaskSettings.fromMap(getTaskSettingsMap(null));
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        assertThat(xContentResult, is("{}"));
    }

    private static Map<String, Object> getTaskSettingsMap(Object topN) {
        Map<String, Object> map = new HashMap<>();
        if (topN != null) {
            map.put(TOP_N_DOCS_ONLY, topN);
        }
        return map;
    }
}
