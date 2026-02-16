/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.services.openshiftai.rerank.OpenShiftAiRerankModelTests.buildTaskSettingsMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class OpenShiftAiRerankTaskSettingsTests extends AbstractBWCWireSerializationTestCase<OpenShiftAiRerankTaskSettings> {
    public static OpenShiftAiRerankTaskSettings createRandom() {
        var returnDocuments = randomOptionalBoolean();
        var topNDocsOnly = randomBoolean() ? randomIntBetween(1, 10) : null;

        return new OpenShiftAiRerankTaskSettings(topNDocsOnly, returnDocuments);
    }

    public void testFromMap_WithValidValues_ReturnsSettings() {
        var settings = OpenShiftAiRerankTaskSettings.fromMap(buildTaskSettingsMap(5, true));
        assertThat(settings.getReturnDocuments(), is(true));
        assertThat(settings.getTopN(), is(5));
    }

    public void testFromMap_WithNullValues_ReturnsSettingsWithNulls() {
        var settings = OpenShiftAiRerankTaskSettings.fromMap(Map.of());
        assertThat(settings.getReturnDocuments(), is(nullValue()));
        assertThat(settings.getTopN(), is(nullValue()));
    }

    public void testFromMap_WithInvalidReturnDocuments_ThrowsValidationException() {
        Map<String, Object> taskMap = Map.of(
            OpenShiftAiRerankTaskSettings.RETURN_DOCUMENTS,
            "invalid",
            OpenShiftAiRerankTaskSettings.TOP_N,
            5
        );
        var thrownException = expectThrows(ValidationException.class, () -> OpenShiftAiRerankTaskSettings.fromMap(new HashMap<>(taskMap)));
        assertThat(thrownException.getMessage(), containsString("field [return_documents] is not of the expected type"));
    }

    public void testFromMap_WithInvalidTopNDocsOnly_ThrowsValidationException() {
        Map<String, Object> taskMap = Map.of(
            OpenShiftAiRerankTaskSettings.RETURN_DOCUMENTS,
            true,
            OpenShiftAiRerankTaskSettings.TOP_N,
            "invalid"
        );
        var thrownException = expectThrows(ValidationException.class, () -> OpenShiftAiRerankTaskSettings.fromMap(new HashMap<>(taskMap)));
        assertThat(thrownException.getMessage(), containsString("field [top_n] is not of the expected type"));
    }

    public void testUpdatedTaskSettings_WithEmptyMap_ReturnsSameSettings() {
        var initialSettings = new OpenShiftAiRerankTaskSettings(5, true);
        OpenShiftAiRerankTaskSettings updatedSettings = (OpenShiftAiRerankTaskSettings) initialSettings.updatedTaskSettings(Map.of());
        assertThat(initialSettings, is(sameInstance(updatedSettings)));
    }

    public void testUpdatedTaskSettings_WithNewReturnDocuments_ReturnsUpdatedSettings() {
        var initialSettings = new OpenShiftAiRerankTaskSettings(5, true);
        OpenShiftAiRerankTaskSettings updatedSettings = (OpenShiftAiRerankTaskSettings) initialSettings.updatedTaskSettings(
            buildTaskSettingsMap(null, false)
        );
        assertThat(updatedSettings.getReturnDocuments(), is(false));
        assertThat(initialSettings.getTopN(), is(updatedSettings.getTopN()));
    }

    public void testUpdatedTaskSettings_WithNewTopNDocsOnly_ReturnsUpdatedSettings() {
        var initialSettings = new OpenShiftAiRerankTaskSettings(5, true);
        OpenShiftAiRerankTaskSettings updatedSettings = (OpenShiftAiRerankTaskSettings) initialSettings.updatedTaskSettings(
            buildTaskSettingsMap(7, null)
        );
        assertThat(updatedSettings.getTopN(), is(7));
        assertThat(updatedSettings.getReturnDocuments(), is(initialSettings.getReturnDocuments()));
    }

    public void testUpdatedTaskSettings_WithMultipleNewValues_ReturnsUpdatedSettings() {
        var initialSettings = new OpenShiftAiRerankTaskSettings(5, true);
        OpenShiftAiRerankTaskSettings updatedSettings = (OpenShiftAiRerankTaskSettings) initialSettings.updatedTaskSettings(
            buildTaskSettingsMap(7, false)
        );
        assertThat(updatedSettings.getReturnDocuments(), is(false));
        assertThat(updatedSettings.getTopN(), is(7));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        testToXContent(2, true, """
            {
                "top_n":2,
                "return_documents":true
            }
            """);
    }

    public void testToXContent_EmptyValues() throws IOException {
        testToXContent(null, null, """
            {}
            """);
    }

    public void testToXContent_OnlyTopN() throws IOException {
        testToXContent(2, null, """
            {
                "top_n":2
            }
            """);
    }

    public void testToXContent_OnlyReturnDocuments() throws IOException {
        testToXContent(null, true, """
            {
                "return_documents":true
            }
            """);
    }

    private static void testToXContent(Integer topN, Boolean doReturnDocuments, String expectedString) throws IOException {
        var taskSettings = new OpenShiftAiRerankTaskSettings(topN, doReturnDocuments);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        taskSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString(expectedString));
    }

    @Override
    protected Writeable.Reader<OpenShiftAiRerankTaskSettings> instanceReader() {
        return OpenShiftAiRerankTaskSettings::new;
    }

    @Override
    protected OpenShiftAiRerankTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OpenShiftAiRerankTaskSettings mutateInstance(OpenShiftAiRerankTaskSettings instance) throws IOException {
        Integer topN = instance.getTopN();
        Boolean returnDocuments = instance.getReturnDocuments();
        switch (between(0, 1)) {
            case 0 -> topN = randomValueOtherThan(topN, () -> randomBoolean() ? randomIntBetween(1, 10) : null);
            case 1 -> returnDocuments = randomValueOtherThan(returnDocuments, ESTestCase::randomOptionalBoolean);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new OpenShiftAiRerankTaskSettings(topN, returnDocuments);
    }

    @Override
    protected OpenShiftAiRerankTaskSettings mutateInstanceForVersion(OpenShiftAiRerankTaskSettings instance, TransportVersion version) {
        return instance;
    }
}
