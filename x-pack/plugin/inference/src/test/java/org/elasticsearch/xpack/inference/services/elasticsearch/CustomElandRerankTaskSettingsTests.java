/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class CustomElandRerankTaskSettingsTests extends AbstractWireSerializingTestCase<CustomElandRerankTaskSettings> {

    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testDefaultsFromMap_MapIsNull_ReturnsDefaultSettings() {
        var customElandRerankTaskSettings = CustomElandRerankTaskSettings.defaultsFromMap(null);

        assertThat(customElandRerankTaskSettings, sameInstance(CustomElandRerankTaskSettings.DEFAULT_SETTINGS));
    }

    public void testDefaultsFromMap_MapIsEmpty_ReturnsDefaultSettings() {
        var customElandRerankTaskSettings = CustomElandRerankTaskSettings.defaultsFromMap(new HashMap<>());

        assertThat(customElandRerankTaskSettings, sameInstance(CustomElandRerankTaskSettings.DEFAULT_SETTINGS));
    }

    public void testDefaultsFromMap_ExtractedReturnDocumentsNull_SetsReturnDocumentToTrue() {
        var customElandRerankTaskSettings = CustomElandRerankTaskSettings.defaultsFromMap(new HashMap<>());

        assertThat(customElandRerankTaskSettings.returnDocuments(), is(Boolean.TRUE));
    }

    public void testFromMap_MapIsNull_ReturnsDefaultSettings() {
        var customElandRerankTaskSettings = CustomElandRerankTaskSettings.fromMap(null);

        assertThat(customElandRerankTaskSettings, sameInstance(CustomElandRerankTaskSettings.DEFAULT_SETTINGS));
    }

    public void testFromMap_MapIsEmpty_ReturnsDefaultSettings() {
        var customElandRerankTaskSettings = CustomElandRerankTaskSettings.fromMap(new HashMap<>());

        assertThat(customElandRerankTaskSettings, sameInstance(CustomElandRerankTaskSettings.DEFAULT_SETTINGS));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new CustomElandRerankTaskSettings(Boolean.TRUE);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"return_documents":true}"""));
    }

    public void testToXContent_DoesNotWriteReturnDocuments_IfNull() throws IOException {
        Boolean bool = null;
        var serviceSettings = new CustomElandRerankTaskSettings(bool);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {}"""));
    }

    public void testOf_PrefersNonNullRequestTaskSettings() {
        var originalSettings = new CustomElandRerankTaskSettings(Boolean.FALSE);
        var requestTaskSettings = new CustomElandRerankTaskSettings(Boolean.TRUE);

        var taskSettings = CustomElandRerankTaskSettings.of(originalSettings, requestTaskSettings);

        assertThat(taskSettings, sameInstance(requestTaskSettings));
    }

    public void testOf_UseOriginalSettings_IfRequestSettingsValuesAreNull() {
        Boolean bool = null;
        var originalSettings = new CustomElandRerankTaskSettings(Boolean.TRUE);
        var requestTaskSettings = new CustomElandRerankTaskSettings(bool);

        var taskSettings = CustomElandRerankTaskSettings.of(originalSettings, requestTaskSettings);

        assertThat(taskSettings, sameInstance(originalSettings));
    }

    private static CustomElandRerankTaskSettings createRandom() {
        return new CustomElandRerankTaskSettings(randomOptionalBoolean());
    }

    @Override
    protected Writeable.Reader<CustomElandRerankTaskSettings> instanceReader() {
        return CustomElandRerankTaskSettings::new;
    }

    @Override
    protected CustomElandRerankTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected CustomElandRerankTaskSettings mutateInstance(CustomElandRerankTaskSettings instance) throws IOException {
        return randomValueOtherThan(instance, CustomElandRerankTaskSettingsTests::createRandom);
    }
}
