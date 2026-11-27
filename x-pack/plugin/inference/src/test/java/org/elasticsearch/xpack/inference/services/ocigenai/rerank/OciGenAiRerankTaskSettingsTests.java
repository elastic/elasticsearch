/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.RETURN_DOCUMENTS;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.TOP_N;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class OciGenAiRerankTaskSettingsTests extends AbstractBWCWireSerializationTestCase<OciGenAiRerankTaskSettings> {

    public static OciGenAiRerankTaskSettings createRandom() {
        return new OciGenAiRerankTaskSettings(randomBoolean() ? null : randomIntBetween(1, 100), randomOptionalBoolean());
    }

    public void testFromMap_ParsesFields() {
        var settings = OciGenAiRerankTaskSettings.fromMap(new HashMap<>(Map.of(TOP_N, 3, RETURN_DOCUMENTS, true)));

        assertThat(settings.getTopN(), is(3));
        assertThat(settings.getReturnDocuments(), is(true));
    }

    public void testFromMap_Empty_ReturnsEmptySettings() {
        assertThat(OciGenAiRerankTaskSettings.fromMap(new HashMap<>()), sameInstance(OciGenAiRerankTaskSettings.EMPTY_SETTINGS));
        assertThat(OciGenAiRerankTaskSettings.fromMap(null), sameInstance(OciGenAiRerankTaskSettings.EMPTY_SETTINGS));
    }

    public void testFromMap_ThrowsForNonPositiveTopN() {
        var exception = expectThrows(ValidationException.class, () -> OciGenAiRerankTaskSettings.fromMap(new HashMap<>(Map.of(TOP_N, 0))));

        assertThat(exception.getMessage(), containsString("[top_n] must be a positive integer"));
    }

    public void testOf_PrefersRequestSettings() {
        var merged = OciGenAiRerankTaskSettings.of(new OciGenAiRerankTaskSettings(3, true), new OciGenAiRerankTaskSettings(5, null));

        assertThat(merged.getTopN(), is(5));
        assertThat(merged.getReturnDocuments(), is(true));
    }

    public void testToXContent() throws IOException {
        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        new OciGenAiRerankTaskSettings(2, false).toXContent(builder, null);

        assertThat(Strings.toString(builder), is(XContentHelper.stripWhitespace("""
            { "top_n": 2, "return_documents": false }
            """)));
    }

    @Override
    protected Writeable.Reader<OciGenAiRerankTaskSettings> instanceReader() {
        return OciGenAiRerankTaskSettings::new;
    }

    @Override
    protected OciGenAiRerankTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OciGenAiRerankTaskSettings mutateInstance(OciGenAiRerankTaskSettings instance) throws IOException {
        return randomValueOtherThan(instance, OciGenAiRerankTaskSettingsTests::createRandom);
    }

    @Override
    protected OciGenAiRerankTaskSettings mutateInstanceForVersion(OciGenAiRerankTaskSettings instance, TransportVersion version) {
        return instance;
    }
}
