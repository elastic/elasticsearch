/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public abstract class AbstractCohereServiceSettingsTests<T extends CohereServiceSettings> extends AbstractBWCSerializationTestCase<T> {

    protected static final ConfigurationParseContext PARSE_CONTEXT = ConfigurationParseContext.PERSISTENT;

    protected boolean ignoreUnknownFields = randomBoolean();

    protected abstract T createGivenCommonSettings(Map<String, Object> commonSettings, ConfigurationParseContext context);

    protected abstract XContentBuilder toXContentFragmentOfExposedFields(T instance, XContentBuilder builder) throws IOException;

    @Override
    public boolean supportsUnknownFields() {
        return ignoreUnknownFields;
    }

    public void testFromMap_Request_SetModelId() {
        var serviceSettings = createGivenCommonSettings(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, "my-model")),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings.commonSettings().modelId(), is("my-model"));
        assertThat(serviceSettings.commonSettings().apiVersion(), is(CohereCommonServiceSettings.CohereApiVersion.V2));
    }

    public void testFromMap_Request_V2_RequiresModelId() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> createGivenCommonSettings(new HashMap<>(), ConfigurationParseContext.REQUEST)
        );

        assertThat(thrownException.getMessage(), containsString(CohereCommonServiceSettings.MODEL_REQUIRED_FOR_V2_API));
    }

    public void testFromMap_Request_DeprecatedModelField() {
        var serviceSettings = createGivenCommonSettings(
            new HashMap<>(Map.of(CohereCommonServiceSettings.OLD_MODEL_ID_FIELD, "old-model")),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings.commonSettings().modelId(), is("old-model"));
    }

    public void testFromMap_Persistent_EmptyMap_DefaultsToV1() {
        var serviceSettings = createGivenCommonSettings(new HashMap<>(), ConfigurationParseContext.PERSISTENT);

        assertThat(serviceSettings.commonSettings().apiVersion(), is(CohereCommonServiceSettings.CohereApiVersion.V1));
        assertThat(serviceSettings.commonSettings().modelId(), is((String) null));
    }

    public void testFromMap_Persistent_WithApiVersion() {
        var serviceSettings = createGivenCommonSettings(
            new HashMap<>(Map.of(CohereCommonServiceSettings.API_VERSION, "v2", ServiceFields.MODEL_ID, "m")),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings.commonSettings().apiVersion(), is(CohereCommonServiceSettings.CohereApiVersion.V2));
        assertThat(serviceSettings.commonSettings().modelId(), is("m"));
    }

    public void testToXContent_ExposedFields_DoesNotContainApiVersion() throws IOException {
        var serviceSettings = createTestInstance();

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        toXContentFragmentOfExposedFields(serviceSettings, builder);
        builder.endObject();
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult.contains(CohereCommonServiceSettings.API_VERSION), is(false));
    }

    public void testToXContentFragment_ContainsApiVersion() throws IOException {
        var serviceSettings = createTestInstance();

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, containsString(CohereCommonServiceSettings.API_VERSION));
    }
}
