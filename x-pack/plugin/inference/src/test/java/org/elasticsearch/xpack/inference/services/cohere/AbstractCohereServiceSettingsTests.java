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
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings.CohereApiVersion;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public abstract class AbstractCohereServiceSettingsTests<T extends CohereServiceSettings> extends AbstractBWCSerializationTestCase<T> {

    /**
     * We always use the {@link ConfigurationParseContext#PERSISTENT} context for tests because api_version gets
     * serialized regardless of the context but we can only deserialize it in the {@link ConfigurationParseContext#PERSISTENT} context.
     */
    protected static final ConfigurationParseContext PARSE_CONTEXT = ConfigurationParseContext.PERSISTENT;

    protected static final boolean ignoreUnknownFields = true;

    protected abstract T createGivenCommonSettings(Map<String, Object> commonSettings, ConfigurationParseContext context);

    protected abstract XContentBuilder toXContentFragmentOfExposedFields(T instance, XContentBuilder builder) throws IOException;

    protected Set<String> getImmutableFields() {
        return Set.of(
            CohereCommonServiceSettings.API_VERSION,
            ServiceFields.MODEL_ID,
            CohereCommonServiceSettings.OLD_MODEL_ID_FIELD,
            ServiceFields.URL
        );
    }

    public void testFromMap_Request_SetModelId() {
        var serviceSettings = createGivenCommonSettings(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, "my-model")),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings.commonSettings().modelId(), is("my-model"));
        assertThat(serviceSettings.commonSettings().apiVersion(), is(CohereApiVersion.V2));
    }

    public void testFromMap_Request_V2_RequiresModelId() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> createGivenCommonSettings(new HashMap<>(), ConfigurationParseContext.REQUEST)
        );

        assertThat(thrownException.getMessage(), containsString(CohereCommonServiceSettings.MODEL_REQUIRED_FOR_V2_API));
    }

    public void testFromMap_Persistent_V2_RequiresModelId() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> createGivenCommonSettings(
                Map.of(CohereCommonServiceSettings.API_VERSION, CohereApiVersion.V2),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(thrownException.getMessage(), containsString(CohereCommonServiceSettings.MODEL_REQUIRED_FOR_V2_API));
    }

    public void testFromMap_Persistent_DeprecatedModelField() {
        var serviceSettings = createGivenCommonSettings(
            new HashMap<>(Map.of(CohereCommonServiceSettings.OLD_MODEL_ID_FIELD, "old-model")),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings.commonSettings().modelId(), is("old-model"));
    }

    public void testFromMap_Persistent_EmptyMap_DefaultsToV1() {
        var serviceSettings = createGivenCommonSettings(new HashMap<>(), ConfigurationParseContext.PERSISTENT);

        assertThat(serviceSettings.commonSettings().apiVersion(), is(CohereApiVersion.V1));
        assertThat(serviceSettings.commonSettings().modelId(), is((String) null));
    }

    public void testFromMap_Persistent_WithApiVersion() {
        var serviceSettings = createGivenCommonSettings(
            new HashMap<>(Map.of(CohereCommonServiceSettings.API_VERSION, "v2", ServiceFields.MODEL_ID, "m")),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings.commonSettings().apiVersion(), is(CohereApiVersion.V2));
        assertThat(serviceSettings.commonSettings().modelId(), is("m"));
    }

    public void testFromMap_Request_WithApiVersion() {
        var e = expectThrows(
            XContentParseException.class,
            () -> createGivenCommonSettings(
                new HashMap<>(Map.of(CohereCommonServiceSettings.API_VERSION, "v2", ServiceFields.MODEL_ID, "m")),
                ConfigurationParseContext.REQUEST
            )
        );
        assertThat(e.getMessage(), containsString("unknown field [api_version]"));
    }

    public void testFromMap_GivenBothDeprecatedAndNewModelId_UsesNewModelId() {
        var serviceSettings = createGivenCommonSettings(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, "new-model", CohereCommonServiceSettings.OLD_MODEL_ID_FIELD, "old-model")),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings.commonSettings().modelId(), is("new-model"));
        assertThat(serviceSettings.commonSettings().apiVersion(), is(CohereApiVersion.V2));
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

    public void testUpdateServiceSettings_GivenImmutableFields_ShouldThrow() {
        var serviceSettings = createTestInstance();

        for (String immutableField : getImmutableFields()) {
            var e = expectThrows(
                XContentParseException.class,
                () -> serviceSettings.updateServiceSettings(Map.of(immutableField, "value"))
            );
            assertThat(e.getMessage(), containsString("unknown field [" + immutableField + "]"));
        }
    }
}
