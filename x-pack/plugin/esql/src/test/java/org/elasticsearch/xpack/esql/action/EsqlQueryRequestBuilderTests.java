/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.ExecutionMetadata;
import org.elasticsearch.xpack.esql.plan.QuerySettingDef;
import org.elasticsearch.xpack.esql.plan.QuerySettings;

import java.time.ZoneId;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class EsqlQueryRequestBuilderTests extends ESTestCase {

    private static EsqlQueryRequestBuilder newBuilder() {
        return EsqlQueryRequestBuilder.newSyncEsqlQueryRequestBuilder(mock(ElasticsearchClient.class));
    }

    public void testExecutionMetadataUnsetByDefault() {
        EsqlQueryRequestBuilder builder = newBuilder();
        assertThat(builder.request().includeCCSMetadata(), nullValue());
        assertThat(builder.request().includeExecutionMetadata(), nullValue());
    }

    public void testExecutionMetadataCcsOnly() {
        EsqlQueryRequestBuilder builder = newBuilder();
        assertSame(builder, builder.executionMetadata(ExecutionMetadata.CCS_ONLY));
        assertThat(builder.request().includeCCSMetadata(), equalTo(Boolean.TRUE));
        assertThat(builder.request().includeExecutionMetadata(), nullValue());
    }

    public void testExecutionMetadataAlways() {
        EsqlQueryRequestBuilder builder = newBuilder();
        assertSame(builder, builder.executionMetadata(ExecutionMetadata.ALWAYS));
        assertThat(builder.request().includeCCSMetadata(), nullValue());
        assertThat(builder.request().includeExecutionMetadata(), equalTo(Boolean.TRUE));
    }

    public void testExecutionMetadataReplacesEarlierMode() {
        EsqlQueryRequestBuilder builder = newBuilder();
        builder.executionMetadata(ExecutionMetadata.ALWAYS).executionMetadata(ExecutionMetadata.CCS_ONLY);
        assertThat(builder.request().includeCCSMetadata(), equalTo(Boolean.TRUE));
        assertThat(builder.request().includeExecutionMetadata(), nullValue());

        builder.executionMetadata(ExecutionMetadata.NEVER);
        assertThat(builder.request().includeCCSMetadata(), nullValue());
        assertThat(builder.request().includeExecutionMetadata(), nullValue());
    }

    public void testSettingString() {
        EsqlQueryRequestBuilder builder = newBuilder();
        assertSame(builder, builder.setting("time_zone", "Europe/Paris"));
        assertThat(builder.request().get(QuerySettings.TIME_ZONE), equalTo(ZoneId.of("Europe/Paris")));
    }

    public void testSettingBoolean() {
        EsqlQueryRequestBuilder builder = newBuilder();
        builder.setting("column_metadata", true);
        assertThat(builder.request().requestSettings().get(QuerySettings.COLUMN_METADATA), equalTo(Boolean.TRUE));
    }

    public void testSettingNullRemoves() {
        EsqlQueryRequestBuilder builder = newBuilder();
        builder.setting("time_zone", "Europe/Paris").setting("time_zone", null);
        assertFalse(builder.request().requestSettings().containsKey(QuerySettings.TIME_ZONE));
    }

    public void testSettingUnknownName() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> newBuilder().setting("no_such_setting", "x"));
        assertThat(e.getMessage(), equalTo("Unknown setting [no_such_setting]"));
    }

    public void testSettingNotExposedInRequestBody() {
        QuerySettingDef<?> def = QuerySettings.ALL.stream().filter(d -> d.requestBody() == false).findFirst().orElse(null);
        assumeTrue("every setting is exposed in the request body", def != null);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> newBuilder().setting(def.name(), "x"));
        assertThat(e.getMessage(), equalTo("Setting [" + def.name() + "] is not exposed as a request body parameter"));
    }

    public void testSettingInvalidValue() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> newBuilder().setting("time_zone", "Not/A_Zone"));
        assertThat(e.getMessage(), containsString("Failed to parse value for setting [time_zone]"));
    }
}
