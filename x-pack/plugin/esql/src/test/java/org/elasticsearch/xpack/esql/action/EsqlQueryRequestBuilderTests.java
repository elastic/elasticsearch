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
}
