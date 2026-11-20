/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plan.QuerySettings;

import java.time.ZoneId;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class EsqlQueryRequestBuilderTests extends ESTestCase {

    private static EsqlQueryRequestBuilder newBuilder() {
        return EsqlQueryRequestBuilder.newSyncEsqlQueryRequestBuilder(mock(ElasticsearchClient.class));
    }

    public void testIncludeCcsMetadata() {
        EsqlQueryRequestBuilder builder = newBuilder();
        assertThat(builder.request().includeCCSMetadata(), nullValue());
        assertSame(builder, builder.includeCcsMetadata(true));
        assertThat(builder.request().includeCCSMetadata(), equalTo(Boolean.TRUE));
    }

    public void testIncludeExecutionMetadata() {
        EsqlQueryRequestBuilder builder = newBuilder();
        assertThat(builder.request().includeExecutionMetadata(), nullValue());
        assertSame(builder, builder.includeExecutionMetadata(true));
        assertThat(builder.request().includeExecutionMetadata(), equalTo(Boolean.TRUE));
    }

    public void testTimeZone() {
        EsqlQueryRequestBuilder builder = newBuilder();
        assertSame(builder, builder.timeZone(ZoneId.of("Europe/Paris")));
        assertThat(builder.request().get(QuerySettings.TIME_ZONE), equalTo(ZoneId.of("Europe/Paris")));
    }
}
