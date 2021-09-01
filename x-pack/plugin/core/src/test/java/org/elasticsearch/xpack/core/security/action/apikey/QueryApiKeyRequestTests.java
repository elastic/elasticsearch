/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class QueryApiKeyRequestTests extends ESTestCase {

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        final SearchModule searchModule = new SearchModule(Settings.EMPTY, List.of());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    public void testReadWrite() throws IOException {
        final QueryApiKeyRequest request1 = new QueryApiKeyRequest();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request1.writeTo(out);
            try (StreamInput in = new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().array()))) {
                assertThat(new QueryApiKeyRequest(in).getQueryBuilder(), nullValue());
            }
        }

        final BoolQueryBuilder boolQueryBuilder2 = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("foo", "bar"))
            .should(QueryBuilders.idsQuery().addIds("id1", "id2"))
            .must(QueryBuilders.wildcardQuery("a.b", "t*y"))
            .mustNot(QueryBuilders.prefixQuery("value", "prod"));
        final QueryApiKeyRequest request2 = new QueryApiKeyRequest(boolQueryBuilder2);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request2.writeTo(out);
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry())) {
                final QueryApiKeyRequest deserialized = new QueryApiKeyRequest(in);
                assertThat(deserialized.getQueryBuilder().getClass(), is(BoolQueryBuilder.class));
                assertThat((BoolQueryBuilder) deserialized.getQueryBuilder(), equalTo(boolQueryBuilder2));
            }
        }

        final QueryApiKeyRequest request3 = new QueryApiKeyRequest(
            QueryBuilders.matchAllQuery(),
            42,
            20,
            List.of(new FieldSortBuilder("name"),
                new FieldSortBuilder("creation_time").setFormat("strict_date_time").order(SortOrder.DESC),
                new FieldSortBuilder("username")),
            new SearchAfterBuilder().setSortValues(new String[] { "key-2048", "2021-07-01T00:00:59.000Z" }));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request3.writeTo(out);
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry())) {
                final QueryApiKeyRequest deserialized = new QueryApiKeyRequest(in);
                assertThat(deserialized.getQueryBuilder().getClass(), is(MatchAllQueryBuilder.class));
                assertThat(deserialized.getFrom(), equalTo(request3.getFrom()));
                assertThat(deserialized.getSize(), equalTo(request3.getSize()));
                assertThat(deserialized.getFieldSortBuilders(), equalTo(request3.getFieldSortBuilders()));
                assertThat(deserialized.getSearchAfterBuilder(), equalTo(request3.getSearchAfterBuilder()));
            }
        }
    }

    public void testValidate() {
        final QueryApiKeyRequest request1 =
            new QueryApiKeyRequest(null, randomIntBetween(0, Integer.MAX_VALUE), randomIntBetween(0, Integer.MAX_VALUE), null, null);
        assertThat(request1.validate(), nullValue());

        final QueryApiKeyRequest request2 =
            new QueryApiKeyRequest(null, randomIntBetween(Integer.MIN_VALUE, -1), randomIntBetween(0, Integer.MAX_VALUE), null, null);
        assertThat(request2.validate().getMessage(), containsString("[from] parameter cannot be negative"));

        final QueryApiKeyRequest request3 =
            new QueryApiKeyRequest(null, randomIntBetween(0, Integer.MAX_VALUE), randomIntBetween(Integer.MIN_VALUE, -1), null, null);
        assertThat(request3.validate().getMessage(), containsString("[size] parameter cannot be negative"));
    }
}
