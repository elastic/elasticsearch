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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class QueryApiKeyRequestTests extends ESTestCase {

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        final SearchModule searchModule = new SearchModule(Settings.EMPTY, false, org.elasticsearch.core.List.of());
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
    }
}
