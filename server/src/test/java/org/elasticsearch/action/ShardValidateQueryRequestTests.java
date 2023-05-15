/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action;

import org.elasticsearch.action.admin.indices.validate.query.ShardValidateQueryRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ShardValidateQueryRequestTests extends ESTestCase {
    protected NamedWriteableRegistry namedWriteableRegistry;

    public void setUp() throws Exception {
        super.setUp();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(IndicesModule.getNamedWriteables());
        entries.addAll(searchModule.getNamedWriteables());
        namedWriteableRegistry = new NamedWriteableRegistry(entries);
    }

    public void testSerialize() throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            ValidateQueryRequest validateQueryRequest = new ValidateQueryRequest("indices");
            validateQueryRequest.query(QueryBuilders.termQuery("field", "value"));
            validateQueryRequest.rewrite(true);
            validateQueryRequest.explain(false);
            ShardValidateQueryRequest request = new ShardValidateQueryRequest(
                new ShardId("index", "foobar", 1),
                AliasFilter.of(QueryBuilders.termQuery("filter_field", "value"), "alias0", "alias1"),
                validateQueryRequest
            );
            request.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                ShardValidateQueryRequest readRequest = new ShardValidateQueryRequest(in);
                assertEquals(request.filteringAliases(), readRequest.filteringAliases());
                assertEquals(request.explain(), readRequest.explain());
                assertEquals(request.query(), readRequest.query());
                assertEquals(request.rewrite(), readRequest.rewrite());
                assertEquals(request.shardId(), readRequest.shardId());
            }
        }
    }
}
