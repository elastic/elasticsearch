/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.search.Query;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

// TODO: Add dense vector tests

public class SemanticQueryBuilderTests extends AbstractQueryTestCase<SemanticQueryBuilder> {

    private static final String SEMANTIC_TEXT_SPARSE_FIELD = "semantic_sparse";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(InferencePlugin.class, TestInferenceServicePlugin.class);
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge(
            "_doc",
            new CompressedXContent(
                Strings.toString(PutMappingRequest.simpleMapping(SEMANTIC_TEXT_SPARSE_FIELD, "type=semantic_text,model_id=test_service"))
            ),
            MapperService.MergeReason.MAPPING_UPDATE
        );
    }

    @Override
    protected SemanticQueryBuilder doCreateTestQueryBuilder() {
        SemanticQueryBuilder builder = new SemanticQueryBuilder(SEMANTIC_TEXT_SPARSE_FIELD, randomAlphaOfLength(4));
        if (randomBoolean()) {
            builder.boost((float) randomDoubleBetween(0.1, 10.0, true));
        }
        if (randomBoolean()) {
            builder.queryName(randomAlphaOfLength(4));
        }

        return builder;
    }

    @Override
    protected void doAssertLuceneQuery(SemanticQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(ESToParentBlockJoinQuery.class));

        // TODO: Extend assertion
    }
}
