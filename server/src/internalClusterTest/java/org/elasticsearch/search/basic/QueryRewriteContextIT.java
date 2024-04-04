/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.basic;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class QueryRewriteContextIT extends ESIntegTestCase {
    private static class TestQueryBuilder extends AbstractQueryBuilder<TestQueryBuilder> {
        private static final String NAME = "test";

        private static TestQueryBuilder fromXContent(XContentParser parser) {
            return new TestQueryBuilder();
        }

        TestQueryBuilder() {
        }

        TestQueryBuilder(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {

        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.endObject();
        }

        @Override
        protected Query doToQuery(SearchExecutionContext context) throws IOException {
            return new MatchNoDocsQuery();
        }

        @Override
        protected boolean doEquals(TestQueryBuilder other) {
            return true;
        }

        @Override
        protected int doHashCode() {
            return 0;
        }
    }

    public static class TestPlugin extends Plugin implements SearchPlugin {
        public TestPlugin() {
        }

        @Override
        public List<QuerySpec<?>> getQueries() {
            return List.of(new QuerySpec<QueryBuilder>(TestQueryBuilder.NAME, TestQueryBuilder::new, TestQueryBuilder::fromXContent));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPlugin.class);
    }

    public void testIndexMetadataMap_TransportSearchAction() {
        // TODO: Test wildcard resolution & aliases
        createIndex("test1", "test2");

        AtomicBoolean gotQueryRewriteContext = new AtomicBoolean(false);
        assertNoFailures(prepareSearch("test1", "test2").setQuery(new TestQueryBuilder() {
            @Override
            protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
                // Check that the first QueryRewriteContext received has a non-empty index metadata map.
                // Later QueryRewriteContext instances received, such as the one generated in the can-match phase, will have an empty index
                // metadata map.
                if (queryRewriteContext.getClass() == QueryRewriteContext.class && gotQueryRewriteContext.getAndSet(true) == false) {
                    Map<String, IndexMetadata> indexMetadataMap = queryRewriteContext.getIndexMetadataMap();
                    assertThat(indexMetadataMap, notNullValue());
                    assertThat(indexMetadataMap.keySet(), equalTo(Set.of("test1", "test2")));
                }

                return super.doRewrite(queryRewriteContext);
            }
        }));
        assertThat(gotQueryRewriteContext.get(), is(true));
    }

    public void testIndexMetadataMap_TransportExplainAction() {
        createIndex("test");

        AtomicBoolean gotQueryRewriteContext = new AtomicBoolean(false);
        client().prepareExplain("test", "1").setQuery(new TestQueryBuilder() {
            @Override
            protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
                // Check that the first QueryRewriteContext received has a non-empty index metadata map.
                // Later QueryRewriteContext instances received, such as the one generated in the can-match phase, will have an empty index
                // metadata map.
                if (queryRewriteContext.getClass() == QueryRewriteContext.class && gotQueryRewriteContext.getAndSet(true) == false) {
                    Map<String, IndexMetadata> indexMetadataMap = queryRewriteContext.getIndexMetadataMap();
                    assertThat(indexMetadataMap, notNullValue());
                    assertThat(indexMetadataMap.keySet(), equalTo(Set.of("test")));
                }

                return super.doRewrite(queryRewriteContext);
            }
        }).get();
        assertThat(gotQueryRewriteContext.get(), is(true));
    }

    public void testIndexMetadataMap_TransportValidateQueryAction() {
        // TODO: Test wildcard resolution & aliases
        createIndex("test1", "test2");

        AtomicBoolean gotQueryRewriteContext = new AtomicBoolean(false);
        ValidateQueryRequest validateQueryRequest = new ValidateQueryRequest("test1", "test2").query(new TestQueryBuilder() {
            @Override
            protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
                // Check that the first QueryRewriteContext received has a non-empty index metadata map.
                // Later QueryRewriteContext instances received, such as the one generated in the can-match phase, will have an empty index
                // metadata map.
                if (queryRewriteContext.getClass() == QueryRewriteContext.class && gotQueryRewriteContext.getAndSet(true) == false) {
                    Map<String, IndexMetadata> indexMetadataMap = queryRewriteContext.getIndexMetadataMap();
                    assertThat(indexMetadataMap, notNullValue());
                    assertThat(indexMetadataMap.keySet(), equalTo(Set.of("test1", "test2")));
                }

                return super.doRewrite(queryRewriteContext);
            }
        });

        client().admin().indices().validateQuery(validateQueryRequest, new ActionListener<>() {
            @Override
            public void onResponse(ValidateQueryResponse validateQueryResponse) {
                assertThat(validateQueryResponse.getStatus(), equalTo(RestStatus.OK));
                assertThat(validateQueryResponse.isValid(), is(true));
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        });
        assertThat(gotQueryRewriteContext.get(), is(true));
    }
}
