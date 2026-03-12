/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.index.IndexSettingsTests.newIndexMeta;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;

public class IdsQueryBuilderTests extends AbstractQueryTestCase<IdsQueryBuilder> {

    @Override
    protected IdsQueryBuilder doCreateTestQueryBuilder() {
        int numberOfIds = randomIntBetween(0, 10);
        String[] ids = new String[numberOfIds];
        for (int i = 0; i < numberOfIds; i++) {
            ids[i] = randomAlphaOfLengthBetween(1, 10);
        }
        IdsQueryBuilder query = new IdsQueryBuilder();
        query.addIds(ids);
        return query;
    }

    @Override
    protected void doAssertLuceneQuery(IdsQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        if (queryBuilder.ids().size() == 0) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        } else {
            assertThat(query, instanceOf(TermInSetQuery.class));
        }
    }

    public void testIllegalArguments() {
        IdsQueryBuilder idsQueryBuilder = new IdsQueryBuilder();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> idsQueryBuilder.addIds((String[]) null));
        assertEquals("[ids] ids cannot be null", e.getMessage());
    }

    // see #7686.
    public void testIdsQueryWithInvalidValues() throws Exception {
        String query = "{ \"ids\": { \"values\": [[1]] } }";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(query));
        assertThat(e.getMessage(), containsString("[ids] failed to parse field [values]"));
    }

    public void testIdsSizeMayNotExceedMaxResultWindow() throws Exception {
        IdsQueryBuilder builder = new IdsQueryBuilder();
        builder.addIds("1", "2", "3");

        final IndexMetadata metadata = newIndexMeta(
            "index1",
            Settings.builder().put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), 2).build()
        );
        final IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);

        SearchExecutionContext searchExecutionContext = new SearchExecutionContext(
            0,
            0,
            settings,
            null,
            null,
            null,
            MappingLookup.EMPTY,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Collections.emptyMap(),
            null,
            MapperMetrics.NOOP,
            SearchExecutionContextHelper.SHARD_SEARCH_STATS
        );

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> builder.doToQuery(searchExecutionContext));
        assertThat(e.getMessage(), equalTo("Too many ids specified, allowed max result window is [2]"));
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "ids" : {
                "values" : [ "1", "100", "4" ],
                "boost" : 1.0
              }
            }""";
        IdsQueryBuilder parsed = (IdsQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertThat(parsed.ids(), contains("1", "100", "4"));

        // check that type that is not an array and also ids that are numbers are parsed
        json = """
            {
              "ids" : {
                "values" : [ 1, 100, 4 ],
                "boost" : 1.0
              }
            }""";
        parsed = (IdsQueryBuilder) parseQuery(json);
        assertThat(parsed.ids(), contains("1", "100", "4"));
    }

    @Override
    protected QueryBuilder parseQuery(XContentParser parser) throws IOException {
        QueryBuilder query = super.parseQuery(parser);
        assertThat(query, instanceOf(IdsQueryBuilder.class));
        return query;
    }

    @Override
    public void testMustRewrite() throws IOException {
        SearchExecutionContext context = createShardContextWithNoType();
        context.setAllowUnmappedFields(true);
        IdsQueryBuilder queryBuilder = createTestQueryBuilder();
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> queryBuilder.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
    }
}
