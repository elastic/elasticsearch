/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;

public class IdsQueryBuilderTests extends AbstractQueryTestCase<IdsQueryBuilder> {

    @Override
    protected IdsQueryBuilder doCreateTestQueryBuilder() {
        final String type;
        if (randomBoolean()) {
            if (frequently()) {
                type = "_doc";
            } else {
                type = randomAlphaOfLengthBetween(1, 10);
            }
        } else if (randomBoolean()) {
            type = Metadata.ALL;
        } else {
            type = null;
        }
        int numberOfIds = randomIntBetween(0, 10);
        String[] ids = new String[numberOfIds];
        for (int i = 0; i < numberOfIds; i++) {
            ids[i] = randomAlphaOfLengthBetween(1, 10);
        }
        IdsQueryBuilder query;
        if (type != null && randomBoolean()) {
            query = new IdsQueryBuilder().types(type);
            query.addIds(ids);
        } else {
            query = new IdsQueryBuilder();
            query.addIds(ids);
        }
        return query;
    }

    @Override
    protected void doAssertLuceneQuery(IdsQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        boolean allTypes = queryBuilder.types().length == 0 || queryBuilder.types().length == 1 && "_all".equals(queryBuilder.types()[0]);
        if (queryBuilder.ids().size() == 0
            // no types
            || context.getFieldType(IdFieldMapper.NAME) == null
            // there are types, but disjoint from the query
            || (allTypes == false && Arrays.asList(queryBuilder.types()).indexOf(context.getType()) == -1)) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        } else {
            assertThat(query, instanceOf(TermInSetQuery.class));
        }
    }

    public void testIllegalArguments() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new IdsQueryBuilder().types((String[]) null));
        assertEquals("[ids] types cannot be null", e.getMessage());

        IdsQueryBuilder idsQueryBuilder = new IdsQueryBuilder();
        e = expectThrows(IllegalArgumentException.class, () -> idsQueryBuilder.addIds((String[]) null));
        assertEquals("[ids] ids cannot be null", e.getMessage());
    }

    // see #7686.
    public void testIdsQueryWithInvalidValues() throws Exception {
        String query = "{ \"ids\": { \"values\": [[1]] } }";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(query));
        assertThat(e.getMessage(), containsString("[ids] failed to parse field [values]"));
    }

    public void testFromJson() throws IOException {
        String json = "{\n"
            + "  \"ids\" : {\n"
            + "    \"type\" : [ \"my_type\" ],\n"
            + "    \"values\" : [ \"1\", \"100\", \"4\" ],\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
        IdsQueryBuilder parsed = (IdsQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertThat(parsed.ids(), contains("1", "100", "4"));
        assertEquals(json, "my_type", parsed.types()[0]);

        // check that type that is not an array and also ids that are numbers are parsed
        json = "{\n"
            + "  \"ids\" : {\n"
            + "    \"type\" : \"my_type\",\n"
            + "    \"values\" : [ 1, 100, 4 ],\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
        parsed = (IdsQueryBuilder) parseQuery(json);
        assertThat(parsed.ids(), contains("1", "100", "4"));
        assertEquals(json, "my_type", parsed.types()[0]);

        // check with empty type array
        json = "{\n"
            + "  \"ids\" : {\n"
            + "    \"type\" : [ ],\n"
            + "    \"values\" : [ \"1\", \"100\", \"4\" ],\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
        parsed = (IdsQueryBuilder) parseQuery(json);
        assertThat(parsed.ids(), contains("1", "100", "4"));
        assertEquals(json, 0, parsed.types().length);

        // check without type
        json = "{\n" + "  \"ids\" : {\n" + "    \"values\" : [ \"1\", \"100\", \"4\" ],\n" + "    \"boost\" : 1.0\n" + "  }\n" + "}";
        parsed = (IdsQueryBuilder) parseQuery(json);
        assertThat(parsed.ids(), contains("1", "100", "4"));
        assertEquals(json, 0, parsed.types().length);
    }

    @Override
    protected QueryBuilder parseQuery(XContentParser parser) throws IOException {
        QueryBuilder query = super.parseQuery(parser);
        assertThat(query, instanceOf(IdsQueryBuilder.class));

        IdsQueryBuilder idsQuery = (IdsQueryBuilder) query;
        if (idsQuery.types().length > 0) {
            assertWarnings(IdsQueryBuilder.TYPES_DEPRECATION_MESSAGE);
        }
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
