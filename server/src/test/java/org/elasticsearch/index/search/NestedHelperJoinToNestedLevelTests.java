/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.InnerHitsRewriteContext;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.junit.Before;

import java.util.Collections;
import java.util.Set;

/**
 * Unit tests for {@link NestedHelper#joinToNestedLevel}: filters are joined from the level of the fields they read, and
 * filters of unknown level from the given parent level when {@link NestedHelper#mightMatchNonNestedDocs} says so.
 */
public class NestedHelperJoinToNestedLevelTests extends MapperServiceTestCase {

    private SearchExecutionContext context;

    @Before
    public void initContext() throws Exception {
        String vectorIndexType = randomFrom("flat", "hnsw");
        context = createSearchExecutionContext(createMapperService(Strings.format("""
            { "_doc": {
              "runtime": { "nested1.rt": { "type": "keyword" } },
              "properties": {
                "foo": { "type": "keyword" },
                "text": { "type": "text", "norms": false },
                "nested1": { "type": "nested", "properties": {
                  "foo": { "type": "keyword" },
                  "text": { "type": "text", "norms": false },
                  "vector": { "type": "dense_vector", "dims": 2, "similarity": "l2_norm", "index_options": { "type": "%s" } },
                  "deep": { "type": "nested", "properties": { "foo": { "type": "keyword" } } }
                } },
                "nested2": { "type": "nested", "include_in_parent": true, "properties": { "foo": { "type": "keyword" } } },
                "nested3": { "type": "nested", "include_in_root": true, "properties": { "foo": { "type": "keyword" } } },
                "other": { "type": "nested", "properties": { "foo": { "type": "keyword" } } }
              }
            } }
            """, vectorIndexType)));
    }

    public void testLevels() {
        assertEquals(Set.of("nested1"), NestedHelper.nestedLevels(term("nested1.foo"), context));
        assertEquals(Set.of("nested1.deep"), NestedHelper.nestedLevels(term("nested1.deep.foo"), context));
        assertEquals(rootLevel(), NestedHelper.nestedLevels(term("foo"), context));
        // exists on a field without doc values or norms is a term of _field_names
        assertEquals(
            Set.of("nested1"),
            NestedHelper.nestedLevels(new TermQuery(new Term(FieldNamesFieldMapper.NAME, "nested1.text")), context)
        );
        // fields indexed at more than one level, metadata fields, runtime fields and unmapped fields have no known level
        assertEquals(Set.of(), NestedHelper.nestedLevels(term("nested2.foo"), context));
        assertEquals(Set.of(), NestedHelper.nestedLevels(term("nested3.foo"), context));
        assertEquals(Set.of(), NestedHelper.nestedLevels(term("_id"), context));
        assertEquals(Set.of(), NestedHelper.nestedLevels(term("nested1.rt"), context));
        assertEquals(Set.of(), NestedHelper.nestedLevels(term("nested1.missing"), context));
        assertEquals(Set.of(), NestedHelper.nestedLevels(Queries.ALL_DOCS_INSTANCE, context));
        // block joins that are not nested queries are leaves without fields
        assertEquals(Set.of(), NestedHelper.nestedLevels(joinFrom(term("foo"), null), context));
    }

    public void testVectorQueryLevels() throws Exception {
        Float similarity = randomBoolean() ? null : 10f;
        KnnVectorQueryBuilder knn = new KnnVectorQueryBuilder("nested1.vector", new float[] { 1, 1 }, 10, 10, null, null, similarity);
        context.nestedScope().nextLevel(context.nestedLookup().getNestedMappers().get("nested1"));
        try {
            assertEquals(Set.of("nested1"), NestedHelper.nestedLevels(knn.toQuery(context), context));
            // inner hits rewrite knn queries to exact ones
            QueryBuilder exact = knn.rewrite(new InnerHitsRewriteContext(context.getParserConfig(), System::currentTimeMillis));
            assertEquals(Set.of("nested1"), NestedHelper.nestedLevels(exact.toQuery(context), context));
        } finally {
            context.nestedScope().previousLevel();
        }
    }

    public void testNestedQueryLevelIsTheLevelItJoinsTo() throws Exception {
        Query nested = new NestedQueryBuilder("nested1", new TermQueryBuilder("nested1.foo", "x"), ScoreMode.None).toQuery(context);
        assertEquals(rootLevel(), NestedHelper.nestedLevels(nested, context));

        context.nestedScope().nextLevel(context.nestedLookup().getNestedMappers().get("nested1"));
        try {
            Query deep = new NestedQueryBuilder("nested1.deep", new TermQueryBuilder("nested1.deep.foo", "x"), ScoreMode.None).toQuery(
                context
            );
            assertEquals(Set.of("nested1"), NestedHelper.nestedLevels(deep, context));
            // a nested query on a sibling object joins to the common ancestor, the root
            Query sibling = new NestedQueryBuilder("nested2", new TermQueryBuilder("nested2.foo", "x"), ScoreMode.None).toQuery(context);
            assertEquals(rootLevel(), NestedHelper.nestedLevels(sibling, context));
        } finally {
            context.nestedScope().previousLevel();
        }
    }

    public void testKnownLevels() {
        // the vector's own level is applied as is, ancestors are joined from their level, however deep the vector is
        assertEquals(term("nested1.deep.foo"), join(term("nested1.deep.foo"), "nested1.deep", "nested1"));
        assertEquals(joinFrom(term("nested1.foo"), "nested1"), join(term("nested1.foo"), "nested1.deep", null));
        assertEquals(joinFrom(term("foo"), null), join(term("foo"), "nested1.deep", "nested1"));

        // negation is resolved to the level of its field
        Query negation = new BooleanQuery.Builder().add(term("nested1.deep.foo"), Occur.MUST_NOT)
            .add(Queries.ALL_DOCS_INSTANCE, Occur.FILTER)
            .build();
        assertSame(negation, join(negation, "nested1.deep", "nested1"));

        // clauses without a known level follow the level of their siblings
        Query withMetadata = new BooleanQuery.Builder().add(term("nested1.deep.foo"), Occur.MUST).add(term("_id"), Occur.MUST).build();
        assertSame(withMetadata, join(withMetadata, "nested1.deep", "nested1"));
    }

    public void testMixedLevels() {
        Query mixed = new BooleanQuery.Builder().add(term("foo"), Occur.MUST)
            .add(term("nested1.foo"), Occur.SHOULD)
            .add(term("nested1.deep.foo"), Occur.MUST_NOT)
            .add(term("_id"), Occur.FILTER)
            .setMinimumNumberShouldMatch(1)
            .build();
        Query expected = new BooleanQuery.Builder().add(joinFrom(term("foo"), null), Occur.MUST)
            .add(joinFrom(term("nested1.foo"), "nested1"), Occur.SHOULD)
            .add(term("nested1.deep.foo"), Occur.MUST_NOT)
            .add(unknownLevel(term("_id"), "nested1.deep", "nested1"), Occur.FILTER)
            .setMinimumNumberShouldMatch(1)
            .build();
        assertEquals(expected, join(mixed, "nested1.deep", "nested1"));
    }

    public void testUnknownLevels() {
        for (Query query : new Query[] {
            term("nested2.foo"),
            term("nested3.foo"),
            term("_id"),
            term("nested1.rt"),
            term("nested1.missing"),
            Queries.ALL_DOCS_INSTANCE,
            joinFrom(term("foo"), null) }) {
            for (String[] levels : new String[][] { { "nested1", null }, { "nested1.deep", "nested1" }, { "nested1.deep", null } }) {
                assertEquals(query.toString(), unknownLevel(query, levels[0], levels[1]), join(query, levels[0], levels[1]));
            }
        }
    }

    public void testLevelsBelowOrBesideTheTarget() {
        // a level below the vector's, or on another branch, cannot be joined down to the vector's documents
        assertEquals(unknownLevel(term("nested1.deep.foo"), "nested1", null), join(term("nested1.deep.foo"), "nested1", null));
        assertEquals(unknownLevel(term("other.foo"), "nested1", null), join(term("other.foo"), "nested1", null));
    }

    private Query join(Query filter, String nestedPath, @Nullable String fallbackParentPath) {
        return NestedHelper.joinToNestedLevel(filter, nestedPath, fallbackParentPath, context);
    }

    /** The rewrite of filters of unknown level. */
    private Query unknownLevel(Query filter, String nestedPath, @Nullable String fallbackParentPath) {
        return NestedHelper.mightMatchNonNestedDocs(filter, nestedPath, context) ? joinFrom(filter, fallbackParentPath) : filter;
    }

    private Query joinFrom(Query filter, @Nullable String level) {
        Query levelFilter = level == null
            ? Queries.newNonNestedFilter(context.indexVersionCreated())
            : context.nestedLookup().getNestedMappers().get(level).nestedTypeFilter();
        return new ToChildBlockJoinQuery(Queries.filtered(filter, levelFilter), context.bitsetFilter(levelFilter));
    }

    private static Query term(String field) {
        return new TermQuery(new Term(field, "x"));
    }

    private static Set<String> rootLevel() {
        return Collections.singleton(null);
    }
}
