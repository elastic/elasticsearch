/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.accesscontrol;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.sandbox.search.CoveringQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.lucene.queries.MinDocQuery;
import org.elasticsearch.search.vectors.DenseVectorQuery;
import org.elasticsearch.search.vectors.VectorSimilarityQuery;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/** Simple tests for opt out query cache*/
public class OptOutQueryCacheTests extends ESTestCase {
    IndexSearcher searcher;
    Directory dir;
    RandomIndexWriter w;
    DirectoryReader reader;

    @Before
    public void initLuceneStuff() throws IOException {
        dir = newDirectory();
        w = new RandomIndexWriter(random(), dir);
        reader = w.getReader();
        searcher = newSearcher(reader);
    }

    @After
    public void closeLuceneStuff() throws IOException {
        w.close();
        dir.close();
        reader.close();
    }

    public void testOptOutQueryCacheSafetyCheck() throws IOException {

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("foo", "bar")), BooleanClause.Occur.MUST);
        builder.add(new TermQuery(new Term("no", "baz")), BooleanClause.Occur.MUST_NOT);
        Weight weight = builder.build().createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);

        // whenever the allowed fields match the fields in the query and we do not deny access to any fields we allow caching.
        IndicesAccessControl.IndexAccessControl permissions = new IndicesAccessControl.IndexAccessControl(
            new FieldPermissions(fieldPermissionDef(new String[] { "foo", "no" }, null)),
            DocumentPermissions.allowAll()
        );
        assertTrue(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            new FieldPermissions(fieldPermissionDef(new String[] { "foo", "no" }, new String[] {})),
            DocumentPermissions.allowAll()
        );
        assertTrue(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            new FieldPermissions(fieldPermissionDef(new String[] { "*" }, new String[] {})),
            DocumentPermissions.allowAll()
        );
        assertTrue(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            new FieldPermissions(fieldPermissionDef(new String[] { "*" }, null)),
            DocumentPermissions.allowAll()
        );
        assertTrue(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            new FieldPermissions(fieldPermissionDef(new String[] { "*" }, new String[] { "oof" })),
            DocumentPermissions.allowAll()
        );
        assertTrue(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            new FieldPermissions(fieldPermissionDef(new String[] { "f*", "n*" }, new String[] {})),
            DocumentPermissions.allowAll()
        );
        assertTrue(OptOutQueryCache.cachingIsSafe(weight, permissions));

        // check we don't cache if a field is not allowed
        permissions = new IndicesAccessControl.IndexAccessControl(
            new FieldPermissions(fieldPermissionDef(new String[] { "foo" }, null)),
            DocumentPermissions.allowAll()
        );
        assertFalse(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            new FieldPermissions(fieldPermissionDef(new String[] { "a*" }, new String[] { "aa" })),
            DocumentPermissions.allowAll()
        );
        assertFalse(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            new FieldPermissions(fieldPermissionDef(null, new String[] { "no" })),
            DocumentPermissions.allowAll()
        );
        assertFalse(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            new FieldPermissions(fieldPermissionDef(null, new String[] { "*" })),
            DocumentPermissions.allowAll()
        );
        assertFalse(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            new FieldPermissions(fieldPermissionDef(new String[] { "foo", "no" }, new String[] { "no" })),
            DocumentPermissions.allowAll()
        );
        assertFalse(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            new FieldPermissions(fieldPermissionDef(new String[] {}, new String[] {})),
            DocumentPermissions.allowAll()
        );
        assertFalse(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            new FieldPermissions(fieldPermissionDef(new String[] {}, null)),
            DocumentPermissions.allowAll()
        );
        assertFalse(OptOutQueryCache.cachingIsSafe(weight, permissions));
    }

    /**
     * Checks that each random query is cacheable only when it is safe to cache, using a searcher over an FLS-filtered reader as in
     * production.
     */
    public void testCachingIsSafeRandomQueries() throws IOException {
        final Set<String> allowedFields = Set.of("foo", "foo_sparse", "_allowed", "_allowed_sparse");
        final Set<String> disallowedFields = Set.of("secret", "secret_sparse", "_disallowed", "_disallowed_sparse");
        final Set<String> denseFields = Stream.concat(allowedFields.stream(), disallowedFields.stream())
            .filter(f -> f.endsWith("_sparse") == false)
            .collect(Collectors.toUnmodifiableSet());

        // metadata fields are granted by default, so _allowed is left out of the grant and _disallowed must be excluded explicitly
        final IndicesAccessControl.IndexAccessControl permissions = new IndicesAccessControl.IndexAccessControl(
            new FieldPermissions(
                fieldPermissionDef(
                    allowedFields.stream().filter(f -> f.startsWith("_") == false).toArray(String[]::new),
                    disallowedFields.stream().filter(f -> f.startsWith("_")).toArray(String[]::new)
                )
            ),
            DocumentPermissions.allowAll()
        );

        try (Directory flsDir = newDirectory()) {
            DirectoryReader unfilteredReader;
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), flsDir)) {
                // one doc has every field and the other only the dense ones, so a sparse field's doc count is less than maxDoc
                Document allFieldsDoc = new Document();
                for (String field : allowedFields) {
                    allFieldsDoc.add(new KeywordField(field, "x", Field.Store.NO));
                }
                for (String field : disallowedFields) {
                    allFieldsDoc.add(new KeywordField(field, "x", Field.Store.NO));
                }

                Document denseFieldsDoc = new Document();
                for (String field : denseFields) {
                    denseFieldsDoc.add(new KeywordField(field, "x", Field.Store.NO));
                }

                // a document block is always written to a single segment, so every sparse field has a FieldInfo in the only leaf
                writer.addDocuments(List.of(allFieldsDoc, denseFieldsDoc));
                unfilteredReader = writer.getReader();
            }

            try (
                DirectoryReader flsReader = permissions.getFieldPermissions()
                    .filter(unfilteredReader, IndexSettingsModule.newIndexSettings("index", Settings.EMPTY), field -> true)
            ) {
                IndexSearcher flsSearcher = newSearcher(flsReader);
                for (int i = 0; i < 100; i++) {
                    // production rewrites before the query cache is consulted
                    RandomQuery query = randomQuery(flsSearcher, allowedFields, disallowedFields, denseFields).rewrite(flsSearcher);
                    Weight weight = query.query().createWeight(flsSearcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
                    assertEquals(query.query().toString(), query.cacheable(), OptOutQueryCache.cachingIsSafe(weight, permissions));

                    // ESToParentBlockJoinQuery visits its child but its weight reports a Lucene query that doesn't, so a compound query
                    // that visits it is checked more precisely than the bare query's weight. Only the bare query is asserted for it.
                    if (query.query() instanceof ESToParentBlockJoinQuery == false) {
                        Query wrapped = wrapInCompoundQuery(query.query());
                        Weight wrappedWeight = wrapped.createWeight(flsSearcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
                        assertEquals(wrapped.toString(), query.cacheable(), OptOutQueryCache.cachingIsSafe(wrappedWeight, permissions));
                    }
                }
            }
        }
    }

    public void testOptOutQueryCacheNoIndicesPermissions() {
        final IndexMetadata indexMetadata = IndexMetadata.builder("index").settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        final IndicesQueryCache indicesQueryCache = mock(IndicesQueryCache.class);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final OptOutQueryCache cache = new OptOutQueryCache(indexMetadata.getIndex(), indicesQueryCache, threadContext);
        final Weight weight = mock(Weight.class);
        final QueryCachingPolicy policy = mock(QueryCachingPolicy.class);
        final Weight w = cache.doCache(weight, policy);
        assertSame(w, weight);
        verifyNoMoreInteractions(indicesQueryCache);
    }

    public void testOptOutQueryCacheIndexDoesNotHaveFieldLevelSecurity() {
        final IndexMetadata indexMetadata = IndexMetadata.builder("index").settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        final IndicesQueryCache indicesQueryCache = mock(IndicesQueryCache.class);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final IndicesAccessControl.IndexAccessControl indexAccessControl = mock(IndicesAccessControl.IndexAccessControl.class);
        when(indexAccessControl.getFieldPermissions()).thenReturn(FieldPermissions.DEFAULT);
        final IndicesAccessControl indicesAccessControl = mock(IndicesAccessControl.class);
        when(indicesAccessControl.getIndexPermissions("index")).thenReturn(indexAccessControl);
        when(indicesAccessControl.isGranted()).thenReturn(true);
        new SecurityContext(Settings.EMPTY, threadContext).putIndicesAccessControl(indicesAccessControl);
        final OptOutQueryCache cache = new OptOutQueryCache(indexMetadata.getIndex(), indicesQueryCache, threadContext);
        final Weight weight = mock(Weight.class);
        final QueryCachingPolicy policy = mock(QueryCachingPolicy.class);
        cache.doCache(weight, policy);
        verify(indicesQueryCache).doCache(same(weight), same(policy));
    }

    private static FieldPermissionsDefinition fieldPermissionDef(String[] granted, String[] denied) {
        return new FieldPermissionsDefinition(granted, denied);
    }

    private static RandomQuery randomQuery(
        IndexSearcher searcher,
        Set<String> allowedFields,
        Set<String> disallowedFields,
        Set<String> denseFields
    ) throws IOException {
        return randomQuery(searcher, allowedFields, disallowedFields, denseFields, randomFrom(QueryType.values()));
    }

    private static RandomQuery randomQuery(
        IndexSearcher searcher,
        Set<String> allowedFields,
        Set<String> disallowedFields,
        Set<String> denseFields,
        QueryType type
    ) throws IOException {
        boolean fieldAllowed = randomBoolean();
        String field = randomFrom(fieldAllowed ? allowedFields : disallowedFields);
        boolean cacheable = fieldAllowed && field.startsWith("_") == false; // Queries on metadata fields are never cached
        BitSetProducer parents = new QueryBitSetProducer(new TermQuery(new Term("parent", "true")));

        return switch (type) {
            case TERM -> new RandomQuery(new TermQuery(new Term(field, "x")), cacheable);
            case PHRASE -> new RandomQuery(new PhraseQuery(field, "a", "b"), cacheable);
            case FIELD_EXISTS -> {
                // a visible field in every document rewrites to match-all, which is cacheable even for a metadata field. Otherwise, the
                // query isn't rewritten and reports its field, so it is cacheable only for a granted non-metadata field
                Query rewritten = searcher.rewrite(new FieldExistsQuery(field));
                boolean matchAll = fieldAllowed && denseFields.contains(field);
                assertEquals(rewritten.toString(), matchAll, rewritten instanceof MatchAllDocsQuery);
                yield new RandomQuery(rewritten, matchAll || cacheable);
            }
            case PREFIX -> new RandomQuery(new PrefixQuery(new Term(field, "a")), cacheable);
            case WILDCARD -> new RandomQuery(new WildcardQuery(new Term(field, "a*b")), cacheable);
            case REGEXP -> new RandomQuery(new RegexpQuery(new Term(field, "a.*b")), cacheable);
            case TERM_IN_SET -> new RandomQuery(new TermInSetQuery(field, List.of(new BytesRef("a"), new BytesRef("b"))), cacheable);
            case MATCH_ALL -> new RandomQuery(MatchAllDocsQuery.INSTANCE, true);
            case MATCH_NONE -> new RandomQuery(MatchNoDocsQuery.INSTANCE, true);
            case MIN_DOC -> new RandomQuery(new MinDocQuery(randomNonNegativeInt()), false); // MinDocQuery doesn't report a field
            case TO_PARENT_BLOCK_JOIN -> new RandomQuery(
                // Lucene's block join queries don't visit the query they wrap
                new ToParentBlockJoinQuery(
                    randomInnerQuery(searcher, allowedFields, disallowedFields, denseFields).query(),
                    parents,
                    org.apache.lucene.search.join.ScoreMode.None
                ),
                false
            );
            case TO_CHILD_BLOCK_JOIN -> new RandomQuery(
                new ToChildBlockJoinQuery(randomInnerQuery(searcher, allowedFields, disallowedFields, denseFields).query(), parents),
                false
            );
            case ES_TO_PARENT_BLOCK_JOIN -> new RandomQuery(
                // ESToParentBlockJoinQuery visits its child, but createWeight returns the Lucene query's weight, which doesn't
                new ESToParentBlockJoinQuery(
                    randomInnerQuery(searcher, allowedFields, disallowedFields, denseFields).query(),
                    parents,
                    org.apache.lucene.search.join.ScoreMode.None,
                    "nested"
                ),
                false
            );
            case DENSE_VECTOR -> new RandomQuery(new DenseVectorQuery.Floats(new float[] { 1f }, field, null), cacheable);
            case VECTOR_SIMILARITY -> new RandomQuery(
                new VectorSimilarityQuery(new DenseVectorQuery.Floats(new float[] { 1f }, field, null), 0.5f, 1f),
                cacheable
            );
            case FILTERED_DENSE_VECTOR -> new RandomQuery(
                new DenseVectorQuery.Floats(
                    new float[] { 1f },
                    field,
                    new TermQuery(new Term(randomFrom(randomBoolean() ? allowedFields : disallowedFields), "x"))
                ),
                false
            );
            case COVERING -> {
                List<Query> queries = new ArrayList<>();
                boolean innerCacheable = true;
                int numQueries = between(1, 3);
                for (int i = 0; i < numQueries; i++) {
                    RandomQuery inner = randomInnerQuery(searcher, allowedFields, disallowedFields, denseFields);
                    queries.add(inner.query());
                    innerCacheable &= inner.cacheable();
                }

                boolean constant = randomBoolean();
                LongValuesSource minimumNumberMatch = constant
                    ? LongValuesSource.constant(between(1, numQueries))
                    : LongValuesSource.fromLongField(randomFrom(randomBoolean() ? allowedFields : disallowedFields));

                // A constant minimum is rewritten to a boolean query, so only the inner clauses matter. That rewrite drops match-none
                // SHOULD clauses, and if fewer than the minimum remain it collapses to a MatchNoDocsQuery, which is always cacheable.
                // CoveringQuery doesn't report the field its minimum match count comes from, so field sources are never
                // cacheable, even when the field is granted.
                Query rewritten = searcher.rewrite(new CoveringQuery(queries, minimumNumberMatch));
                yield new RandomQuery(rewritten, constant && (rewritten instanceof MatchNoDocsQuery || innerCacheable));
            }
        };
    }

    private static RandomQuery randomInnerQuery(
        IndexSearcher searcher,
        Set<String> allowedFields,
        Set<String> disallowedFields,
        Set<String> denseFields
    ) throws IOException {
        return randomQuery(
            searcher,
            allowedFields,
            disallowedFields,
            denseFields,
            randomValueOtherThanMany(QueryType::wraps, () -> randomFrom(QueryType.values()))
        );
    }

    /** Wraps the query in a randomly chosen compound query, adding only clauses that are always safe to cache. */
    private static Query wrapInCompoundQuery(Query query) {
        return switch (randomIntBetween(0, 6)) {
            case 0 -> new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST).build();
            case 1 -> new BooleanQuery.Builder().add(query, BooleanClause.Occur.SHOULD).build();
            case 2 -> new BooleanQuery.Builder().add(query, BooleanClause.Occur.FILTER).build();
            case 3 -> new BooleanQuery.Builder().add(MatchAllDocsQuery.INSTANCE, BooleanClause.Occur.MUST)
                .add(query, BooleanClause.Occur.MUST_NOT)
                .build();
            case 4 -> new DisjunctionMaxQuery(List.of(query), randomFloat());
            case 5 -> new ConstantScoreQuery(query);
            case 6 -> new BoostQuery(query, randomFloat());
            default -> throw new AssertionError("unexpected wrapper");
        };
    }

    private enum QueryType {
        TERM(false),
        PHRASE(false),
        FIELD_EXISTS(false),
        PREFIX(false),
        WILDCARD(false),
        REGEXP(false),
        TERM_IN_SET(false),
        MATCH_ALL(false),
        MATCH_NONE(false),
        MIN_DOC(false),
        TO_PARENT_BLOCK_JOIN(true),
        TO_CHILD_BLOCK_JOIN(true),
        ES_TO_PARENT_BLOCK_JOIN(true),
        DENSE_VECTOR(false),
        VECTOR_SIMILARITY(false),
        FILTERED_DENSE_VECTOR(false),
        COVERING(true);

        private final boolean wraps;

        QueryType(boolean wraps) {
            this.wraps = wraps;
        }

        boolean wraps() {
            return wraps;
        }
    }

    private record RandomQuery(Query query, boolean cacheable) {
        RandomQuery rewrite(IndexSearcher searcher) throws IOException {
            return new RandomQuery(searcher.rewrite(query), cacheable);
        }
    }
}
