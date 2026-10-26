/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.accesscontrol;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
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
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.lucene.queries.MinDocQuery;
import org.elasticsearch.search.vectors.DenseVectorQuery;
import org.elasticsearch.search.vectors.FilteredDenseVectorQuery;
import org.elasticsearch.search.vectors.VectorSimilarityQuery;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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
     * Builds a boolean query from random clauses and checks that it is cacheable only if every clause is.
     */
    public void testCachingIsSafeRandomClauses() throws IOException {
        final Set<String> allowedFields = Set.of("foo", "bar");
        final Set<String> disallowedFields = Set.of(
            randomValueOtherThanMany(allowedFields::contains, () -> randomAlphaOfLengthBetween(3, 10)),
            "_" + randomAlphaOfLengthBetween(1, 10)
        );
        final IndicesAccessControl.IndexAccessControl permissions = new IndicesAccessControl.IndexAccessControl(
            new FieldPermissions(fieldPermissionDef(allowedFields.toArray(String[]::new), null)),
            DocumentPermissions.allowAll()
        );

        for (int i = 0; i < 20; i++) {
            int numClauses = between(1, 10);
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            boolean expectedCacheable = true;
            for (int j = 0; j < numClauses; j++) {
                Clause clause = randomClause(allowedFields, disallowedFields);
                builder.add(clause.query(), randomFrom(BooleanClause.Occur.values()));
                expectedCacheable &= clause.cacheable();
            }

            // don't rewrite, it could collapse the clauses
            Weight weight = builder.build().createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
            assertEquals(expectedCacheable, OptOutQueryCache.cachingIsSafe(weight, permissions));
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

    private Clause randomClause(Set<String> allowedFields, Set<String> disallowedFields) throws IOException {
        return randomClause(allowedFields, disallowedFields, randomFrom(QueryType.values()));
    }

    private Clause randomClause(Set<String> allowedFields, Set<String> disallowedFields, QueryType type) throws IOException {
        boolean fieldAllowed = randomBoolean();
        String field = randomFrom(fieldAllowed ? allowedFields : disallowedFields);
        BitSetProducer parents = new QueryBitSetProducer(new TermQuery(new Term("parent", "true")));

        return switch (type) {
            case TERM -> new Clause(new TermQuery(new Term(field, "x")), fieldAllowed);
            case PHRASE -> new Clause(new PhraseQuery(field, "a", "b"), fieldAllowed);
            case FIELD_EXISTS -> new Clause(new FieldExistsQuery(field), fieldAllowed);
            // multi-term queries must be rewritten before creating a weight
            case PREFIX -> new Clause(searcher.rewrite(new PrefixQuery(new Term(field, "a"))), fieldAllowed);
            case WILDCARD -> new Clause(searcher.rewrite(new WildcardQuery(new Term(field, "a*b"))), fieldAllowed);
            case REGEXP -> new Clause(searcher.rewrite(new RegexpQuery(new Term(field, "a.*b"))), fieldAllowed);
            case TERM_IN_SET -> new Clause(
                searcher.rewrite(new TermInSetQuery(field, List.of(new BytesRef("a"), new BytesRef("b")))),
                fieldAllowed
            );
            case TERMS_MATCHING_ONLY -> new Clause(new ConsumeTermsMatchingQuery(field), fieldAllowed);
            case MATCH_ALL -> new Clause(new MatchAllDocsQuery(), true);
            case MATCH_NONE -> new Clause(new MatchNoDocsQuery(), true);
            // MinDocQuery doesn't report a field, and must be rewritten against the reader before creating a weight
            case MIN_DOC -> new Clause(searcher.rewrite(new MinDocQuery(randomNonNegativeInt())), false);
            // Lucene's block join queries don't visit the query they wrap
            case TO_PARENT_BLOCK_JOIN -> new Clause(
                new ToParentBlockJoinQuery(
                    randomInnerClause(allowedFields, disallowedFields).query(),
                    parents,
                    org.apache.lucene.search.join.ScoreMode.None
                ),
                false
            );
            case TO_CHILD_BLOCK_JOIN -> new Clause(
                new ToChildBlockJoinQuery(randomInnerClause(allowedFields, disallowedFields).query(), parents),
                false
            );
            case ES_TO_PARENT_BLOCK_JOIN -> {
                Clause inner = randomInnerClause(allowedFields, disallowedFields);
                yield new Clause(
                    new ESToParentBlockJoinQuery(inner.query(), parents, org.apache.lucene.search.join.ScoreMode.None, "nested"),
                    inner.cacheable()
                );
            }
            // DenseVectorQuery and VectorSimilarityQuery don't report their field yet, so they are never cacheable
            case DENSE_VECTOR -> new Clause(DenseVectorQuery.Floats.codecScored(new float[] { 1f }, field), false);
            case VECTOR_SIMILARITY -> new Clause(
                new VectorSimilarityQuery(DenseVectorQuery.Floats.codecScored(new float[] { 1f }, field), 0.5f, 1f),
                false
            );
            case FILTERED_DENSE_VECTOR -> new Clause(
                new FilteredDenseVectorQuery(
                    DenseVectorQuery.Floats.codecScored(new float[] { 1f }, field),
                    new TermQuery(new Term(randomFrom(randomBoolean() ? allowedFields : disallowedFields), "x"))
                ),
                false
            );
        };
    }

    private Clause randomInnerClause(Set<String> allowedFields, Set<String> disallowedFields) throws IOException {
        return randomClause(
            allowedFields,
            disallowedFields,
            randomValueOtherThanMany(QueryType::wraps, () -> randomFrom(QueryType.values()))
        );
    }

    private enum QueryType {
        TERM(false),
        PHRASE(false),
        FIELD_EXISTS(false),
        PREFIX(false),
        WILDCARD(false),
        REGEXP(false),
        TERM_IN_SET(false),
        TERMS_MATCHING_ONLY(false),
        MATCH_ALL(false),
        MATCH_NONE(false),
        MIN_DOC(false),
        TO_PARENT_BLOCK_JOIN(true),
        TO_CHILD_BLOCK_JOIN(true),
        ES_TO_PARENT_BLOCK_JOIN(true),
        DENSE_VECTOR(false),
        VECTOR_SIMILARITY(false),
        FILTERED_DENSE_VECTOR(false);

        private final boolean wraps;

        QueryType(boolean wraps) {
            this.wraps = wraps;
        }

        boolean wraps() {
            return wraps;
        }
    }

    private static FieldPermissionsDefinition fieldPermissionDef(String[] granted, String[] denied) {
        return new FieldPermissionsDefinition(granted, denied);
    }

    private record Clause(Query query, boolean cacheable) {}

    /**
     * Real multi-term queries call {@code acceptField} before {@code consumeTermsMatching}, so a hidden field is rejected before
     * {@code consumeTermsMatching} runs. This query reports its field only through {@code consumeTermsMatching}, so that check is tested.
     */
    private static class ConsumeTermsMatchingQuery extends Query {
        private final String field;

        ConsumeTermsMatchingQuery(String field) {
            this.field = field;
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.consumeTermsMatching(this, field, () -> new ByteRunAutomaton(Automata.makeAnyString()));
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
            return new ConstantScoreWeight(this, boost) {
                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) {
                    return null;
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return true;
                }
            };
        }

        @Override
        public String toString(String f) {
            return "ConsumeTermsMatchingQuery[" + field + "]";
        }

        @Override
        public boolean equals(Object obj) {
            return sameClassAs(obj) && field.equals(((ConsumeTermsMatchingQuery) obj).field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(classHash(), field);
        }
    }
}
