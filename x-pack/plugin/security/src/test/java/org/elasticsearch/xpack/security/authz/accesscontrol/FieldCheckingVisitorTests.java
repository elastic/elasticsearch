/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.accesscontrol;

import org.apache.lucene.index.Term;
import org.apache.lucene.sandbox.search.CoveringQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.sameInstance;

/**
 * Calls the {@link FieldCheckingVisitor} methods directly, so each method's field check is tested even though real queries always
 * call {@code acceptField} before {@code consumeTerms} or {@code consumeTermsMatching}.
 */
public class FieldCheckingVisitorTests extends ESTestCase {
    private static final String CACHEABLE_FIELD = "foo";

    // metadata fields are granted by default, so _allowed is granted and _disallowed must be excluded explicitly
    private static final IndicesAccessControl.IndexAccessControl PERMISSIONS = new IndicesAccessControl.IndexAccessControl(
        new FieldPermissions(new FieldPermissionsDefinition(new String[] { CACHEABLE_FIELD }, new String[] { "_disallowed" })),
        DocumentPermissions.allowAll()
    );
    private static final Supplier<ByteRunAutomaton> UNUSED_AUTOMATON = () -> { throw new AssertionError("automaton not evaluated"); };
    private static final Query UNUSED_QUERY = new UnusedQuery();

    public void testAcceptField() {
        FieldCheckingVisitor visitor = new FieldCheckingVisitor(PERMISSIONS);

        assertTrue(visitor.acceptField(CACHEABLE_FIELD));
        assertTrue(visitor.isCacheable());

        assertFalse(visitor.acceptField(randomUncacheableField()));
        assertFalse(visitor.isCacheable());

        assertFalse(visitor.acceptField(CACHEABLE_FIELD));
        assertFalse(visitor.isCacheable());
    }

    public void testConsumeTerms() {
        FieldCheckingVisitor visitor = new FieldCheckingVisitor(PERMISSIONS);
        visitor.consumeTerms(UNUSED_QUERY);
        assertTrue(visitor.isCacheable());

        int numTerms = between(1, 5);
        Term[] terms = new Term[numTerms];
        for (int i = 0; i < numTerms; i++) {
            terms[i] = new Term(CACHEABLE_FIELD, "x" + i);
        }
        visitor.consumeTerms(UNUSED_QUERY, terms);
        assertTrue(visitor.isCacheable());

        terms[randomIntBetween(0, numTerms - 1)] = new Term(randomUncacheableField(), "x");
        visitor.consumeTerms(UNUSED_QUERY, terms);
        assertFalse(visitor.isCacheable());
    }

    public void testConsumeTermsMatching() {
        FieldCheckingVisitor visitor = new FieldCheckingVisitor(PERMISSIONS);

        visitor.consumeTermsMatching(UNUSED_QUERY, CACHEABLE_FIELD, UNUSED_AUTOMATON);
        assertTrue(visitor.isCacheable());

        visitor.consumeTermsMatching(UNUSED_QUERY, randomUncacheableField(), UNUSED_AUTOMATON);
        assertFalse(visitor.isCacheable());
    }

    public void testVisitLeaf() {
        FieldCheckingVisitor visitor = new FieldCheckingVisitor(PERMISSIONS);

        visitor.visitLeaf(MatchAllDocsQuery.INSTANCE);
        visitor.visitLeaf(MatchNoDocsQuery.INSTANCE);
        assertTrue(visitor.isCacheable());

        // visitLeaf checks both that the query reports a field and that the field is cacheable
        visitor.visitLeaf(new FieldQuery(CACHEABLE_FIELD));
        assertTrue(visitor.isCacheable());

        Consumer<Runnable> testUncacheable = r -> {
            visitor.reset();
            r.run();
            assertFalse(visitor.isCacheable());
        };
        testUncacheable.accept(() -> visitor.visitLeaf(new NoFieldQuery()));
        testUncacheable.accept(() -> visitor.visitLeaf(new FieldQuery(randomUncacheableField())));
    }

    public void testGetSubVisitor() {
        FieldCheckingVisitor visitor = new FieldCheckingVisitor(PERMISSIONS);
        assertThat(visitor.getSubVisitor(randomFrom(BooleanClause.Occur.values()), UNUSED_QUERY), sameInstance(visitor));
        assertTrue(visitor.isCacheable());

        CoveringQuery covering = new CoveringQuery(List.of(new TermQuery(new Term(CACHEABLE_FIELD, "x"))), LongValuesSource.constant(1));
        assertThat(visitor.getSubVisitor(randomFrom(BooleanClause.Occur.values()), covering), sameInstance(QueryVisitor.EMPTY_VISITOR));
        assertFalse(visitor.isCacheable());

        assertThat(visitor.getSubVisitor(randomFrom(BooleanClause.Occur.values()), UNUSED_QUERY), sameInstance(QueryVisitor.EMPTY_VISITOR));
        assertFalse(visitor.isCacheable());
    }

    public void testUncacheableIsSticky() {
        FieldCheckingVisitor visitor = new FieldCheckingVisitor(PERMISSIONS);
        List<Runnable> uncacheableCalls = List.of(
            () -> visitor.acceptField(randomUncacheableField()),
            () -> visitor.consumeTerms(UNUSED_QUERY, new Term(randomUncacheableField(), "x")),
            () -> visitor.consumeTermsMatching(UNUSED_QUERY, randomUncacheableField(), UNUSED_AUTOMATON),
            () -> visitor.visitLeaf(new NoFieldQuery())
        );
        randomFrom(uncacheableCalls).run();
        assertFalse(visitor.isCacheable());

        List<Runnable> cacheableCalls = List.of(
            () -> assertFalse(visitor.acceptField(CACHEABLE_FIELD)),
            () -> visitor.consumeTerms(UNUSED_QUERY, new Term(CACHEABLE_FIELD, "x")),
            () -> visitor.consumeTermsMatching(UNUSED_QUERY, CACHEABLE_FIELD, UNUSED_AUTOMATON),
            () -> visitor.visitLeaf(UNUSED_QUERY)  // visitLeaf should not visit the query once cacheable is false
        );
        for (Runnable call : cacheableCalls) {
            call.run();
            assertFalse(visitor.isCacheable());
        }
    }

    public void testReset() {
        FieldCheckingVisitor visitor = new FieldCheckingVisitor(PERMISSIONS);
        assertFalse(visitor.acceptField(randomUncacheableField()));
        assertFalse(visitor.isCacheable());

        visitor.reset();
        assertTrue(visitor.isCacheable());
    }

    /** Fields that are never cacheable: one not granted, and metadata fields whether granted or not. */
    private static String randomUncacheableField() {
        return randomFrom("secret", "_allowed", "_disallowed");
    }

    /**
     * Stands in for a leaf query that matches documents without reporting a field to the visitor, so its field permissions can't be
     * checked.
     */
    private static class NoFieldQuery extends Query {
        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }

        @Override
        public String toString(String field) {
            return "NoFieldQuery";
        }

        @Override
        public boolean equals(Object obj) {
            return sameClassAs(obj);
        }

        @Override
        public int hashCode() {
            return classHash();
        }
    }

    /**
     * Stands in for a leaf query that reports its field to the visitor before visiting itself as a leaf.
     */
    private static class FieldQuery extends Query {
        private final String field;

        FieldQuery(String field) {
            this.field = field;
        }

        @Override
        public void visit(QueryVisitor visitor) {
            if (visitor.acceptField(field)) {
                visitor.visitLeaf(this);
            }
        }

        @Override
        public String toString(String f) {
            return "FieldQuery[" + field + "]";
        }

        @Override
        public boolean equals(Object obj) {
            return sameClassAs(obj) && field.equals(((FieldQuery) obj).field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(classHash(), field);
        }
    }

    /**
     * Stands in for the query argument of visitor methods that should only look at their other arguments, so any use of it fails the
     * test.
     */
    private static class UnusedQuery extends Query {
        @Override
        public void visit(QueryVisitor visitor) {
            throw new AssertionError("query not used");
        }

        @Override
        public Query rewrite(IndexSearcher indexSearcher) {
            throw new AssertionError("query not used");
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
            throw new AssertionError("query not used");
        }

        @Override
        public String toString(String field) {
            throw new AssertionError("query not used");
        }

        @Override
        public boolean equals(Object obj) {
            throw new AssertionError("query not used");
        }

        @Override
        public int hashCode() {
            throw new AssertionError("query not used");
        }
    }
}
