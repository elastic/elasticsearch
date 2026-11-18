/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.internal.MaxClauseCountQueryVisitor;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/** The guard must survive empty-query rewrites and transport without becoming publicly constructible. */
public class SequenceSourceQueryBuilderTests extends AbstractWireSerializingTestCase<SequenceSourceQueryBuilder> {
    @Override
    protected SequenceSourceQueryBuilder createTestInstance() {
        return new SequenceSourceQueryBuilder(new TermQueryBuilder("phase", randomAlphaOfLength(5)), List.of("sequence-a", "sequence-b"));
    }

    @Override
    protected SequenceSourceQueryBuilder mutateInstance(SequenceSourceQueryBuilder instance) {
        return new SequenceSourceQueryBuilder(new TermQueryBuilder("phase", "changed"), List.of("sequence-a"));
    }

    @Override
    protected Writeable.Reader<SequenceSourceQueryBuilder> instanceReader() {
        return SequenceSourceQueryBuilder::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(
                SequenceSourceQueryBuilder.ENTRY,
                new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, BoolQueryBuilder.NAME, BoolQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchNoneQueryBuilder.NAME, MatchNoneQueryBuilder::new)
            )
        );
    }

    public void testMatchNonePreservesScopeBoundary() throws Exception {
        var guard = new SequenceSourceQueryBuilder(new MatchNoneQueryBuilder(), List.of("sequence-a"));
        var context = new QueryRewriteContext(XContentParserConfiguration.EMPTY, null, () -> 0L);
        assertSame(guard, guard.rewrite(context));
    }

    /** Foreign PIT slots must remain empty before any source-specific query or mapping interpretation. */
    public void testForeignPitSlotDoesNotRewriteSourceQuery() throws Exception {
        // A real execution context requires mapping/search services that this boundary must never consult.
        // Expose only its physical index, and make any attempt to rewrite the source query fail explicitly.
        SearchExecutionContext context = mock(SearchExecutionContext.class);
        when(context.index()).thenReturn(new Index("outside-source", "outside-source-uuid"));
        QueryBuilder delegate = mock(QueryBuilder.class);
        when(delegate.rewrite(context)).thenThrow(new AssertionError("foreign PIT slot must not rewrite the source query"));
        var guard = new SequenceSourceQueryBuilder(delegate, List.of("sequence-a"));
        var visitor = new MaxClauseCountQueryVisitor(Integer.MAX_VALUE);

        assertThat(guard.doToQuery(context, visitor), instanceOf(MatchNoDocsQuery.class));
        assertEquals(1, visitor.getNumClauses());
        verify(context).index();
        verifyNoMoreInteractions(context);
        verifyNoInteractions(delegate);
    }

    /** A known foreign shard must stop before a delegate can rewrite fields or register an external lookup. */
    public void testForeignRewriteDoesNotTouchDelegate() throws Exception {
        SearchExecutionContext context = mock(SearchExecutionContext.class);
        when(context.getFullyQualifiedIndex()).thenReturn(new Index("outside-source", "outside-source-uuid"));
        QueryBuilder delegate = mock(QueryBuilder.class);
        var guard = new SequenceSourceQueryBuilder(delegate, List.of("sequence-a"));
        assertTrue(guard.rewrite(context) instanceof MatchNoneQueryBuilder);
        verify(context).getQueryRewriteInterceptor();
        verify(context).getFullyQualifiedIndex();
        verifyNoMoreInteractions(context);
        verifyNoInteractions(delegate);
    }

    /** Ordinary own-index rewriting preserves the scope boundary and the original query metadata. */
    public void testOwnRewritePreservesScopeAndQueryMetadata() throws Exception {
        SearchExecutionContext context = mock(SearchExecutionContext.class);
        when(context.getFullyQualifiedIndex()).thenReturn(new Index("sequence-a", "sequence-a-uuid"));
        QueryBuilder delegate = mock(QueryBuilder.class);
        when(delegate.rewrite(context)).thenReturn(new TermQueryBuilder("phase", "resolved"));
        var guard = new SequenceSourceQueryBuilder(delegate, List.of("sequence-a")).boost(2).queryName("scoped");
        QueryBuilder rewritten = guard.rewrite(context);
        assertTrue(rewritten instanceof SequenceSourceQueryBuilder);
        assertEquals(2, rewritten.boost(), 0);
        assertEquals("scoped", rewritten.queryName());
        assertThat(rewritten.toString(), containsString("resolved"));
        verify(delegate).rewrite(context);
    }

    /** Existing standard query-builder wire types retain both the opaque predicate and user query metadata. */
    public void testKeyEnvelopeSurvivesExistingWireAndCoordinatorRewrite() throws Exception {
        var base = new BoolQueryBuilder().filter(new MatchNoneQueryBuilder()).queryName("ordinary-base");
        var predicate = new BoolQueryBuilder().minimumShouldMatch(1)
            .should(new BoolQueryBuilder().filter(new TermQueryBuilder("key", "waiting")));
        var original = new SequenceSourceQueryBuilder(SequenceSourceQueryBuilder.withKeyFilter(base, predicate), List.of("sequence-a"))
            .boost(2)
            .queryName("ordinary-scope");
        var copy = copyInstance(original);
        assertEquals(original, copy);
        var context = new QueryRewriteContext(XContentParserConfiguration.EMPTY, null, () -> 0L);
        QueryBuilder rewritten = copy.rewrite(context);
        assertTrue(rewritten instanceof SequenceSourceQueryBuilder);
        assertThat(rewritten.toString(), containsString("waiting"));
        assertThat(rewritten.toString(), containsString("ordinary-base"));
        assertEquals("ordinary-scope", rewritten.queryName());
        assertEquals(2, rewritten.boost(), 0);
        assertEquals(rewritten, copyInstance((SequenceSourceQueryBuilder) rewritten));
    }

    /** A user-named base inside the compiler envelope must remain ordinary input rather than a nested compiler tag. */
    public void testUserBaseNameDoesNotBecomeEnvelopeOnCoordinator() throws Exception {
        var base = new TermQueryBuilder("phase", "A").queryName("__esql_sequence_waiting_keys_v1");
        var guard = new SequenceSourceQueryBuilder(
            SequenceSourceQueryBuilder.withKeyFilter(base, new MatchAllQueryBuilder()),
            List.of("sequence-a")
        );
        var context = new QueryRewriteContext(XContentParserConfiguration.EMPTY, null, () -> 0L);
        assertSame(guard, guard.rewrite(context));
        assertEquals(guard, copyInstance(guard));
    }

    /** A malformed compiler-owned root must fail closed instead of weakening its source query. */
    public void testMalformedKeyEnvelopeIsRefused() {
        var guard = new SequenceSourceQueryBuilder(
            new TermQueryBuilder("key", "value").queryName("__esql_sequence_waiting_keys_v1"),
            List.of("sequence-a")
        );
        var context = new QueryRewriteContext(XContentParserConfiguration.EMPTY, null, () -> 0L);
        assertThat(
            expectThrows(IllegalStateException.class, () -> guard.rewrite(context)).getMessage(),
            containsString("Invalid compiler-owned SEQUENCE key-filter envelope")
        );
    }

    public void testRejectUnsupportedPeer() throws Exception {
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersionUtils.getPreviousVersion(Sequence.ESQL_SEQUENCE));
            assertThat(
                expectThrows(IllegalArgumentException.class, () -> createTestInstance().writeTo(out)).getMessage(),
                containsString("SEQUENCE source isolation is not supported")
            );
        }
    }

    public void testPublicSearchCannotConstructGuard() throws Exception {
        try (var parser = createParser(JsonXContent.jsonXContent, "{}")) {
            parser.nextToken();
            assertThat(
                expectThrows(XContentParseException.class, () -> SequenceSourceQueryBuilder.fromXContent(parser)).getMessage(),
                containsString("reserved for internal ES|QL searches")
            );
        }
    }
}
