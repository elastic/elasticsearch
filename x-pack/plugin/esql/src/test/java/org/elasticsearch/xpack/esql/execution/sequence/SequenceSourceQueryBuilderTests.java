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
                new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new)
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

    public void testRejectUnsupportedPeer() throws Exception {
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersionUtils.getPreviousVersion(SequenceSourceQueryBuilder.ESQL_SEQUENCE_R6_SOURCE_SCOPE));
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
