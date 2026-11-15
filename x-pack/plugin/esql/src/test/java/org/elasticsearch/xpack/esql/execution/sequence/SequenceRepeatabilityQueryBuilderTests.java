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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/** The guard must survive empty-query rewrites and transport without becoming publicly constructible. */
public class SequenceRepeatabilityQueryBuilderTests extends AbstractWireSerializingTestCase<SequenceRepeatabilityQueryBuilder> {
    @Override
    protected SequenceRepeatabilityQueryBuilder createTestInstance() {
        return new SequenceRepeatabilityQueryBuilder(
            new TermQueryBuilder("phase", randomAlphaOfLength(5)),
            List.of("key", "payload"),
            List.of("sequence-a", "sequence-b")
        );
    }

    @Override
    protected SequenceRepeatabilityQueryBuilder mutateInstance(SequenceRepeatabilityQueryBuilder instance) {
        return new SequenceRepeatabilityQueryBuilder(new TermQueryBuilder("phase", "changed"), List.of("other"), List.of("sequence-a"));
    }

    @Override
    protected Writeable.Reader<SequenceRepeatabilityQueryBuilder> instanceReader() {
        return SequenceRepeatabilityQueryBuilder::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(
                SequenceRepeatabilityQueryBuilder.ENTRY,
                new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new)
            )
        );
    }

    public void testMatchNoneDoesNotEraseValidation() throws Exception {
        var guard = new SequenceRepeatabilityQueryBuilder(new MatchNoneQueryBuilder(), List.of("changing"), List.of("sequence-a"));
        var context = new QueryRewriteContext(XContentParserConfiguration.EMPTY, null, () -> 0L);
        assertSame(guard, guard.rewrite(context));
    }

    /** Identity hydration must not touch fields that selection required but the requested output does not demand. */
    public void testIdentityReadValidatesOnlyDemandedFields() throws Exception {
        assertValidatedFields(List.of("payload"));
    }

    public void testCandidateReadExplicitlyValidatesTimestamp() throws Exception {
        assertValidatedFields(List.of("@timestamp", "key"));
    }

    private void assertValidatedFields(List<String> fields) throws Exception {
        // This boundary only delegates field admission; real mapped-value behavior is covered by the integration tests.
        SearchExecutionContext context = mock(SearchExecutionContext.class);
        when(context.index()).thenReturn(new Index("sequence-a", "sequence-a-uuid"));
        var guard = new SequenceRepeatabilityQueryBuilder(new MatchNoneQueryBuilder(), fields, List.of("sequence-a"));

        assertThat(guard.doToQuery(context, new MaxClauseCountQueryVisitor(Integer.MAX_VALUE)), instanceOf(MatchNoDocsQuery.class));
        verify(context).index();
        for (String field : fields) {
            verify(context).getFieldType(field);
        }
        verify(context, times(fields.size())).getFieldType(anyString());
    }

    /** Foreign PIT slots must remain empty before any source-specific query or mapping interpretation. */
    public void testForeignPitSlotDoesNotRewriteSourceQuery() throws Exception {
        // A real execution context requires mapping/search services that this boundary must never consult.
        // Expose only its physical index, and make any attempt to rewrite the source query fail explicitly.
        SearchExecutionContext context = mock(SearchExecutionContext.class);
        when(context.index()).thenReturn(new Index("outside-source", "outside-source-uuid"));
        QueryBuilder delegate = mock(QueryBuilder.class);
        when(delegate.rewrite(context)).thenThrow(new AssertionError("foreign PIT slot must not rewrite the source query"));
        var guard = new SequenceRepeatabilityQueryBuilder(delegate, List.of("changing"), List.of("sequence-a"));
        var visitor = new MaxClauseCountQueryVisitor(Integer.MAX_VALUE);

        assertThat(guard.doToQuery(context, visitor), instanceOf(MatchNoDocsQuery.class));
        assertEquals(1, visitor.getNumClauses());
        verify(context).index();
        verifyNoMoreInteractions(context);
        verifyNoInteractions(delegate);
    }

    public void testRejectUnsupportedPeer() throws Exception {
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(
                TransportVersionUtils.getPreviousVersion(SequenceRepeatabilityQueryBuilder.ESQL_SEQUENCE_REPEATABLE_INPUT)
            );
            assertThat(
                expectThrows(IllegalArgumentException.class, () -> createTestInstance().writeTo(out)).getMessage(),
                containsString("SEQUENCE repeatable source evaluation is not supported")
            );
        }
    }

    public void testPublicSearchCannotConstructGuard() throws Exception {
        try (var parser = createParser(JsonXContent.jsonXContent, "{}")) {
            parser.nextToken();
            assertThat(
                expectThrows(XContentParseException.class, () -> SequenceRepeatabilityQueryBuilder.fromXContent(parser)).getMessage(),
                containsString("reserved for internal ES|QL searches")
            );
        }
    }
}
