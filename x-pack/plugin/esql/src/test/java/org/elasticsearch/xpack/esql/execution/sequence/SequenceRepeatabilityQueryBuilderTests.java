/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.List;

import static org.hamcrest.Matchers.containsString;

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
