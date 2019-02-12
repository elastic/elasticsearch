/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.gen.pipeline;

import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe.AttributeResolver;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.List;

import static java.util.Collections.emptyList;

public class BinaryPipesTests extends ESTestCase {
    public void testSupportedByAggsOnlyQuery() {
        Pipe supported = new DummyPipe(true);
        Pipe unsupported = new DummyPipe(false);

        assertFalse(new DummyBinaryPipe(unsupported, unsupported).supportedByAggsOnlyQuery());
        assertTrue(new DummyBinaryPipe(unsupported, supported).supportedByAggsOnlyQuery());
        assertTrue(new DummyBinaryPipe(supported, unsupported).supportedByAggsOnlyQuery());
        assertTrue(new DummyBinaryPipe(supported, supported).supportedByAggsOnlyQuery());
    }

    public void testResolveAttributes() {
        Pipe needsNothing = new DummyPipe(randomBoolean());
        Pipe resolvesTo = new DummyPipe(randomBoolean());
        Pipe needsResolution = new DummyPipe(randomBoolean()) {
            @Override
            public Pipe resolveAttributes(AttributeResolver resolver) {
                return resolvesTo;
            }
        };
        AttributeResolver resolver = a -> {
            fail("not exepected");
            return null;
        };

        Pipe d = new DummyBinaryPipe(needsNothing, needsNothing);
        assertSame(d, d.resolveAttributes(resolver));

        d = new DummyBinaryPipe(needsNothing, needsResolution);
        Pipe expected = new DummyBinaryPipe(needsNothing, resolvesTo);
        assertEquals(expected, d.resolveAttributes(resolver));

        d = new DummyBinaryPipe(needsResolution, needsNothing);
        expected = new DummyBinaryPipe(resolvesTo, needsNothing);
        assertEquals(expected, d.resolveAttributes(resolver));
    }

    public void testCollectFields() {
        DummyPipe wantsScore = new DummyPipe(randomBoolean()) {
            @Override
            public void collectFields(SqlSourceBuilder sourceBuilder) {
                sourceBuilder.trackScores();
            }
        };
        DummyPipe wantsNothing = new DummyPipe(randomBoolean());
        assertFalse(tracksScores(new DummyBinaryPipe(wantsNothing, wantsNothing)));
        assertTrue(tracksScores(new DummyBinaryPipe(wantsScore, wantsNothing)));
        assertTrue(tracksScores(new DummyBinaryPipe(wantsNothing, wantsScore)));
    }

    /**
     * Returns {@code true} if the processor defintion builds a query that
     * tracks scores, {@code false} otherwise. Used for testing
     * {@link Pipe#collectFields(SqlSourceBuilder)}.
     */
    static boolean tracksScores(Pipe d) {
        SqlSourceBuilder b = new SqlSourceBuilder();
        d.collectFields(b);
        SearchSourceBuilder source = new SearchSourceBuilder();
        b.build(source);
        return source.trackScores();
    }

    public static BinaryPipe randomBinaryPipe() {
        return new DummyBinaryPipe(randomUnaryPipe(), randomUnaryPipe());
    }

    public static Pipe randomUnaryPipe() {
        return new ConstantInput(Source.EMPTY, Literal.of(Source.EMPTY, randomAlphaOfLength(16)), randomAlphaOfLength(16));
    }

    public static final class DummyBinaryPipe extends BinaryPipe {
        public DummyBinaryPipe(Pipe left, Pipe right) {
            this(Source.EMPTY, left, right);
        }

        public DummyBinaryPipe(Source source, Pipe left, Pipe right) {
            super(source, null, left, right);
        }

        @Override
        protected NodeInfo<BinaryPipe> info() {
            return NodeInfo.create(this, DummyBinaryPipe::new, left(), right());
        }

        @Override
        public Processor asProcessor() {
            return null;
        }

        @Override
        protected BinaryPipe replaceChildren(Pipe left, Pipe right) {
            return new DummyBinaryPipe(source(), left, right);
        }
    }

    public static class DummyPipe extends Pipe {
        private final boolean supportedByAggsOnlyQuery;

        public DummyPipe(boolean supportedByAggsOnlyQuery) {
            this(Source.EMPTY, supportedByAggsOnlyQuery);
        }

        public DummyPipe(Source source, boolean supportedByAggsOnlyQuery) {
            super(source, null, emptyList());
            this.supportedByAggsOnlyQuery = supportedByAggsOnlyQuery;
        }

        @Override
        protected NodeInfo<DummyPipe> info() {
            return NodeInfo.create(this, DummyPipe::new, supportedByAggsOnlyQuery);
        }

        @Override
        public Pipe replaceChildren(List<Pipe> newChildren) {
            throw new UnsupportedOperationException("this type of node doesn't have any children to replace");
        }

        @Override
        public boolean supportedByAggsOnlyQuery() {
            return supportedByAggsOnlyQuery;
        }

        @Override
        public boolean resolved() {
            return true;
        }

        @Override
        public Processor asProcessor() {
            return null;
        }

        @Override
        public Pipe resolveAttributes(AttributeResolver resolver) {
            return this;
        }

        @Override
        public void collectFields(SqlSourceBuilder sourceBuilder) {
        }
    }
}