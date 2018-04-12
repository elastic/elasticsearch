/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition.AttributeResolver;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.List;

import static java.util.Collections.emptyList;

public class BinaryProcessorDefinitionTests extends ESTestCase {
    public void testSupportedByAggsOnlyQuery() {
        ProcessorDefinition supported = new DummyProcessorDefinition(true);
        ProcessorDefinition unsupported = new DummyProcessorDefinition(false);

        assertFalse(new DummyBinaryProcessorDefinition(unsupported, unsupported).supportedByAggsOnlyQuery());
        assertFalse(new DummyBinaryProcessorDefinition(unsupported, supported).supportedByAggsOnlyQuery());
        assertFalse(new DummyBinaryProcessorDefinition(supported, unsupported).supportedByAggsOnlyQuery());
        assertTrue(new DummyBinaryProcessorDefinition(supported, supported).supportedByAggsOnlyQuery());
    }

    public void testResolveAttributes() {
        ProcessorDefinition needsNothing = new DummyProcessorDefinition(randomBoolean());
        ProcessorDefinition resolvesTo = new DummyProcessorDefinition(randomBoolean());
        ProcessorDefinition needsResolution = new DummyProcessorDefinition(randomBoolean()) {
            @Override
            public ProcessorDefinition resolveAttributes(AttributeResolver resolver) {
                return resolvesTo;
            }
        };
        AttributeResolver resolver = a -> {
            fail("not exepected");
            return null;
        };

        ProcessorDefinition d = new DummyBinaryProcessorDefinition(needsNothing, needsNothing);
        assertSame(d, d.resolveAttributes(resolver));

        d = new DummyBinaryProcessorDefinition(needsNothing, needsResolution);
        ProcessorDefinition expected = new DummyBinaryProcessorDefinition(needsNothing, resolvesTo);
        assertEquals(expected, d.resolveAttributes(resolver));

        d = new DummyBinaryProcessorDefinition(needsResolution, needsNothing);
        expected = new DummyBinaryProcessorDefinition(resolvesTo, needsNothing);
        assertEquals(expected, d.resolveAttributes(resolver));
    }

    public void testCollectFields() {
        DummyProcessorDefinition wantsScore = new DummyProcessorDefinition(randomBoolean()) {
            @Override
            public void collectFields(SqlSourceBuilder sourceBuilder) {
                sourceBuilder.trackScores();
            }
        };
        DummyProcessorDefinition wantsNothing = new DummyProcessorDefinition(randomBoolean());
        assertFalse(tracksScores(new DummyBinaryProcessorDefinition(wantsNothing, wantsNothing)));
        assertTrue(tracksScores(new DummyBinaryProcessorDefinition(wantsScore, wantsNothing)));
        assertTrue(tracksScores(new DummyBinaryProcessorDefinition(wantsNothing, wantsScore)));
    }

    /**
     * Returns {@code true} if the processor defintion builds a query that
     * tracks scores, {@code false} otherwise. Used for testing
     * {@link ProcessorDefinition#collectFields(SqlSourceBuilder)}.
     */
    static boolean tracksScores(ProcessorDefinition d) {
        SqlSourceBuilder b = new SqlSourceBuilder();
        d.collectFields(b);
        SearchSourceBuilder source = new SearchSourceBuilder();
        b.build(source);
        return source.trackScores();
    }

    public static final class DummyBinaryProcessorDefinition extends BinaryProcessorDefinition {
        public DummyBinaryProcessorDefinition(ProcessorDefinition left, ProcessorDefinition right) {
            this(Location.EMPTY, left, right);
        }

        public DummyBinaryProcessorDefinition(Location location, ProcessorDefinition left, ProcessorDefinition right) {
            super(location, null, left, right);
        }

        @Override
        protected NodeInfo<BinaryProcessorDefinition> info() {
            return NodeInfo.create(this, DummyBinaryProcessorDefinition::new, left(), right());
        }

        @Override
        public Processor asProcessor() {
            return null;
        }

        @Override
        protected BinaryProcessorDefinition replaceChildren(ProcessorDefinition left, ProcessorDefinition right) {
            return new DummyBinaryProcessorDefinition(location(), left, right);
        }
    }

    public static class DummyProcessorDefinition extends ProcessorDefinition {
        private final boolean supportedByAggsOnlyQuery;

        public DummyProcessorDefinition(boolean supportedByAggsOnlyQuery) {
            this(Location.EMPTY, supportedByAggsOnlyQuery);
        }

        public DummyProcessorDefinition(Location location, boolean supportedByAggsOnlyQuery) {
            super(location, null, emptyList());
            this.supportedByAggsOnlyQuery = supportedByAggsOnlyQuery;
        }

        @Override
        protected NodeInfo<DummyProcessorDefinition> info() {
            return NodeInfo.create(this, DummyProcessorDefinition::new, supportedByAggsOnlyQuery);
        }

        @Override
        public ProcessorDefinition replaceChildren(List<ProcessorDefinition> newChildren) {
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
        public ProcessorDefinition resolveAttributes(AttributeResolver resolver) {
            return this;
        }

        @Override
        public void collectFields(SqlSourceBuilder sourceBuilder) {
        }
    }
}