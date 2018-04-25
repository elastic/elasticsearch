/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.BinaryProcessorDefinitionTests.DummyProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition.AttributeResolver;
import org.elasticsearch.xpack.sql.tree.Location;

import static org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.BinaryProcessorDefinitionTests.tracksScores;

public class UnaryProcessorDefinitionTests extends ESTestCase {
    public void testSupportedByAggsOnlyQuery() {
        ProcessorDefinition supported = new DummyProcessorDefinition(true);
        ProcessorDefinition unsupported = new DummyProcessorDefinition(false);

        assertFalse(newUnaryProcessor(unsupported).supportedByAggsOnlyQuery());
        assertTrue(newUnaryProcessor(supported).supportedByAggsOnlyQuery());
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

        ProcessorDefinition d = newUnaryProcessor(needsNothing);
        assertSame(d, d.resolveAttributes(resolver));

        d = newUnaryProcessor(needsResolution);
        ProcessorDefinition expected = newUnaryProcessor(resolvesTo);
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
        assertFalse(tracksScores(newUnaryProcessor(wantsNothing)));
        assertTrue(tracksScores(newUnaryProcessor(wantsScore)));
    }

    private ProcessorDefinition newUnaryProcessor(ProcessorDefinition child) {
        return new UnaryProcessorDefinition(Location.EMPTY, null, child, null);
    }
}
