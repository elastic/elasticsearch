/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition.AttributeResolver;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;

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

    public static final class DummyBinaryProcessorDefinition extends BinaryProcessorDefinition {
        public DummyBinaryProcessorDefinition(ProcessorDefinition left, ProcessorDefinition right) {
            super(null, left, right);
        }

        @Override
        public Processor asProcessor() {
            return null;
        }

        @Override
        protected BinaryProcessorDefinition replaceChildren(ProcessorDefinition left, ProcessorDefinition right) {
            return new DummyBinaryProcessorDefinition(left, right);
        }
    }

    public static class DummyProcessorDefinition extends ProcessorDefinition {
        private final boolean supportedByAggsOnlyQuery;

        public DummyProcessorDefinition(boolean supportedByAggsOnlyQuery) {
            super(null, emptyList());
            this.supportedByAggsOnlyQuery = supportedByAggsOnlyQuery;
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
    }
}
