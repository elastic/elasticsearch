/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;

import static java.util.Collections.emptyList;

public class BinaryProcessorDefinitionTests extends ESTestCase {
    public void testSupportedByAggsOnlyQuery() {
        ProcessorDefinition supported = new DummyProcessorDefinition(true);
        ProcessorDefinition unsupported = new DummyProcessorDefinition(false);

        assertFalse(newBinaryProcessor(unsupported, unsupported).supportedByAggsOnlyQuery());
        assertFalse(newBinaryProcessor(unsupported, supported).supportedByAggsOnlyQuery());
        assertFalse(newBinaryProcessor(supported, unsupported).supportedByAggsOnlyQuery());
        assertTrue(newBinaryProcessor(supported, supported).supportedByAggsOnlyQuery());
    }

    private ProcessorDefinition newBinaryProcessor(ProcessorDefinition left, ProcessorDefinition right) {
        return new BinaryProcessorDefinition(null, left, right) {
            @Override
            public Processor asProcessor() {
                return null;
            }
        };
    }

    static class DummyProcessorDefinition extends ProcessorDefinition {
        private final boolean supportedByAggsOnlyQuery;

        DummyProcessorDefinition(boolean supportedByAggsOnlyQuery) {
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
    }
}
