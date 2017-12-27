/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.BinaryProcessorDefinitionTests.DummyProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;

import static java.util.Collections.emptyList;

public class UnaryProcessorDefinitionTests extends ESTestCase {
    public void testSupportedByAggsOnlyQuery() {
        ProcessorDefinition supported = new DummyProcessorDefinition(true);
        ProcessorDefinition unsupported = new DummyProcessorDefinition(false);

        assertFalse(newUnaryProcessor(unsupported).supportedByAggsOnlyQuery());
        assertTrue(newUnaryProcessor(supported).supportedByAggsOnlyQuery());
    }

    private ProcessorDefinition newUnaryProcessor(ProcessorDefinition child) {
        return new UnaryProcessorDefinition(null, child, null);
    }
}
