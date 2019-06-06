/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.tree.NodeSubclassTests;
import org.junit.BeforeClass;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import static java.util.stream.Collectors.toCollection;


public class ProcessorTests extends ESTestCase {

    private static List<Class<? extends Processor>> processors;

    @BeforeClass
    public static void init() throws Exception {
        processors = NodeSubclassTests.subclassesOf(Processor.class);
    }

    public void testProcessorRegistration() throws Exception {
        LinkedHashSet<String> registered = Processors.getNamedWriteables().stream()
                .map(e -> e.name)
                .collect(toCollection(LinkedHashSet::new));

        // discover available processors
        int missing = processors.size() - registered.size();

        List<String> notRegistered = new ArrayList<>();
        for (Class<? extends Processor> proc : processors) {
            String procName = proc.getName();
            assertTrue(procName + " does NOT implement NamedWriteable", NamedWriteable.class.isAssignableFrom(proc));
            Field name = null;
            String value = null;
            try {
                name = proc.getField("NAME");
            } catch (Exception ex) {
                fail(procName + " does NOT provide a NAME field\n" + ex);
            }
            try {
                value = name.get(proc).toString();
            } catch (Exception ex) {
                fail(procName + " does NOT provide a static NAME field\n" + ex);
            }
            if (!registered.contains(value)) {
                notRegistered.add(procName);
            }
            Class<?> declaringClass = proc.getMethod("getWriteableName").getDeclaringClass();
            assertEquals("Processor: " + proc + " doesn't override getWriteableName", proc, declaringClass);
        }

        if (missing > 0) {
            fail(missing + " processor(s) not registered : " + notRegistered);
        } else {
            assertEquals("Detection failed: discovered more registered processors than classes", 0, missing);
        }
    }
}
