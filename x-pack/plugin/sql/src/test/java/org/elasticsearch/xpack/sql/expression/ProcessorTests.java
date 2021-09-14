/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;
import org.elasticsearch.xpack.sql.tree.SqlNodeSubclassTests;
import org.junit.BeforeClass;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toCollection;

public class ProcessorTests extends ESTestCase {

    private static Set<Class<? extends Processor>> processors;

    @BeforeClass
    public static void init() throws Exception {
        processors = SqlNodeSubclassTests.subclassesOf(Processor.class);
    }

    public void testProcessorRegistration() throws Exception {
        LinkedHashSet<String> registered = Processors.getNamedWriteables().stream()
                .filter(e -> Processor.class == e.categoryClass)
                .map(e -> e.name)
                .collect(toCollection(LinkedHashSet::new));

        // discover available processors
        int missing = processors.size() - registered.size();

        List<String> notRegistered = new ArrayList<>();
        Set<String> processorNames = new LinkedHashSet<>();
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
            processorNames.add(value);
            if (registered.contains(value) == false) {
                notRegistered.add(procName);
            }
            Class<?> declaringClass = proc.getMethod("getWriteableName").getDeclaringClass();
            assertEquals("Processor: " + proc + " doesn't override getWriteableName", proc, declaringClass);
        }

        if (notRegistered.isEmpty() == false) {
            fail(missing + " processor(s) not registered : " + notRegistered);
        } else {
            // FIXME: the project split causes different behaviour between Gradle vs IDEs
            // Eclipse considers classes from both projects, Gradle does not
            // hence why this is disabled for now
            registered.removeAll(processorNames);
            if (registered.isEmpty() == false) {
                fail("Detection failed: discovered more registered processors than actual classes; extra " + registered);
            }
        }
    }
}
