/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.elasticsearch.test.ESTestCase;

public class NamedWriteableRegistryTests extends ESTestCase {

    private static class DummyNamedWriteable implements NamedWriteable {
        DummyNamedWriteable(StreamInput in) {}
        @Override
        public String getWriteableName() {
            return "test";
        }
        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }

    public void testEmpty() throws IOException {
        new NamedWriteableRegistry(Collections.emptyList()); // does not throw exception
    }

    public void testBasic() throws IOException {
        NamedWriteableRegistry.Entry entry =
            new NamedWriteableRegistry.Entry(NamedWriteable.class, "test", DummyNamedWriteable::new);
        NamedWriteableRegistry registry = new NamedWriteableRegistry(Collections.singletonList(entry));
        Writeable.Reader<? extends NamedWriteable> reader = registry.getReader(NamedWriteable.class, "test");
        assertNotNull(reader.read(null));
    }

    public void testDuplicates() throws IOException {
        NamedWriteableRegistry.Entry entry =
            new NamedWriteableRegistry.Entry(NamedWriteable.class, "test", DummyNamedWriteable::new);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new NamedWriteableRegistry(Arrays.asList(entry, entry)));
        assertTrue(e.getMessage(), e.getMessage().contains("is already registered"));
    }

    public void testUnknownCategory() throws IOException {
        NamedWriteableRegistry registry = new NamedWriteableRegistry(Collections.emptyList());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            registry.getReader(NamedWriteable.class, "test"));
        assertTrue(e.getMessage(), e.getMessage().contains("Unknown NamedWriteable category ["));
    }

    public void testUnknownName() throws IOException {
        NamedWriteableRegistry.Entry entry =
            new NamedWriteableRegistry.Entry(NamedWriteable.class, "test", DummyNamedWriteable::new);
        NamedWriteableRegistry registry = new NamedWriteableRegistry(Collections.singletonList(entry));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            registry.getReader(NamedWriteable.class, "dne"));
        assertTrue(e.getMessage(), e.getMessage().contains("Unknown NamedWriteable ["));
    }
}
