/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class DatasetMappingTests extends AbstractWireSerializingTestCase<DatasetMapping> {

    @Override
    protected Writeable.Reader<DatasetMapping> instanceReader() {
        return DatasetMapping::new;
    }

    @Override
    protected DatasetMapping createTestInstance() {
        return DatasetTests.randomMapping();
    }

    @Override
    protected DatasetMapping mutateInstance(DatasetMapping instance) {
        return randomValueOtherThan(instance, DatasetTests::randomMapping);
    }

    public void testAssembleReturnsNullWhenAllAbsent() {
        assertNull(DatasetMapping.assemble(null, null, null));
    }

    public void testAssembleNonNullWithOnlyTimestamp() {
        DatasetMapping mapping = DatasetMapping.assemble(null, "@timestamp", null);
        assertNotNull(mapping);
        assertNull(mapping.mappings());
        assertEquals("@timestamp", mapping.timestampField());
    }

    public void testDynamicRejectsUnknownValue() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DatasetMapping.Dynamic.fromString("strict"));
        assertTrue(e.getMessage().contains("strict"));
    }

    public void testDynamicDefaultMappingsPreservesOrderAndSource() throws IOException {
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("when", new DatasetFieldMapping("date", "ts"));
        props.put("amount", new DatasetFieldMapping("double", null));
        DatasetMapping.Mappings mappings = new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, props);
        DatasetMapping mapping = new DatasetMapping(mappings, null, null);
        DatasetMapping copy = copyInstance(mapping);
        assertEquals(mapping, copy);
        assertEquals("ts", copy.mappings().properties().get("when").source());
        assertEquals(java.util.List.of("when", "amount"), java.util.List.copyOf(copy.mappings().properties().keySet()));
    }
}
