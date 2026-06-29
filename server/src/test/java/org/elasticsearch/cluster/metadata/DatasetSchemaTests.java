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

public class DatasetSchemaTests extends AbstractWireSerializingTestCase<DatasetSchema> {

    @Override
    protected Writeable.Reader<DatasetSchema> instanceReader() {
        return DatasetSchema::new;
    }

    @Override
    protected DatasetSchema createTestInstance() {
        return DatasetTests.randomSchema();
    }

    @Override
    protected DatasetSchema mutateInstance(DatasetSchema instance) {
        return randomValueOtherThan(instance, DatasetTests::randomSchema);
    }

    public void testAssembleReturnsNullWhenAllAbsent() {
        assertNull(DatasetSchema.assemble(null, null, null));
    }

    public void testAssembleNonNullWithOnlyTimestamp() {
        DatasetSchema schema = DatasetSchema.assemble(null, "@timestamp", null);
        assertNotNull(schema);
        assertNull(schema.mappings());
        assertEquals("@timestamp", schema.timestampField());
    }

    public void testDynamicRejectsUnknownValue() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DatasetSchema.Dynamic.fromString("strict"));
        assertTrue(e.getMessage().contains("strict"));
    }

    public void testDynamicDefaultMappingsPreservesOrderAndSource() throws IOException {
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("when", new DatasetFieldMapping("date", "ts"));
        props.put("amount", new DatasetFieldMapping("double", null));
        DatasetSchema.Mappings mappings = new DatasetSchema.Mappings(DatasetSchema.Dynamic.TRUE, props);
        DatasetSchema schema = new DatasetSchema(mappings, null, null);
        DatasetSchema copy = copyInstance(schema);
        assertEquals(schema, copy);
        assertEquals("ts", copy.mappings().properties().get("when").source());
        assertEquals(java.util.List.of("when", "amount"), java.util.List.copyOf(copy.mappings().properties().keySet()));
    }
}
