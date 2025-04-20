/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.Collections;

public class SparseVectorFieldTypeTests extends FieldTypeTestCase {

    public void testDocValuesDisabled() {
        MappedFieldType fieldType = new SparseVectorFieldMapper.SparseVectorFieldType("field", false, Collections.emptyMap());
        assertFalse(fieldType.hasDocValues());
        expectThrows(IllegalArgumentException.class, () -> fieldType.fielddataBuilder(FieldDataContext.noRuntimeFields("test")));
    }

    public void testIsNotAggregatable() {
        MappedFieldType fieldType = new SparseVectorFieldMapper.SparseVectorFieldType("field", false, Collections.emptyMap());
        assertFalse(fieldType.isAggregatable());
    }
}
