/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;

public class PointFieldTypeTests extends FieldTypeTestCase<MappedFieldType> {
    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new PointFieldMapper.PointFieldType();
    }
}
