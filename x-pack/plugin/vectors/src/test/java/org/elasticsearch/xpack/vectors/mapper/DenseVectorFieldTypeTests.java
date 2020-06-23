/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.mapper;

import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.Map;

public class DenseVectorFieldTypeTests extends FieldTypeTestCase<MappedFieldType> {

    @Override
    protected MappedFieldType createDefaultFieldType(String name, Map<String, String> meta) {
        return new DenseVectorFieldMapper.DenseVectorFieldType(name, 4, meta);
    }
}
