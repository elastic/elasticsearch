/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;

public class MultiFieldValueFetcher extends FieldValueFetcher {

    public MultiFieldValueFetcher(MappedFieldType fieldType, IndexFieldData<?> fieldData) {
        super(fieldType, fieldData);
    }

    @Override
    public String name() {
        return fieldType.name().substring(0, fieldType.name().lastIndexOf('.'));
    }
}
