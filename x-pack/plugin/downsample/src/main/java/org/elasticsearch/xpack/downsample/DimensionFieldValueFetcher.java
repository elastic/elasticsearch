/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DimensionFieldValueFetcher extends FieldValueFetcher {

    private final DimensionFieldProducer dimensionFieldProducer = createFieldProducer();

    protected DimensionFieldValueFetcher(final MappedFieldType fieldType, final IndexFieldData<?> fieldData) {
        super(fieldType.name(), fieldType, fieldData);
    }

    private DimensionFieldProducer createFieldProducer() {
        final String filedName = fieldType.name();
        return new DimensionFieldProducer(filedName, new DimensionFieldProducer.Dimension(filedName));
    }

    @Override
    public AbstractDownsampleFieldProducer fieldProducer() {
        return this.dimensionFieldProducer;
    }

    /**
     * Retrieve field value fetchers for a list of dimensions.
     */
    static List<FieldValueFetcher> create(final SearchExecutionContext context, final String[] dimensions) {
        List<FieldValueFetcher> fetchers = new ArrayList<>();
        for (String dimension : dimensions) {
            MappedFieldType fieldType = context.getFieldType(dimension);
            assert fieldType != null : "Unknown dimension field type for dimension field: [" + dimension + "]";

            if (context.fieldExistsInIndex(dimension)) {
                final IndexFieldData<?> fieldData = context.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
                final String fieldName = context.isMultiField(dimension)
                    ? fieldType.name().substring(0, fieldType.name().lastIndexOf('.'))
                    : fieldType.name();
                fetchers.add(new DimensionFieldValueFetcher(fieldType, fieldData));
            }
        }
        return Collections.unmodifiableList(fetchers);
    }
}
