/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DimensionFieldValueFetcher extends FieldValueFetcher {

    private final DimensionFieldProducer dimensionFieldProducer = createFieldProducer();

    protected DimensionFieldValueFetcher(final String fieldName, final MappedFieldType fieldType, final IndexFieldData<?> fieldData) {
        super(fieldName, fieldType, fieldData);
    }

    private DimensionFieldProducer createFieldProducer() {
        return new DimensionFieldProducer(name, new DimensionFieldProducer.Dimension(name));
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
            assert fieldType != null : "Unknown type for dimension field: [" + dimension + "]";

            if (context.fieldExistsInIndex(fieldType.name())) {
                final IndexFieldData<?> fieldData = context.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
                if (fieldType instanceof FlattenedFieldMapper.KeyedFlattenedFieldType flattenedFieldType) {
                    // Name of the field type and name of the dimension are different in this case.
                    var dimensionName = flattenedFieldType.rootName() + '.' + flattenedFieldType.key();
                    fetchers.add(new DimensionFieldValueFetcher(dimensionName, fieldType, fieldData));
                } else {
                    final String fieldName = context.isMultiField(dimension)
                        ? fieldType.name().substring(0, fieldType.name().lastIndexOf('.'))
                        : fieldType.name();
                    fetchers.add(new DimensionFieldValueFetcher(fieldName, fieldType, fieldData));
                }
            }
        }
        return Collections.unmodifiableList(fetchers);
    }
}
