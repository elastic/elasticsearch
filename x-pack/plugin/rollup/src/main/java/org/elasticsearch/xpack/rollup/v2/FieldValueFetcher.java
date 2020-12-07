/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.rollup.v2;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.mapper.DocValueFetcher;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class FieldValueFetcher {
    final String name;
    final MappedFieldType fieldType;
    final DocValueFormat format;
    final IndexFieldData<?> fieldData;

    private FieldValueFetcher(String name, MappedFieldType fieldType, IndexFieldData<?> fieldData) {
        this.name = name;
        this.fieldType = fieldType;
        this.format = fieldType.docValueFormat(null, null);
        this.fieldData = fieldData;
    }

    DocValueFetcher.Leaf getLeaf(LeafReaderContext context) {
        final LeafFieldData leafFieldData = fieldData.load(context);
        return leafFieldData.getLeafValueFetcher(format);
    }

    Object format(Object value) {
        if (value instanceof Long) {
            return format.format((long) value);
        } else if (value instanceof Double) {
            return format.format((double) value);
        } else if (value instanceof BytesRef) {
            return format.format((BytesRef) value);
        } else if (value instanceof String) {
            return value.toString();
        } else {
            throw new IllegalArgumentException("Invalid type:[" + value.getClass() + "]");
        }
    }

    static List<FieldValueFetcher> build(QueryShardContext context, String[] fields) {
        List<FieldValueFetcher> fetchers = new ArrayList<>();
        for (String field : fields) {
            MappedFieldType fieldType = context.getFieldType(field);
            if (fieldType == null) {
                throw new IllegalArgumentException("Unknown field [" + fieldType.name() + "]");
            }
            IndexFieldData<?> fieldData = context.getForField(fieldType);
            fetchers.add(new FieldValueFetcher(field, fieldType, fieldData));
        }
        return Collections.unmodifiableList(fetchers);
    }
}
