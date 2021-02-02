/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.function.Function;

public final class DocLookup {

    private final Function<String, MappedFieldType> fieldTypeLookup;
    private final Function<MappedFieldType, IndexFieldData<?>> fieldDataLookup;

    DocLookup(Function<String, MappedFieldType> fieldTypeLookup, Function<MappedFieldType, IndexFieldData<?>> fieldDataLookup) {
        this.fieldTypeLookup = fieldTypeLookup;
        this.fieldDataLookup = fieldDataLookup;
    }

    public MappedFieldType fieldType(String fieldName) {
        return fieldTypeLookup.apply(fieldName);
    }

    public IndexFieldData<?> getForField(MappedFieldType fieldType) {
        return fieldDataLookup.apply(fieldType);
    }

    public LeafDocLookup getLeafDocLookup(LeafReaderContext context) {
        return new LeafDocLookup(fieldTypeLookup, fieldDataLookup, context);
    }
}
