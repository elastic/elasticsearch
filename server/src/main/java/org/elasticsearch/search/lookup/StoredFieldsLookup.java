/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.function.Function;

public class StoredFieldsLookup {

    private final Function<String, MappedFieldType> fieldTypeLookup;

    StoredFieldsLookup(Function<String, MappedFieldType> fieldTypeLookup) {
        this.fieldTypeLookup = fieldTypeLookup;
    }

    LeafStoredFieldsLookup getLeafFieldsLookup(LeafReaderContext context) {
        return new LeafStoredFieldsLookup(fieldTypeLookup, context.reader());
    }
}
