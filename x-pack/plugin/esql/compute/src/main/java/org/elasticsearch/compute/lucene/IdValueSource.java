/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.support.ValuesSource;

public class IdValueSource extends ValuesSource.Bytes {

    private final IdFieldIndexFieldData indexFieldData;

    public IdValueSource(IdFieldIndexFieldData indexFieldData) {
        this.indexFieldData = indexFieldData;
    }

    @Override
    public SortedBinaryDocValues bytesValues(LeafReaderContext leafReaderContext) {
        return indexFieldData.load(leafReaderContext).getBytesValues();
    }
}
