/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.IndexHistogramFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;

public class HistogramFieldValueFetcher extends FieldValueFetcher {
    private final IndexHistogramFieldData histogramFieldData;

    protected HistogramFieldValueFetcher(MappedFieldType fieldType, IndexHistogramFieldData histogramFieldData) {
        super(fieldType, histogramFieldData);
        this.histogramFieldData = histogramFieldData;
    }

    @Override
    public LeafFieldData getLeafFieldData(LeafReaderContext context) {
        return this.histogramFieldData.load(context);
    }
}
