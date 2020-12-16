/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.rollup.v2;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.mapper.DocValueFetcher;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A {@link FieldValueFetcher} that overrides how values are fetched
 * so that the leaf's `nextValue` returns the interval of the value
 * instead of the value itself. This is For fields defined in a
 * {@link HistogramGroupConfig}
 */
class FieldHistogramValueFetcher extends FieldValueFetcher {

    private final long interval;

    private FieldHistogramValueFetcher(String name, MappedFieldType fieldType, IndexFieldData<?> fieldData, long interval) {
        super(name, fieldType, fieldData);
        this.interval = interval;
    }

    @Override
    DocValueFetcher.Leaf getLeaf(LeafReaderContext context) {
        final LeafFieldData leafFieldData = fieldData.load(context);
        DocValueFetcher.Leaf valueFetcherLeaf = leafFieldData.getLeafValueFetcher(format);
        return new DocValueFetcher.Leaf() {
            @Override
            public boolean advanceExact(int docId) throws IOException {
                return valueFetcherLeaf.advanceExact(docId);
            }

            @Override
            public int docValueCount() throws IOException {
                return valueFetcherLeaf.docValueCount();
            }

            @Override
            public Object nextValue() throws IOException {
                // TODO: Check the type of the returned value (numbers only)
                Double value = (Double) valueFetcherLeaf.nextValue();
                return Math.floor(value / interval) * interval;
            }
        };
    }

    static List<FieldValueFetcher> build(QueryShardContext context, HistogramGroupConfig histogramGroupConfig) {
        List<FieldHistogramValueFetcher> fetchers = new ArrayList<>();
        for (String field : histogramGroupConfig.getFields()) {
            MappedFieldType fieldType = context.getFieldType(field);
            if (fieldType == null) {
                throw new IllegalArgumentException("Unknown field [" + fieldType.name() + "]");
            }
            IndexFieldData<?> fieldData = context.getForField(fieldType);
            fetchers.add(new FieldHistogramValueFetcher(field, fieldType, fieldData, histogramGroupConfig.getInterval()));
        }
        return Collections.unmodifiableList(fetchers);
    }
}
