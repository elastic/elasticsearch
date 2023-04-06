/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite.InternalBucket;
import org.elasticsearch.xpack.ql.execution.search.extractor.BucketExtractor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.ql.util.CollectionUtils.mapSize;

public class SampleCriterion extends Criterion<SampleQueryRequest> {

    private final List<BucketExtractor> keys;
    private final List<String> keyFields;
    // the first query to run (sample filter + composite aggs on keys)
    private final SampleQueryRequest firstQuery;
    // intermediate queries (sample filter + composite aggs on keys + keys values filtering)
    private final SampleQueryRequest midQuery;
    // final stage query (sample filter + a single set of keys values filter)
    private final SampleQueryRequest finalQuery;

    public SampleCriterion(
        SampleQueryRequest firstQuery,
        SampleQueryRequest midQuery,
        SampleQueryRequest finalQuery,
        List<String> keyFields,
        List<BucketExtractor> keys
    ) {
        super(keys.size());
        this.firstQuery = firstQuery;
        this.midQuery = midQuery;
        this.finalQuery = finalQuery;
        this.keys = keys;
        this.keyFields = keyFields;
    }

    public SampleQueryRequest firstQuery() {
        return firstQuery;
    }

    public SampleQueryRequest midQuery() {
        return midQuery;
    }

    public SampleQueryRequest finalQuery() {
        return finalQuery;
    }

    public List<Map<String, Object>> keys(List<InternalBucket> buckets) {
        List<Map<String, Object>> values = new ArrayList<>(buckets.size());
        for (Bucket bucket : buckets) {
            values.add(key(bucket));
        }
        return values;
    }

    public List<List<Object>> keyValues(List<InternalBucket> buckets) {
        List<Map<String, Object>> keys = keys(buckets);
        final List<List<Object>> compositeKeyValues = new ArrayList<>(buckets.size());
        for (Map<String, Object> key : keys) {
            final List<Object> values = new ArrayList<>(keySize());
            for (String keyField : keyFields) {
                values.add(key.get(keyField));
            }
            compositeKeyValues.add(values);
        }
        return compositeKeyValues;
    }

    private Map<String, Object> key(Bucket bucket) {
        Map<String, Object> key = new HashMap<>(mapSize(keySize()));
        for (int i = 0; i < keySize(); i++) {
            key.put(keyFields.get(i), keys.get(i).extract(bucket));
        }
        return key;
    }

    @Override
    public String toString() {
        return keys.toString();
    }
}
