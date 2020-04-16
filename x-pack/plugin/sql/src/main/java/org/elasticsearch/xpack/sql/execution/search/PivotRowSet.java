/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.xpack.ql.execution.search.extractor.BucketExtractor;
import org.elasticsearch.xpack.ql.type.Schema;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static java.util.Collections.emptyList;

class PivotRowSet extends SchemaCompositeAggRowSet {

    private final List<Object[]> data;
    private final Map<String, Object> lastAfterKey;

    PivotRowSet(Schema schema, List<BucketExtractor> exts, BitSet mask, SearchResponse response, int limit,
            Map<String, Object> previousLastKey) {
        super(schema, exts, mask, response, limit);

        data = buckets.isEmpty() ? emptyList() : new ArrayList<>();

        // the last page contains no data, handle that to avoid NPEs and such
        if (buckets.isEmpty()) {
            lastAfterKey = null;
            return;
        }

        // consume buckets until all pivot columns are initialized or the next grouping starts
        // to determine a group, find all group-by extractors (CompositeKeyExtractor)
        // extract their values and keep iterating through the buckets as long as the result is the same

        Map<String, Object> currentRowGroupKey = null;
        Map<String, Object> lastCompletedGroupKey = null;
        Object[] currentRow = new Object[columnCount()];

        for (int bucketIndex = 0; bucketIndex < buckets.size(); bucketIndex++) {
            CompositeAggregation.Bucket bucket = buckets.get(bucketIndex);
            Map<String, Object> key = bucket.getKey();

            // does the bucket below to the same group?
            if (currentRowGroupKey == null || sameCompositeKey(currentRowGroupKey, key)) {
                currentRowGroupKey = key;
            }
            // done computing row
            else {
                // be sure to remember the last consumed group before changing to the new one
                lastCompletedGroupKey = currentRowGroupKey;
                currentRowGroupKey = key;
                // save the data
                data.add(currentRow);

                if (limit > 0 && data.size() == limit) {
                    break;
                }
                // create a new row
                currentRow = new Object[columnCount()];
            }

            for (int columnIndex = 0; columnIndex < currentRow.length; columnIndex++) {
                BucketExtractor extractor = userExtractor(columnIndex);
                Object value = extractor.extract(bucket);

                // rerun the bucket through all the extractors but update only the non-null components
                // since the pivot extractors will react only when encountering the matching group
                if (currentRow[columnIndex] == null && value != null) {
                    currentRow[columnIndex] = value;
                }
            }
        }
        
        // check the last group using the following:
        // a. limit has been reached, the rest of the data is ignored.
        if (limit > 0 && data.size() == limit) {
            afterKey = null;
        }
        // b. the last key has been sent before (it's the last page)
        else if ((previousLastKey != null && sameCompositeKey(previousLastKey, currentRowGroupKey))) {
            data.add(currentRow);
            afterKey = null;
        }
        // c. all the values are initialized (there might be another page but no need to ask for the group again)
        // d. or no data was added (typically because there's a null value such as the group)
        else if (hasNull(currentRow) == false || data.isEmpty()) {
            data.add(currentRow);
            afterKey = currentRowGroupKey;
        }
        // otherwise we can't tell whether it's complete or not
        // so discard the last group and ask for it on the next page
        else {
            afterKey = lastCompletedGroupKey;
        }

        // lastly initialize the size and remainingData
        size = data.size();
        remainingData = remainingData(afterKey != null, size, limit);
        lastAfterKey = currentRowGroupKey;
    }

    private boolean hasNull(Object[] currentRow) {
        for (Object object : currentRow) {
            if (object == null) {
                return true;
            }
        }
        return false;
    }

    // compare the equality of two composite key WITHOUT the last group
    // this method relies on the internal map implementation which preserves the key position
    // hence why the comparison happens against the current key (not the previous one which might
    // have a different order due to serialization)
    static boolean sameCompositeKey(Map<String, Object> previous, Map<String, Object> current) {
        int keys = current.size() - 1;
        int keyIndex = 0;
        for (Entry<String, Object> entry : current.entrySet()) {
            if (keyIndex++ >= keys) {
                return true;
            }
            if (Objects.equals(entry.getValue(), previous.get(entry.getKey())) == false) {
                return false;
            }
        }
        // there's no other key, it's the same group
        return true;
    }

    @Override
    protected Object getColumn(int column) {
        return data.get(row)[column];
    }

    Map<String, Object> lastAfterKey() {
        return lastAfterKey;
    }
}