/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.comparators.DocComparator;

 /**
  * A {@link SortField} that first compares the shard index and then uses the document number (_doc)
  * to tiebreak if the value is the same.
  **/
public class ShardDocSortField extends SortField {
    public static final String NAME = "_shard_doc";

    private final int shardRequestIndex;

    public ShardDocSortField(int shardRequestIndex, boolean reverse) {
        super(NAME, Type.LONG, reverse);
        assert shardRequestIndex >= 0;
        this.shardRequestIndex = shardRequestIndex;
    }

    int getShardRequestIndex() {
        return shardRequestIndex;
    }

    @Override
    public FieldComparator<?> getComparator(int numHits, int sortPos) {
        final DocComparator delegate = new DocComparator(numHits, getReverse(), sortPos);

        return new FieldComparator<Long>() {
            @Override
            public int compare(int slot1, int slot2) {
                return delegate.compare(slot1, slot2);
            }

            @Override
            public int compareValues(Long first, Long second) {
                return Long.compare(first, second);
            }

            @Override
            public void setTopValue(Long value) {
                int topShardIndex = (int) (value >> 32);
                if (shardRequestIndex == topShardIndex) {
                    delegate.setTopValue(value.intValue());
                } else if (shardRequestIndex < topShardIndex) {
                    delegate.setTopValue(Integer.MAX_VALUE);
                } else {
                    delegate.setTopValue(-1);
                }
            }

            @Override
            public Long value(int slot) {
                return (((long) shardRequestIndex) << 32) | (delegate.value(slot) & 0xFFFFFFFFL);
            }

            @Override
            public LeafFieldComparator getLeafComparator(LeafReaderContext context) {
                return delegate.getLeafComparator(context);
            }
        };
    }
}
