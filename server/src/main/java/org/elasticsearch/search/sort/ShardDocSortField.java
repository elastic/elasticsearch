/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        final DocComparator delegate = new DocComparator(numHits, false, sortPos);

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
