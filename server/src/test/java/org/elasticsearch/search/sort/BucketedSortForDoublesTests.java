/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;

public class BucketedSortForDoublesTests extends BucketedSortTestCase<BucketedSort.ForDoubles> {
    @Override
    public BucketedSort.ForDoubles build(SortOrder sortOrder, DocValueFormat format, double[] values) {
        return new BucketedSort.ForDoubles(bigArrays(), sortOrder, format) {
            @Override
            public Leaf forLeaf(LeafReaderContext ctx) throws IOException {
                return new Leaf() {
                    int index = -1;

                    @Override
                    protected boolean advanceExact(int doc) throws IOException {
                        index = doc;
                        return doc < values.length;
                    }

                    @Override
                    protected double docValue() throws IOException {
                        return values[index];
                    }
                };
            }
        };
    }

    @Override
    protected SortValue expectedSortValue(double v) {
        return SortValue.from(v);
    }
}
