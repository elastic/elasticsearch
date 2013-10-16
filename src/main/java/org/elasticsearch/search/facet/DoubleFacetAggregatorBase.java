/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.search.facet;

import org.elasticsearch.index.fielddata.DoubleValues;

/**
 * Simple Facet aggregator base class for {@link DoubleValues}
 */
public abstract class DoubleFacetAggregatorBase {
    private int total;
    private int missing;

    public void onDoc(int docId, DoubleValues values) {
        int numValues = values.setDocument(docId);
        int tempMissing = 1;
        for (int i = 0; i < numValues; i++) {
            tempMissing = 0;
            onValue(docId, values.nextValue());
            total++;
        }
        missing += tempMissing;
    }

    protected abstract void onValue(int docId, double next);

    public final int total() {
        return total;
    }

    public final int missing() {
        return missing;
    }
}
