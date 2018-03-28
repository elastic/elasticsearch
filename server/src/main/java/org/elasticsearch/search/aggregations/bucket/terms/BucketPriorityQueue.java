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
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.PriorityQueue;

import java.util.Comparator;

public class BucketPriorityQueue<B extends Terms.Bucket> extends PriorityQueue<B> {

    private final Comparator<? super B> comparator;

    public BucketPriorityQueue(int size, Comparator<? super B> comparator) {
        super(size);
        this.comparator = comparator;
    }

    @Override
    protected boolean lessThan(B a, B b) {
        return comparator.compare(a, b) > 0; // reverse, since we reverse again when adding to a list
    }
}
