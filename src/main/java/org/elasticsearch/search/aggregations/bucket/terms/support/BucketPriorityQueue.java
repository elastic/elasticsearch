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
package org.elasticsearch.search.aggregations.bucket.terms.support;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.util.Comparator;

public class BucketPriorityQueue extends PriorityQueue<Terms.Bucket> {

    private final Comparator<Terms.Bucket> comparator;

    public BucketPriorityQueue(int size, Comparator<Terms.Bucket> comparator) {
        super(size);
        this.comparator = comparator;
    }

    @Override
    protected boolean lessThan(Terms.Bucket a, Terms.Bucket b) {
        return comparator.compare(a, b) > 0; // reverse, since we reverse again when adding to a list
    }
}
