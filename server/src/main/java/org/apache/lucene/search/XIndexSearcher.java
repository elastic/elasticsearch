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

package org.apache.lucene.search;

import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;
import java.util.List;

/**
 * A wrapper for {@link IndexSearcher} that makes {@link IndexSearcher#search(List, Weight, Collector)}
 * visible by sub-classes.
 */
public class XIndexSearcher extends IndexSearcher {
    private final IndexSearcher in;

    public XIndexSearcher(IndexSearcher in) {
        super(in.getIndexReader());
        this.in = in;
        setSimilarity(in.getSimilarity());
        setQueryCache(in.getQueryCache());
        setQueryCachingPolicy(in.getQueryCachingPolicy());
    }

    @Override
    public void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
        in.search(leaves, weight, collector);
    }
}
