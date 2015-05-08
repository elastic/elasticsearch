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
package org.elasticsearch.search.lookup.impl;

import com.google.common.collect.ImmutableMap.Builder;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.lookup.IndexLookup;
import org.elasticsearch.search.lookup.LeafIndexLookup;

public class IndexLookupImpl implements IndexLookup {

    public IndexLookupImpl(Builder<String, Object> builder) {
        builder.put("_FREQUENCIES", FLAG_FREQUENCIES);
        builder.put("_POSITIONS", FLAG_POSITIONS);
        builder.put("_OFFSETS", FLAG_OFFSETS);
        builder.put("_PAYLOADS", FLAG_PAYLOADS);
        builder.put("_CACHE", FLAG_CACHE);
    }

    public LeafIndexLookup getLeafIndexLookup(LeafReaderContext context) {
        return new LeafIndexLookupImpl(context);
    }

}
