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
package org.elasticsearch.search.lookup;

import com.google.common.collect.ImmutableMap.Builder;

import org.apache.lucene.index.LeafReaderContext;

public class IndexLookup {

    /**
     * Flag to pass to {@link IndexField#get(String, flags)} if you require
     * offsets in the returned {@link IndexFieldTerm}.
     */
    public static final int FLAG_OFFSETS = 2;

    /**
     * Flag to pass to {@link IndexField#get(String, flags)} if you require
     * payloads in the returned {@link IndexFieldTerm}.
     */
    public static final int FLAG_PAYLOADS = 4;

    /**
     * Flag to pass to {@link IndexField#get(String, flags)} if you require
     * frequencies in the returned {@link IndexFieldTerm}. Frequencies might be
     * returned anyway for some lucene codecs even if this flag is no set.
     */
    public static final int FLAG_FREQUENCIES = 8;

    /**
     * Flag to pass to {@link IndexField#get(String, flags)} if you require
     * positions in the returned {@link IndexFieldTerm}.
     */
    public static final int FLAG_POSITIONS = 16;

    /**
     * Flag to pass to {@link IndexField#get(String, flags)} if you require
     * positions in the returned {@link IndexFieldTerm}.
     */
    public static final int FLAG_CACHE = 32;

    public IndexLookup(Builder<String, Object> builder) {
        builder.put("_FREQUENCIES", IndexLookup.FLAG_FREQUENCIES);
        builder.put("_POSITIONS", IndexLookup.FLAG_POSITIONS);
        builder.put("_OFFSETS", IndexLookup.FLAG_OFFSETS);
        builder.put("_PAYLOADS", IndexLookup.FLAG_PAYLOADS);
        builder.put("_CACHE", IndexLookup.FLAG_CACHE);
    }

    public LeafIndexLookup getLeafIndexLookup(LeafReaderContext context) {
        return new LeafIndexLookup(context);
    }

}
