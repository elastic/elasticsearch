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

import com.google.common.collect.ImmutableMap;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.LeafFieldsLookup;
import org.elasticsearch.search.lookup.LeafIndexLookup;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.Map;

/**
 * Per-segment version of {@link SearchLookup}.
 */
public class LeafSearchLookupImpl implements LeafSearchLookup {

    final LeafReaderContext ctx;
    final LeafDocLookup docMap;
    final SourceLookupImpl sourceLookup;
    final LeafFieldsLookup fieldsLookup;
    final LeafIndexLookup indexLookup;
    final ImmutableMap<String, Object> asMap;

    public LeafSearchLookupImpl(LeafReaderContext ctx, LeafDocLookup docMap, SourceLookupImpl sourceLookup,
            LeafFieldsLookup fieldsLookup, LeafIndexLookup indexLookup, Map<String, Object> topLevelMap) {
        this.ctx = ctx;
        this.docMap = docMap;
        this.sourceLookup = sourceLookup;
        this.fieldsLookup = fieldsLookup;
        this.indexLookup = indexLookup;

        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.putAll(topLevelMap);
        
        builder.put("doc", LookupImpl.createProxy(LeafDocLookup.class, docMap));
        builder.put("_doc", LookupImpl.createProxy(LeafDocLookup.class, docMap));
        builder.put("_source", LookupImpl.createProxy(SourceLookup.class, sourceLookup));
        builder.put("_fields", LookupImpl.createProxy(LeafFieldsLookup.class, fieldsLookup));
        builder.put("_index", LookupImpl.createProxy(LeafIndexLookup.class, indexLookup));
        asMap = builder.build();
    }

    public ImmutableMap<String, Object> asMap() {
        return this.asMap;
    }

    public SourceLookupImpl source() {
        return this.sourceLookup;
    }

    public LeafIndexLookup indexLookup() {
        return this.indexLookup;
    }

    public LeafFieldsLookup fields() {
        return this.fieldsLookup;
    }

    public LeafDocLookup doc() {
        return this.docMap;
    }

    public void setDocument(int docId) {
        docMap.setDocument(docId);
        sourceLookup.setSegmentAndDocument(ctx, docId);
        fieldsLookup.setDocument(docId);
        indexLookup.setDocument(docId);
    }
}
