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

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.ScriptDocValues;

import java.util.Map;

/**
 * Per-segment version of {@link SearchLookup}.
 */
public interface LeafSearchLookup {

    Map<String, Object> asMap();

    SourceLookup source();  // TODO change to Map<String, String>

    Map<Object, Object> fields();

    Map<String, ScriptDocValues<?>> doc();

    void setDocument(int docId);

    static LeafSearchLookup fromIndex(LeafReaderContext ctx, LeafDocLookup docMap, SourceLookup sourceLookup, LeafStoredFieldsLookup fieldsLookup) {
        Map<String, Object> asMap = Map.of(
            "doc", docMap,
            "_doc", docMap,
            "_source", sourceLookup,
            "_fields", fieldsLookup);
        return new LeafSearchLookup() {
            @Override
            public Map<String, Object> asMap() {
                return asMap;
            }

            @Override
            public SourceLookup source() {
                return sourceLookup;
            }

            @Override
            public Map<Object, Object> fields() {
                return fieldsLookup;
            }

            @Override
            public Map<String, ScriptDocValues<?>> doc() {
                return docMap;
            }

            @Override
            public void setDocument(int docId) {
                docMap.setDocument(docId);
                sourceLookup.setSegmentAndDocument(ctx, docId);
                fieldsLookup.setDocument(docId);
            }
        };
    }
}
