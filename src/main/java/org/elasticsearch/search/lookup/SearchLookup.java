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

package org.elasticsearch.search.lookup;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.mapper.MapperService;

/**
 *
 */
public class SearchLookup {

    final DocLookup docMap;

    final SourceLookup sourceLookup;

    final FieldsLookup fieldsLookup;

    final ImmutableMap<String, Object> asMap;

    public SearchLookup(MapperService mapperService, FieldDataCache fieldDataCache, @Nullable String[] types) {
        docMap = new DocLookup(mapperService, fieldDataCache, types);
        sourceLookup = new SourceLookup();
        fieldsLookup = new FieldsLookup(mapperService, types);
        asMap = ImmutableMap.<String, Object>of("doc", docMap, "_doc", docMap, "_source", sourceLookup, "_fields", fieldsLookup);
    }

    public ImmutableMap<String, Object> asMap() {
        return this.asMap;
    }

    public SourceLookup source() {
        return this.sourceLookup;
    }

    public FieldsLookup fields() {
        return this.fieldsLookup;
    }

    public DocLookup doc() {
        return this.docMap;
    }

    public void setScorer(Scorer scorer) {
        docMap.setScorer(scorer);
    }

    public void setNextReader(IndexReader reader) {
        docMap.setNextReader(reader);
        sourceLookup.setNextReader(reader);
        fieldsLookup.setNextReader(reader);
    }

    public void setNextDocId(int docId) {
        docMap.setNextDocId(docId);
        sourceLookup.setNextDocId(docId);
        fieldsLookup.setNextDocId(docId);
    }
}
