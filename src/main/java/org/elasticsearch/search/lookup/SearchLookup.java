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

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;

/**
 *
 */
public class SearchLookup {

    final DocLookup docMap;

    final SourceLookup sourceLookup;

    final FieldsLookup fieldsLookup;
    
    final IndexLookup indexLookup;
    
    final ImmutableMap<String, Object> asMap;

    public SearchLookup(MapperService mapperService, IndexFieldDataService fieldDataService, @Nullable String[] types) {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        docMap = new DocLookup(mapperService, fieldDataService, types);
        sourceLookup = new SourceLookup();
        fieldsLookup = new FieldsLookup(mapperService, types);
        indexLookup = new IndexLookup(builder);
        
        builder.put("doc", docMap);
        builder.put("_doc", docMap);
        builder.put("_source", sourceLookup);
        builder.put("_fields", fieldsLookup);
        builder.put("_index", indexLookup);
        asMap = builder.build();
    }

    public ImmutableMap<String, Object> asMap() {
        return this.asMap;
    }

    public SourceLookup source() {
        return this.sourceLookup;
    }
    
    public IndexLookup indexLookup() {
        return this.indexLookup;
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

    public void setNextReader(AtomicReaderContext context) {
        docMap.setNextReader(context);
        sourceLookup.setNextReader(context);
        fieldsLookup.setNextReader(context);
        indexLookup.setNextReader(context);
    }

    public void setNextDocId(int docId) {
        docMap.setNextDocId(docId);
        sourceLookup.setNextDocId(docId);
        fieldsLookup.setNextDocId(docId);
        indexLookup.setNextDocId(docId);
    }
}
