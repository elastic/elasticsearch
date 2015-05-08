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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;

/**
 *
 */
public class SearchLookupImpl {

    final DocLookupImpl docMap;

    final SourceLookupImpl sourceLookup;

    final FieldsLookupImpl fieldsLookup;

    final IndexLookupImpl indexLookup;

    final ImmutableMap<String, Object> asMap;

    public SearchLookupImpl(MapperService mapperService, IndexFieldDataService fieldDataService, @Nullable String[] types) {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        docMap = new DocLookupImpl(mapperService, fieldDataService, types);
        sourceLookup = new SourceLookupImpl();
        fieldsLookup = new FieldsLookupImpl(mapperService, types);
        indexLookup = new IndexLookupImpl(builder);
        asMap = builder.build();
    }

    public LeafSearchLookupImpl getLeafSearchLookup(LeafReaderContext context) {
        return new LeafSearchLookupImpl(context,
                docMap.getLeafDocLookup(context),
                sourceLookup,
                fieldsLookup.getLeafFieldsLookup(context),
                indexLookup.getLeafIndexLookup(context),
                asMap);
    }

    public DocLookupImpl doc() {
        return docMap;
    }

    public SourceLookupImpl source() {
        return sourceLookup;
    }
}
