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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;

import java.util.function.Function;

public class SearchLookup {

    final DocLookup docMap;

    final SourceLookup sourceLookup;

    final FieldsLookup fieldsLookup;

    public SearchLookup(MapperService mapperService, Function<MappedFieldType, IndexFieldData<?>> fieldDataLookup,
                        @Nullable String[] types) {
        docMap = new DocLookup(mapperService, fieldDataLookup, types);
        sourceLookup = new SourceLookup();
        fieldsLookup = new FieldsLookup(mapperService, types);
    }

    public LeafSearchLookup getLeafSearchLookup(LeafReaderContext context) {
        return new LeafSearchLookup(context,
                docMap.getLeafDocLookup(context),
                sourceLookup,
                fieldsLookup.getLeafFieldsLookup(context));
    }

    public DocLookup doc() {
        return docMap;
    }

    public SourceLookup source() {
        return sourceLookup;
    }
}
