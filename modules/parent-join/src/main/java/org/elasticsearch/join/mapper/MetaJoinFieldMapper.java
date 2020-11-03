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

package org.elasticsearch.join.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.function.Supplier;

/**
 * Simple field mapper hack to ensure that there is a one and only {@link ParentJoinFieldMapper} per mapping.
 * This field mapper is not used to index or query any data, it is used as a marker in the mapping that
 * denotes the presence of a parent-join field and forbids the addition of any additional ones.
 * This class is also used to quickly retrieve the parent-join field defined in a mapping without
 * specifying the name of the field.
 */
public class MetaJoinFieldMapper extends MetadataFieldMapper {

    static final String NAME = "_parent_join";
    static final String CONTENT_TYPE = "parent_join";

    public static class MetaJoinFieldType extends StringFieldType {

        private final String joinField;

        private MetaJoinFieldType(String joinField) {
            super(NAME, false, false, false, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
            this.joinField = joinField;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            throw new UnsupportedOperationException("Cannot load field data for metadata field [" + NAME + "]");
        }

        @Override
        public ValueFetcher valueFetcher(MapperService mapperService, SearchLookup searchLookup, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for metadata field [" + NAME + "].");
        }

        @Override
        public Object valueForDisplay(Object value) {
            throw new UnsupportedOperationException();
        }

        public String getJoinField() {
            return joinField;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new UnsupportedOperationException("Exists query not supported for fields of type" + typeName());
        }
    }

    MetaJoinFieldMapper(String joinField) {
        super(new MetaJoinFieldType(joinField));
    }

    @Override
    public MetaJoinFieldType fieldType() {
        return (MetaJoinFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        throw new IllegalStateException("Should never be called");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
