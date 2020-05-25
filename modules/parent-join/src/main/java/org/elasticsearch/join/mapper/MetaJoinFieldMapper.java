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

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.List;

/**
 * Simple field mapper hack to ensure that there is a one and only {@link ParentJoinFieldMapper} per mapping.
 * This field mapper is not used to index or query any data, it is used as a marker in the mapping that
 * denotes the presence of a parent-join field and forbids the addition of any additional ones.
 * This class is also used to quickly retrieve the parent-join field defined in a mapping without
 * specifying the name of the field.
 */
public class MetaJoinFieldMapper extends FieldMapper {
    static final String NAME = "_parent_join";
    static final String CONTENT_TYPE = "parent_join";

    static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new MetaJoinFieldType();

        static {
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setHasDocValues(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.freeze();
        }
    }

    static class Builder extends FieldMapper.Builder<Builder> {
        Builder() {
            super(NAME, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public MetaJoinFieldMapper build(BuilderContext context) {
            fieldType.setName(NAME);
            return new MetaJoinFieldMapper(name, fieldType, context.indexSettings());
        }
    }

    public static class MetaJoinFieldType extends StringFieldType {

        private ParentJoinFieldMapper mapper;

        MetaJoinFieldType() {}

        protected MetaJoinFieldType(MetaJoinFieldType ref) {
            super(ref);
        }

        public MetaJoinFieldType clone() {
            return new MetaJoinFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder();
        }

        @Override
        public ValuesSourceType getValuesSourceType() {
            return CoreValuesSourceType.BYTES;
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            BytesRef binaryValue = (BytesRef) value;
            return binaryValue.utf8ToString();
        }

        public ParentJoinFieldMapper getMapper() {
            return mapper;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new UnsupportedOperationException("Exists query not supported for fields of type" + typeName());
        }
    }

    MetaJoinFieldMapper(String name, MappedFieldType fieldType, Settings indexSettings) {
        super(name, fieldType, ParentIdFieldMapper.Defaults.FIELD_TYPE, indexSettings, MultiFields.empty(), CopyTo.empty());
    }

    void setFieldMapper(ParentJoinFieldMapper mapper) {
        fieldType().mapper = mapper;
    }

    @Override
    public MetaJoinFieldType fieldType() {
        return (MetaJoinFieldType) super.fieldType();
    }

    @Override
    protected MetaJoinFieldMapper clone() {
        return (MetaJoinFieldMapper) super.clone();
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {

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
