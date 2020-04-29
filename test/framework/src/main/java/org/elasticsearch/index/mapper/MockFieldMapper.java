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

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.List;

// this sucks how much must be overridden just do get a dummy field mapper...
public class MockFieldMapper extends FieldMapper {
    static Settings dummySettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();

    public MockFieldMapper(String fullName) {
        this(fullName, new FakeFieldType());
    }

    public MockFieldMapper(String fullName, MappedFieldType fieldType) {
        super(findSimpleName(fullName), setName(fullName, fieldType), setName(fullName, fieldType), dummySettings,
            MultiFields.empty(), new CopyTo.Builder().build());
    }

    public MockFieldMapper(String fullName,
                           MappedFieldType fieldType,
                           MultiFields multifields,
                           CopyTo copyTo) {
        super(findSimpleName(fullName), setName(fullName, fieldType), setName(fullName, fieldType), dummySettings,
            multifields, copyTo);
    }

    static MappedFieldType setName(String fullName, MappedFieldType fieldType) {
        fieldType.setName(fullName);
        return fieldType;
    }

    static String findSimpleName(String fullName) {
        int ndx = fullName.lastIndexOf('.');
        return fullName.substring(ndx + 1);
    }

    public static class FakeFieldType extends TermBasedFieldType {
        public FakeFieldType() {
        }

        protected FakeFieldType(FakeFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new FakeFieldType(this);
        }

        @Override
        public String typeName() {
            return "faketype";
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            }
        }
    }

    @Override
    protected String contentType() {
        return null;
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {

    }

    public static class Builder extends FieldMapper.Builder<MockFieldMapper.Builder> {
        protected Builder(String name, MappedFieldType fieldType, MappedFieldType defaultFieldType) {
            super(name, fieldType, defaultFieldType);
            builder = this;
        }

        @Override
        public MockFieldMapper build(BuilderContext context) {
            MultiFields multiFields = multiFieldsBuilder.build(this, context);
            return new MockFieldMapper(name(), fieldType, multiFields, copyTo);
        }
    }
}
