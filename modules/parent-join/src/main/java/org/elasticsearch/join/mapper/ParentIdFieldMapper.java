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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * A field mapper used internally by the {@link ParentJoinFieldMapper} to index
 * the value that link documents in the index (parent _id or _id if the document is a parent).
 */
public final class ParentIdFieldMapper extends FieldMapper {
    static final String CONTENT_TYPE = "parent";

    static class Defaults {
        static final MappedFieldType FIELD_TYPE = new ParentIdFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }
    }

    static class Builder extends FieldMapper.Builder<Builder, ParentIdFieldMapper> {
        private final String parent;
        private final Set<String> children;

        Builder(String name, String parent, Set<String> children) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
            this.parent = parent;
            this.children = children;
        }

        public Set<String> getChildren() {
            return children;
        }

        public Builder eagerGlobalOrdinals(boolean eagerGlobalOrdinals) {
            fieldType().setEagerGlobalOrdinals(eagerGlobalOrdinals);
            return builder;
        }

        @Override
        public ParentIdFieldMapper build(BuilderContext context) {
            fieldType.setName(name);
            return new ParentIdFieldMapper(name, parent, children, fieldType, context.indexSettings());
        }
    }

    public static final class ParentIdFieldType extends StringFieldType {
        ParentIdFieldType() {
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
        }

        protected ParentIdFieldType(ParentIdFieldType ref) {
            super(ref);
        }

        public ParentIdFieldType clone() {
            return new ParentIdFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new DocValuesIndexFieldData.Builder();
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            BytesRef binaryValue = (BytesRef) value;
            return binaryValue.utf8ToString();
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new DocValuesFieldExistsQuery(name());
        }
    }

    private final String parentName;
    private Set<String> children;

    protected ParentIdFieldMapper(String simpleName,
                                  String parentName,
                                  Set<String> children,
                                  MappedFieldType fieldType,
                                  Settings indexSettings) {
        super(simpleName, fieldType, Defaults.FIELD_TYPE, indexSettings, MultiFields.empty(), CopyTo.empty());
        this.parentName = parentName;
        this.children = children;
    }

    @Override
    protected ParentIdFieldMapper clone() {
        return (ParentIdFieldMapper) super.clone();
    }

    /**
     * Returns the parent name associated with this mapper.
     */
    public String getParentName() {
        return parentName;
    }

    public Query getParentFilter() {
        return new TermQuery(new Term(name().substring(0, name().indexOf('#')), parentName));
    }
    /**
     * Returns the children names associated with this mapper.
     */
    public Collection<String> getChildren() {
        return children;
    }

    public Query getChildFilter(String type) {
        return new TermQuery(new Term(name().substring(0, name().indexOf('#')), type));
    }

    public Query getChildrenFilter() {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (String child : children) {
            builder.add(getChildFilter(child), BooleanClause.Occur.SHOULD);
        }
        return new ConstantScoreQuery(builder.build());
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        if (context.externalValueSet() == false) {
            throw new IllegalStateException("external value not set");
        }
        String refId = (String) context.externalValue();
        BytesRef binaryValue = new BytesRef(refId);
        Field field = new Field(fieldType().name(), binaryValue, fieldType());
        fields.add(field);
        fields.add(new SortedDocValuesField(fieldType().name(), binaryValue));
    }


    @Override
    protected void doMerge(Mapper mergeWith) {
        super.doMerge(mergeWith);
        ParentIdFieldMapper parentMergeWith = (ParentIdFieldMapper) mergeWith;
        this.children = parentMergeWith.children;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
