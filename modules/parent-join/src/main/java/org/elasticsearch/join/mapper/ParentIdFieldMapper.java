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
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A field mapper used internally by the {@link ParentJoinFieldMapper} to index
 * the value that link documents in the index (parent _id or _id if the document is a parent).
 */
public final class ParentIdFieldMapper extends FieldMapper {
    static final String CONTENT_TYPE = "parent";

    static class Defaults {
        static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }
    }

    static class Builder extends FieldMapper.Builder<Builder> {
        private final String parent;
        private final Set<String> children;

        Builder(String name, String parent, Set<String> children) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
            this.parent = parent;
            this.children = children;
        }

        public Set<String> getChildren() {
            return children;
        }

        public Builder eagerGlobalOrdinals(boolean eagerGlobalOrdinals) {
            this.eagerGlobalOrdinals = eagerGlobalOrdinals;
            return builder;
        }

        @Override
        public ParentIdFieldMapper build(BuilderContext context) {
            return new ParentIdFieldMapper(name, parent, children, fieldType,
                new ParentIdFieldType(buildFullName(context), eagerGlobalOrdinals, meta));
        }
    }

    public static final class ParentIdFieldType extends StringFieldType {
        ParentIdFieldType(String name, boolean eagerGlobalOrdinals, Map<String, String> meta) {
            super(name, true, true, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            setEagerGlobalOrdinals(eagerGlobalOrdinals);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), CoreValuesSourceType.BYTES);
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
                                  FieldType fieldType,
                                  MappedFieldType mappedFieldType) {
        super(simpleName, fieldType, mappedFieldType, MultiFields.empty(), CopyTo.empty());
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
    protected void parseCreateField(ParseContext context) throws IOException {
        if (context.externalValueSet() == false) {
            throw new IllegalStateException("external value not set");
        }
        String refId = (String) context.externalValue();
        BytesRef binaryValue = new BytesRef(refId);
        Field field = new Field(fieldType().name(), binaryValue, fieldType);
        context.doc().add(field);
        context.doc().add(new SortedDocValuesField(fieldType().name(), binaryValue));
    }

    @Override
    protected Object parseSourceValue(Object value, String format) {
        throw new UnsupportedOperationException("The " + typeName() + " field is not stored in _source.");
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        ParentIdFieldMapper parentMergeWith = (ParentIdFieldMapper) other;
        this.children = parentMergeWith.children;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
