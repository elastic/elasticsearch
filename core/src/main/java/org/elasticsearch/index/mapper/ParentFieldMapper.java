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

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.DocValuesTermsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeMapValue;

public class ParentFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_parent";
    public static final String CONTENT_TYPE = "_parent";

    public static class Defaults {
        public static final String NAME = ParentFieldMapper.NAME;

        public static final ParentFieldType FIELD_TYPE = new ParentFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.setDocValuesType(DocValuesType.SORTED);
            FIELD_TYPE.setEagerGlobalOrdinals(false);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, ParentFieldMapper> {

        private String parentType;

        private final String documentType;

        public Builder(String documentType) {
            super(Defaults.NAME, new ParentFieldType(Defaults.FIELD_TYPE, documentType), Defaults.FIELD_TYPE);
            // Defaults to true
            eagerGlobalOrdinals(true);
            this.documentType = documentType;
            builder = this;
        }

        public Builder type(String type) {
            this.parentType = type;
            return builder;
        }

        public Builder eagerGlobalOrdinals(boolean eagerGlobalOrdinals) {
            fieldType().setEagerGlobalOrdinals(eagerGlobalOrdinals);
            return builder;
        }

        @Override
        public ParentFieldMapper build(BuilderContext context) {
            if (parentType == null) {
                throw new MapperParsingException("[_parent] field mapping must contain the [type] option");
            }
            name = joinField(parentType);
            setupFieldType(context);
            return new ParentFieldMapper(createParentJoinFieldMapper(documentType, context), fieldType, parentType, context.indexSettings());
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        private static final ParseField FIELDDATA = new ParseField("fielddata").withAllDeprecated("eager_global_ordinals");
        @Override
        public MetadataFieldMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(parserContext.type());
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("type")) {
                    builder.type(fieldNode.toString());
                    iterator.remove();
                } else if (FIELDDATA.match(fieldName)) {
                    // for bw compat only
                    Map<String, Object> fieldDataSettings = nodeMapValue(fieldNode, "fielddata");
                    if (fieldDataSettings.containsKey("loading")) {
                        builder.eagerGlobalOrdinals("eager_global_ordinals".equals(fieldDataSettings.get("loading")));
                    }
                    iterator.remove();
                } else if (fieldName.equals("eager_global_ordinals")) {
                    builder.eagerGlobalOrdinals(XContentMapValues.nodeBooleanValue(fieldNode, "eager_global_ordinals"));
                    iterator.remove();
                }
            }
            return builder;
        }

        @Override
        public MetadataFieldMapper getDefault(MappedFieldType fieldType, ParserContext context) {
            final Settings indexSettings = context.mapperService().getIndexSettings().getSettings();
            final String typeName = context.type();
            KeywordFieldMapper parentJoinField = createParentJoinFieldMapper(typeName, new BuilderContext(indexSettings, new ContentPath(0)));
            MappedFieldType childJoinFieldType = new ParentFieldType(Defaults.FIELD_TYPE, typeName);
            childJoinFieldType.setName(ParentFieldMapper.NAME);
            return new ParentFieldMapper(parentJoinField, childJoinFieldType, null, indexSettings);
        }
    }

    static KeywordFieldMapper createParentJoinFieldMapper(String docType, BuilderContext context) {
        KeywordFieldMapper.Builder parentJoinField = new KeywordFieldMapper.Builder(joinField(docType));
        parentJoinField.indexOptions(IndexOptions.NONE);
        parentJoinField.docValues(true);
        parentJoinField.fieldType().setDocValuesType(DocValuesType.SORTED);
        return parentJoinField.build(context);
    }

    static final class ParentFieldType extends MappedFieldType {

        final String documentType;

        ParentFieldType() {
            documentType = null;
            setEagerGlobalOrdinals(true);
        }

        ParentFieldType(ParentFieldType ref, String documentType) {
            super(ref);
            this.documentType = documentType;
        }

        private ParentFieldType(ParentFieldType ref) {
            super(ref);
            this.documentType = ref.documentType;
        }

        @Override
        public MappedFieldType clone() {
            return new ParentFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new DocValuesFieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, @Nullable QueryShardContext context) {
            return termsQuery(Collections.singletonList(value), context);
        }

        @Override
        public Query termsQuery(List values, @Nullable QueryShardContext context) {
            BytesRef[] ids = new BytesRef[values.size()];
            for (int i = 0; i < ids.length; i++) {
                ids[i] = BytesRefs.toBytesRef(values.get(i));
            }
            BooleanQuery.Builder query = new BooleanQuery.Builder();
            query.add(new DocValuesTermsQuery(name(), ids), BooleanClause.Occur.MUST);
            query.add(new TermQuery(new Term(TypeFieldMapper.NAME, documentType)), BooleanClause.Occur.FILTER);
            return query.build();
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            return new DocValuesIndexFieldData.Builder();
        }
    }

    private final String parentType;
    // has no impact of field data settings, is just here for creating a join field,
    // the parent field mapper in the child type pointing to this type determines the field data settings for this join field
    private final KeywordFieldMapper parentJoinField;

    private ParentFieldMapper(KeywordFieldMapper parentJoinField, MappedFieldType childJoinFieldType, String parentType, Settings indexSettings) {
        super(NAME, childJoinFieldType, Defaults.FIELD_TYPE, indexSettings);
        this.parentType = parentType;
        this.parentJoinField = parentJoinField;
    }

    public MappedFieldType getParentJoinFieldType() {
        return parentJoinField.fieldType();
    }

    public String type() {
        return parentType;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        parse(context);
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        boolean parent = context.docMapper().isParent(context.sourceToParse().type());
        if (parent) {
            fields.add(new SortedDocValuesField(parentJoinField.fieldType().name(), new BytesRef(context.sourceToParse().id())));
        }

        if (!active()) {
            return;
        }

        if (context.parser().currentName() != null && context.parser().currentName().equals(Defaults.NAME)) {
            // we are in the parsing of _parent phase
            String parentId = context.parser().text();
            context.sourceToParse().parent(parentId);
            fields.add(new SortedDocValuesField(fieldType.name(), new BytesRef(parentId)));
        } else {
            // otherwise, we are running it post processing of the xcontent
            String parsedParentId = context.doc().get(Defaults.NAME);
            if (context.sourceToParse().parent() != null) {
                String parentId = context.sourceToParse().parent();
                if (parsedParentId == null) {
                    if (parentId == null) {
                        throw new MapperParsingException("No parent id provided, not within the document, and not externally");
                    }
                    // we did not add it in the parsing phase, add it now
                    fields.add(new SortedDocValuesField(fieldType.name(), new BytesRef(parentId)));
                } else if (parentId != null && !parsedParentId.equals(Uid.createUid(parentType, parentId))) {
                    throw new MapperParsingException("Parent id mismatch, document value is [" + Uid.createUid(parsedParentId).id() + "], while external value is [" + parentId + "]");
                }
            }
        }
        // we have parent mapping, yet no value was set, ignore it...
    }

    public static String joinField(String parentType) {
        return ParentFieldMapper.NAME + "#" + parentType;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public Iterator<Mapper> iterator() {
        return Collections.<Mapper>singleton(parentJoinField).iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (!active()) {
            return builder;
        }
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        builder.startObject(CONTENT_TYPE);
        builder.field("type", parentType);
        if (includeDefaults || fieldType().eagerGlobalOrdinals() == false) {
            builder.field("eager_global_ordinals", fieldType().eagerGlobalOrdinals());
        }
        builder.endObject();
        return builder;
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        ParentFieldMapper fieldMergeWith = (ParentFieldMapper) mergeWith;
        ParentFieldType currentFieldType = (ParentFieldType) fieldType.clone();
        super.doMerge(mergeWith, updateAllTypes);
        if (fieldMergeWith.parentType != null && Objects.equals(parentType, fieldMergeWith.parentType) == false) {
            throw new IllegalArgumentException("The _parent field's type option can't be changed: [" + parentType + "]->[" + fieldMergeWith.parentType + "]");
        }

        if (active()) {
            fieldType = currentFieldType;
        }
    }

    /**
     * @return Whether the _parent field is actually configured.
     */
    public boolean active() {
        return parentType != null;
    }

}
