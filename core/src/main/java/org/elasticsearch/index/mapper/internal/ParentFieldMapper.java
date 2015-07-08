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
package org.elasticsearch.index.mapper.internal;

import com.google.common.base.Objects;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.SettingsLoader;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeMapValue;

/**
 *
 */
public class ParentFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_parent";
    public static final String CONTENT_TYPE = "_parent";

    public static class Defaults {
        public static final String NAME = ParentFieldMapper.NAME;

        public static final MappedFieldType FIELD_TYPE = new ParentFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setNames(new MappedFieldType.Names(NAME));
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, ParentFieldMapper> {

        protected String indexName;

        private String type;

        public Builder() {
            super(Defaults.NAME, Defaults.FIELD_TYPE);
            this.indexName = name;
            builder = this;
        }

        public Builder type(String type) {
            this.type = type;
            return builder;
        }

        @Override
        public ParentFieldMapper build(BuilderContext context) {
            if (type == null) {
                throw new MapperParsingException("[_parent] field mapping must contain the [type] option");
            }
            setupFieldType(context);
            fieldType.setHasDocValues(context.indexCreatedVersion().onOrAfter(Version.V_2_0_0));
            return new ParentFieldMapper(fieldType, type, context.indexSettings());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder();
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("type")) {
                    builder.type(fieldNode.toString());
                    iterator.remove();
                } else if (fieldName.equals("postings_format") && parserContext.indexVersionCreated().before(Version.V_2_0_0)) {
                    // ignore before 2.0, reject on and after 2.0
                    iterator.remove();
                } else if (fieldName.equals("fielddata")) {
                    // Only take over `loading`, since that is the only option now that is configurable:
                    Map<String, String> fieldDataSettings = SettingsLoader.Helper.loadNestedFromMap(nodeMapValue(fieldNode, "fielddata"));
                    if (fieldDataSettings.containsKey(MappedFieldType.Loading.KEY)) {
                        Settings settings = settingsBuilder().put(MappedFieldType.Loading.KEY, fieldDataSettings.get(MappedFieldType.Loading.KEY)).build();
                        builder.fieldDataSettings(settings);
                    }
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    static final class ParentFieldType extends MappedFieldType {

        public ParentFieldType() {
            setFieldDataType(new FieldDataType("_parent", settingsBuilder().put(MappedFieldType.Loading.KEY, Loading.EAGER_VALUE)));
        }

        protected ParentFieldType(ParentFieldType ref) {
            super(ref);
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
        public Uid value(Object value) {
            if (value == null) {
                return null;
            }
            return Uid.createUid(value.toString());
        }

        @Override
        public Object valueForSearch(Object value) {
            if (value == null) {
                return null;
            }
            String sValue = value.toString();
            if (sValue == null) {
                return null;
            }
            int index = sValue.indexOf(Uid.DELIMITER);
            if (index == -1) {
                return sValue;
            }
            return sValue.substring(index + 1);
        }

        /**
         * We don't need to analyzer the text, and we need to convert it to UID...
         */
        @Override
        public boolean useTermQueryWithQueryString() {
            return true;
        }

        @Override
        public Query termQuery(Object value, @Nullable QueryParseContext context) {
            return termsQuery(Collections.singletonList(value), context);
        }

        @Override
        public Query termsQuery(List values, @Nullable QueryParseContext context) {
            if (context == null) {
                return super.termsQuery(values, context);
            }

            List<String> types = new ArrayList<>(context.mapperService().types().size());
            for (DocumentMapper documentMapper : context.mapperService().docMappers(false)) {
                if (!documentMapper.parentFieldMapper().active()) {
                    types.add(documentMapper.type());
                }
            }

            List<BytesRef> bValues = new ArrayList<>(values.size());
            for (Object value : values) {
                BytesRef bValue = BytesRefs.toBytesRef(value);
                if (Uid.hasDelimiter(bValue)) {
                    bValues.add(bValue);
                } else {
                    // we use all non child types, cause we don't know if its exact or not...
                    for (String type : types) {
                        bValues.add(Uid.createUidAsBytes(type, bValue));
                    }
                }
            }
            return new TermsQuery(names().indexName(), bValues);
        }
    }

    private final String type;

    protected ParentFieldMapper(MappedFieldType fieldType, String type, Settings indexSettings) {
        super(NAME, setupDocValues(indexSettings, fieldType), setupDocValues(indexSettings, Defaults.FIELD_TYPE), indexSettings);
        this.type = type;
    }

    public ParentFieldMapper(Settings indexSettings, MappedFieldType existing) {
        this(existing == null ? Defaults.FIELD_TYPE.clone() : existing.clone(), null, indexSettings);
    }

    static MappedFieldType setupDocValues(Settings indexSettings, MappedFieldType fieldType) {
        fieldType = fieldType.clone();
        fieldType.setHasDocValues(Version.indexCreated(indexSettings).onOrAfter(Version.V_2_0_0));
        return fieldType;
    }

    public String type() {
        return type;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        parse(context);
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        boolean parent = context.docMapper().isParent(context.type());
        if (parent && fieldType().hasDocValues()) {
            fields.add(createJoinField(context.type(), context.id()));
        }

        if (!active()) {
            return;
        }

        if (context.parser().currentName() != null && context.parser().currentName().equals(Defaults.NAME)) {
            // we are in the parsing of _parent phase
            String parentId = context.parser().text();
            context.sourceToParse().parent(parentId);
            fields.add(new Field(fieldType().names().indexName(), Uid.createUid(context.stringBuilder(), type, parentId), fieldType()));
            if (fieldType().hasDocValues()) {
                fields.add(createJoinField(type, parentId));
            }
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
                    fields.add(new Field(fieldType().names().indexName(), Uid.createUid(context.stringBuilder(), type, parentId), fieldType()));
                    if (fieldType().hasDocValues()) {
                        fields.add(createJoinField(type, parentId));
                    }
                } else if (parentId != null && !parsedParentId.equals(Uid.createUid(context.stringBuilder(), type, parentId))) {
                    throw new MapperParsingException("Parent id mismatch, document value is [" + Uid.createUid(parsedParentId).id() + "], while external value is [" + parentId + "]");
                }
            }
        }
        // we have parent mapping, yet no value was set, ignore it...
    }

    private SortedDocValuesField createJoinField(String parentType, String id) {
        String joinField = joinField(parentType);
        return new SortedDocValuesField(joinField, new BytesRef(id));
    }

    public static String joinField(String parentType) {
        return ParentFieldMapper.NAME + "#" + parentType;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (!active()) {
            return builder;
        }
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        builder.startObject(CONTENT_TYPE);
        builder.field("type", type);
        if (includeDefaults || hasCustomFieldDataSettings()) {
            builder.field("fielddata", (Map) fieldType().fieldDataType().getSettings().getAsMap());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        super.merge(mergeWith, mergeResult);
        ParentFieldMapper fieldMergeWith = (ParentFieldMapper) mergeWith;
        if (Objects.equal(type, fieldMergeWith.type) == false) {
            mergeResult.addConflict("The _parent field's type option can't be changed: [" + type + "]->[" + fieldMergeWith.type + "]");
        }
    }

    /**
     * @return Whether the _parent field is actually configured.
     */
    public boolean active() {
        return type != null;
    }

}
