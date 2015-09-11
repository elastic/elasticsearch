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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DocValuesType;
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
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
        public static final MappedFieldType JOIN_FIELD_TYPE = new ParentFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            FIELD_TYPE.setNames(new MappedFieldType.Names(NAME));
            FIELD_TYPE.freeze();

            JOIN_FIELD_TYPE.setHasDocValues(true);
            JOIN_FIELD_TYPE.setDocValuesType(DocValuesType.SORTED);
            JOIN_FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, ParentFieldMapper> {

        private String parentType;

        protected String indexName;

        private final String documentType;

        private final MappedFieldType parentJoinFieldType = Defaults.JOIN_FIELD_TYPE.clone();

        private final MappedFieldType childJoinFieldType = Defaults.JOIN_FIELD_TYPE.clone();

        public Builder(String documentType) {
            super(Defaults.NAME, Defaults.FIELD_TYPE);
            this.indexName = name;
            this.documentType = documentType;
            builder = this;
        }

        public Builder type(String type) {
            this.parentType = type;
            return builder;
        }

        @Override
        public Builder fieldDataSettings(Settings fieldDataSettings) {
            Settings settings = Settings.builder().put(childJoinFieldType.fieldDataType().getSettings()).put(fieldDataSettings).build();
            childJoinFieldType.setFieldDataType(new FieldDataType(childJoinFieldType.fieldDataType().getType(), settings));
            return this;
        }

        @Override
        public ParentFieldMapper build(BuilderContext context) {
            if (parentType == null) {
                throw new MapperParsingException("[_parent] field mapping must contain the [type] option");
            }
            parentJoinFieldType.setNames(new MappedFieldType.Names(joinField(documentType)));
            parentJoinFieldType.setFieldDataType(null);
            childJoinFieldType.setNames(new MappedFieldType.Names(joinField(parentType)));
            return new ParentFieldMapper(fieldType, parentJoinFieldType, childJoinFieldType, parentType, context.indexSettings());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(parserContext.type());
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("type")) {
                    builder.type(fieldNode.toString());
                    iterator.remove();
                } else if (fieldName.equals("postings_format") && parserContext.indexVersionCreated().before(Version.V_2_0_0_beta1)) {
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

    private final String parentType;
    // determines the field data settings
    private MappedFieldType childJoinFieldType;
    // has no impact of field data settings, is just here for creating a join field, the parent field mapper in the child type pointing to this type determines the field data settings for this join field
    private final MappedFieldType parentJoinFieldType;

    protected ParentFieldMapper(MappedFieldType fieldType, MappedFieldType parentJoinFieldType, MappedFieldType childJoinFieldType, String parentType, Settings indexSettings) {
        super(NAME, fieldType, Defaults.FIELD_TYPE, indexSettings);
        this.parentType = parentType;
        this.parentJoinFieldType = parentJoinFieldType;
        this.parentJoinFieldType.freeze();
        this.childJoinFieldType = childJoinFieldType;
        if (childJoinFieldType != null) {
            this.childJoinFieldType.freeze();
        }
    }

    public ParentFieldMapper(Settings indexSettings, MappedFieldType existing, String parentType) {
        this(existing == null ? Defaults.FIELD_TYPE.clone() : existing.clone(), joinFieldTypeForParentType(parentType, indexSettings), null, null, indexSettings);
    }

    private static MappedFieldType joinFieldTypeForParentType(String parentType, Settings indexSettings) {
        MappedFieldType parentJoinFieldType = Defaults.JOIN_FIELD_TYPE.clone();
        parentJoinFieldType.setNames(new MappedFieldType.Names(joinField(parentType)));
        parentJoinFieldType.freeze();
        return parentJoinFieldType;
    }

    public MappedFieldType getParentJoinFieldType() {
        return parentJoinFieldType;
    }

    public MappedFieldType getChildJoinFieldType() {
        return childJoinFieldType;
    }

    public String type() {
        return parentType;
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        if (context.sourceToParse().flyweight() == false) {
            parse(context);
        }
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        boolean parent = context.docMapper().isParent(context.type());
        if (parent) {
            addJoinFieldIfNeeded(fields, parentJoinFieldType, context.id());
        }

        if (!active()) {
            return;
        }

        if (context.parser().currentName() != null && context.parser().currentName().equals(Defaults.NAME)) {
            // we are in the parsing of _parent phase
            String parentId = context.parser().text();
            context.sourceToParse().parent(parentId);
            fields.add(new Field(fieldType().names().indexName(), Uid.createUid(context.stringBuilder(), parentType, parentId), fieldType()));
            addJoinFieldIfNeeded(fields, childJoinFieldType, parentId);
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
                    fields.add(new Field(fieldType().names().indexName(), Uid.createUid(context.stringBuilder(), parentType, parentId), fieldType()));
                    addJoinFieldIfNeeded(fields, childJoinFieldType, parentId);
                } else if (parentId != null && !parsedParentId.equals(Uid.createUid(context.stringBuilder(), parentType, parentId))) {
                    throw new MapperParsingException("Parent id mismatch, document value is [" + Uid.createUid(parsedParentId).id() + "], while external value is [" + parentId + "]");
                }
            }
        }
        // we have parent mapping, yet no value was set, ignore it...
    }

    private void addJoinFieldIfNeeded(List<Field> fields, MappedFieldType fieldType, String id) {
        if (fieldType.hasDocValues()) {
            fields.add(new SortedDocValuesField(fieldType.names().indexName(), new BytesRef(id)));
        }
    }

    public static String joinField(String parentType) {
        return ParentFieldMapper.NAME + "#" + parentType;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    private boolean joinFieldHasCustomFieldDataSettings() {
        return childJoinFieldType != null && childJoinFieldType.fieldDataType() != null && childJoinFieldType.fieldDataType().equals(Defaults.JOIN_FIELD_TYPE.fieldDataType()) == false;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (!active()) {
            return builder;
        }
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        builder.startObject(CONTENT_TYPE);
        builder.field("type", parentType);
        if (includeDefaults || joinFieldHasCustomFieldDataSettings()) {
            builder.field("fielddata", (Map) childJoinFieldType.fieldDataType().getSettings().getAsMap());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        super.merge(mergeWith, mergeResult);
        ParentFieldMapper fieldMergeWith = (ParentFieldMapper) mergeWith;
        if (Objects.equals(parentType, fieldMergeWith.parentType) == false) {
            mergeResult.addConflict("The _parent field's type option can't be changed: [" + parentType + "]->[" + fieldMergeWith.parentType + "]");
        }

        List<String> conflicts = new ArrayList<>();
        fieldType().checkCompatibility(fieldMergeWith.fieldType(), conflicts, true); // always strict, this cannot change
        parentJoinFieldType.checkCompatibility(fieldMergeWith.parentJoinFieldType, conflicts, true); // same here
        if (childJoinFieldType != null) {
            // TODO: this can be set to false when the old parent/child impl is removed, we can do eager global ordinals loading per type.
            childJoinFieldType.checkCompatibility(fieldMergeWith.childJoinFieldType, conflicts, mergeResult.updateAllTypes() == false);
        }
        for (String conflict : conflicts) {
            mergeResult.addConflict(conflict);
        }

        if (active() && mergeResult.simulate() == false && mergeResult.hasConflicts() == false) {
            childJoinFieldType = fieldMergeWith.childJoinFieldType.clone();
        }
    }

    /**
     * @return Whether the _parent field is actually configured.
     */
    public boolean active() {
        return parentType != null;
    }

}
