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
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.SettingsLoader;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.InternalMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.RootMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.builder;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeMapValue;
import static org.elasticsearch.index.mapper.MapperBuilders.parent;

/**
 *
 */
public class ParentFieldMapper extends AbstractFieldMapper<Uid> implements InternalMapper, RootMapper {

    public static final String NAME = "_parent";

    public static final String CONTENT_TYPE = "_parent";


    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final String NAME = ParentFieldMapper.NAME;

        public static final FieldType FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends Mapper.Builder<Builder, ParentFieldMapper> {

        protected String indexName;

        private String type;
        protected Settings fieldDataSettings;

        public Builder() {
            super(Defaults.NAME);
            this.indexName = name;
            builder = this;
        }

        public Builder type(String type) {
            this.type = type;
            return builder;
        }

        public Builder fieldDataSettings(Settings settings) {
            this.fieldDataSettings = settings;
            return builder;
        }

        @Override
        public ParentFieldMapper build(BuilderContext context) {
            if (type == null) {
                throw new MapperParsingException("Parent mapping must contain the parent type");
            }
            return new ParentFieldMapper(name, indexName, type, fieldDataSettings, context.indexSettings());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            ParentFieldMapper.Builder builder = parent();
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
                    if (fieldDataSettings.containsKey(Loading.KEY)) {
                        Settings settings = settingsBuilder().put(Loading.KEY, fieldDataSettings.get(Loading.KEY)).build();
                        builder.fieldDataSettings(settings);
                    }
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    private final String type;
    private final BytesRef typeAsBytes;

    protected ParentFieldMapper(String name, String indexName, String type, @Nullable Settings fieldDataSettings, Settings indexSettings) {
        super(new Names(name, indexName, indexName, name), Defaults.BOOST, new FieldType(Defaults.FIELD_TYPE), false,
                Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER, null, null, fieldDataSettings, indexSettings);
        this.type = type;
        this.typeAsBytes = type == null ? null : new BytesRef(type);
    }

    public ParentFieldMapper(Settings indexSettings) {
        this(Defaults.NAME, Defaults.NAME, null, null, indexSettings);
        this.fieldDataType = new FieldDataType("_parent", settingsBuilder().put(Loading.KEY, Loading.LAZY_VALUE));
    }

    public String type() {
        return type;
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("_parent", settingsBuilder().put(Loading.KEY, Loading.EAGER_VALUE));
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        parse(context);
    }

    @Override
    public boolean includeInObject() {
        return true;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        if (!active()) {
            return;
        }

        if (context.parser().currentName() != null && context.parser().currentName().equals(Defaults.NAME)) {
            // we are in the parsing of _parent phase
            String parentId = context.parser().text();
            context.sourceToParse().parent(parentId);
            fields.add(new Field(names.indexName(), Uid.createUid(context.stringBuilder(), type, parentId), fieldType));
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
                    fields.add(new Field(names.indexName(), Uid.createUid(context.stringBuilder(), type, parentId), fieldType));
                } else if (parentId != null && !parsedParentId.equals(Uid.createUid(context.stringBuilder(), type, parentId))) {
                    throw new MapperParsingException("Parent id mismatch, document value is [" + Uid.createUid(parsedParentId).id() + "], while external value is [" + parentId + "]");
                }
            }
        }
        // we have parent mapping, yet no value was set, ignore it...
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

    @Override
    public BytesRef indexedValueForSearch(Object value) {
        if (value instanceof BytesRef) {
            BytesRef bytesRef = (BytesRef) value;
            if (Uid.hasDelimiter(bytesRef)) {
                return bytesRef;
            }
            return Uid.createUidAsBytes(typeAsBytes, bytesRef);
        }
        String sValue = value.toString();
        if (sValue.indexOf(Uid.DELIMITER) == -1) {
            return Uid.createUidAsBytes(type, sValue);
        }
        return super.indexedValueForSearch(value);
    }

    @Override
    public Query termQuery(Object value, @Nullable QueryParseContext context) {
        if (context == null) {
            return super.termQuery(value, context);
        }
        return new ConstantScoreQuery(termFilter(value, context));
    }

    @Override
    public Filter termFilter(Object value, @Nullable QueryParseContext context) {
        if (context == null) {
            return super.termFilter(value, context);
        }
        BytesRef bValue = BytesRefs.toBytesRef(value);
        if (Uid.hasDelimiter(bValue)) {
            return Queries.wrap(new TermQuery(new Term(names.indexName(), bValue)));
        }

        List<String> types = new ArrayList<>(context.mapperService().types().size());
        for (DocumentMapper documentMapper : context.mapperService().docMappers(false)) {
            if (!documentMapper.parentFieldMapper().active()) {
                types.add(documentMapper.type());
            }
        }

        if (types.isEmpty()) {
            return Queries.newMatchNoDocsFilter();
        } else if (types.size() == 1) {
            return Queries.wrap(new TermQuery(new Term(names.indexName(), Uid.createUidAsBytes(types.get(0), bValue))));
        } else {
            // we use all non child types, cause we don't know if its exact or not...
            List<BytesRef> typesValues = new ArrayList<>(types.size());
            for (String type : context.mapperService().types()) {
                typesValues.add(Uid.createUidAsBytes(type, bValue));
            }
            return Queries.wrap(new TermsQuery(names.indexName(), typesValues));
        }
    }

    @Override
    public Filter termsFilter(List values, @Nullable QueryParseContext context) {
        if (context == null) {
            return super.termsFilter(values, context);
        }
        // This will not be invoked if values is empty, so don't check for empty
        if (values.size() == 1) {
            return termFilter(values.get(0), context);
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
        return Queries.wrap(new TermsQuery(names.indexName(), bValues));
    }

    /**
     * We don't need to analyzer the text, and we need to convert it to UID...
     */
    @Override
    public boolean useTermQueryWithQueryString() {
        return true;
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
        if (customFieldDataSettings != null) {
            builder.field("fielddata", (Map) customFieldDataSettings.getAsMap());
        } else if (includeDefaults) {
            builder.field("fielddata", (Map) fieldDataType.getSettings().getAsMap());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        ParentFieldMapper other = (ParentFieldMapper) mergeWith;
        if (!Objects.equal(type, other.type)) {
            mergeResult.addConflict("The _parent field's type option can't be changed: [" + type + "]->[" + other.type + "]");
        }

        if (!mergeResult.simulate()) {
            ParentFieldMapper fieldMergeWith = (ParentFieldMapper) mergeWith;
            if (fieldMergeWith.customFieldDataSettings != null) {
                if (!Objects.equal(fieldMergeWith.customFieldDataSettings, this.customFieldDataSettings)) {
                    this.customFieldDataSettings = fieldMergeWith.customFieldDataSettings;
                    this.fieldDataType = new FieldDataType(defaultFieldDataType().getType(),
                            builder().put(defaultFieldDataType().getSettings()).put(this.customFieldDataSettings)
                    );
                }
            }
        }
    }

    /**
     * @return Whether the _parent field is actually used.
     */
    public boolean active() {
        return type != null;
    }

}
