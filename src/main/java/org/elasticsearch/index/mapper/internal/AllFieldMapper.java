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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.all.AllField;
import org.elasticsearch.common.lucene.all.AllTermQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.InternalMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.RootMapper;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.similarity.SimilarityLookupService;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeMapValue;
import static org.elasticsearch.index.mapper.MapperBuilders.all;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

/**
 *
 */
public class AllFieldMapper extends AbstractFieldMapper<String> implements InternalMapper, RootMapper {

    public interface IncludeInAll extends Mapper {

        void includeInAll(Boolean includeInAll);

        void includeInAllIfNotSet(Boolean includeInAll);

        void unsetIncludeInAll();
    }

    public static final String NAME = "_all";

    public static final String CONTENT_TYPE = "_all";

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final String NAME = AllFieldMapper.NAME;
        public static final String INDEX_NAME = AllFieldMapper.NAME;
        public static final EnabledAttributeMapper ENABLED = EnabledAttributeMapper.UNSET_ENABLED;

        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            FIELD_TYPE.setTokenized(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, AllFieldMapper> {

        private EnabledAttributeMapper enabled = Defaults.ENABLED;

        public Builder() {
            super(Defaults.NAME, new FieldType(Defaults.FIELD_TYPE));
            builder = this;
            indexName = Defaults.INDEX_NAME;
        }

        public Builder enabled(EnabledAttributeMapper enabled) {
            this.enabled = enabled;
            return this;
        }

        @Override
        public AllFieldMapper build(BuilderContext context) {
            // In case the mapping overrides these
            // TODO: this should be an exception! it doesnt make sense to not index this field
            if (fieldType.indexOptions() == IndexOptions.NONE) {
                fieldType.setIndexOptions(Defaults.FIELD_TYPE.indexOptions());
            }
            fieldType.setTokenized(true);

            return new AllFieldMapper(name, fieldType, indexAnalyzer, searchAnalyzer, enabled, similarity, normsLoading, fieldDataSettings, context.indexSettings());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            AllFieldMapper.Builder builder = all();
            
            // parseField below will happily parse the doc_values setting, but it is then never passed to
            // the AllFieldMapper ctor in the builder since it is not valid. Here we validate
            // the doc values settings (old and new) are rejected
            Object docValues = node.get("doc_values");
            if (docValues != null && nodeBooleanValue(docValues)) {
                throw new MapperParsingException("Field [" + name + "] is always tokenized and cannot have doc values");
            }
            // convoluted way of specifying doc values
            Object fielddata = node.get("fielddata");
            if (fielddata != null) {
                Map<String, Object> fielddataMap = nodeMapValue(fielddata, "fielddata");
                Object format = fielddataMap.get("format");
                if ("doc_values".equals(format)) {
                    throw new MapperParsingException("Field [" + name + "] is always tokenized and cannot have doc values");
                }
            }
            
            parseField(builder, builder.name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    builder.enabled(nodeBooleanValue(fieldNode) ? EnabledAttributeMapper.ENABLED : EnabledAttributeMapper.DISABLED);
                    iterator.remove();
                } else if (fieldName.equals("auto_boost") && parserContext.indexVersionCreated().before(Version.V_2_0_0)) {
                    // Old 1.x setting which is now ignored
                    iterator.remove();
                }
            }
            return builder;
        }
    }


    private EnabledAttributeMapper enabledState;

    public AllFieldMapper(Settings indexSettings) {
        this(Defaults.NAME, new FieldType(Defaults.FIELD_TYPE), null, null, Defaults.ENABLED, null, null, null, indexSettings);
    }

    protected AllFieldMapper(String name, FieldType fieldType, NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer,
                             EnabledAttributeMapper enabled, SimilarityProvider similarity, Loading normsLoading,
                             @Nullable Settings fieldDataSettings, Settings indexSettings) {
        super(new Names(name, name, name, name), 1.0f, fieldType, false, indexAnalyzer, searchAnalyzer,
                similarity, normsLoading, fieldDataSettings, indexSettings);
        this.enabledState = enabled;

    }

    public boolean enabled() {
        return this.enabledState.enabled;
    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return new FieldDataType("string");
    }

    @Override
    public Query queryStringTermQuery(Term term) {
        return new AllTermQuery(term);
    }

    @Override
    public Query termQuery(Object value, QueryParseContext context) {
        return queryStringTermQuery(names().createIndexNameTerm(indexedValueForSearch(value)));
    }

    @Override
    public void preParse(ParseContext context) throws IOException {
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        // we parse in post parse
        return null;
    }

    @Override
    public boolean includeInObject() {
        return true;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        if (!enabledState.enabled) {
            return;
        }
        // reset the entries
        context.allEntries().reset();
        Analyzer analyzer = findAnalyzer(context);
        fields.add(new AllField(names.indexName(), context.allEntries(), analyzer, fieldType));
    }

    private Analyzer findAnalyzer(ParseContext context) {
        Analyzer analyzer = indexAnalyzer;
        if (analyzer == null) {
            analyzer = context.docMapper().mappers().indexAnalyzer();
            if (analyzer == null) {
                // This should not happen, should we log warn it?
                analyzer = Lucene.STANDARD_ANALYZER;
            }
        }
        return analyzer;
    }
    
    @Override
    public String value(Object value) {
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);
        if (!includeDefaults) {
            // simulate the generation to make sure we don't add unnecessary content if all is default
            // if all are defaults, no need to write it at all - generating is twice is ok though
            BytesStreamOutput bytesStreamOutput = new BytesStreamOutput(0);
            XContentBuilder b =  new XContentBuilder(builder.contentType().xContent(), bytesStreamOutput);
            b.startObject().flush();
            long pos = bytesStreamOutput.position();
            innerToXContent(b, false);
            b.flush();
            if (pos == bytesStreamOutput.position()) {
                return builder;
            }
        }
        builder.startObject(CONTENT_TYPE);
        innerToXContent(builder, includeDefaults);
        builder.endObject();
        return builder;
    }

    private void innerToXContent(XContentBuilder builder, boolean includeDefaults) throws IOException {
        if (includeDefaults || enabledState != Defaults.ENABLED) {
            builder.field("enabled", enabledState.enabled);
        }
        if (includeDefaults || fieldType.stored() != Defaults.FIELD_TYPE.stored()) {
            builder.field("store", fieldType.stored());
        }
        if (includeDefaults || fieldType.storeTermVectors() != Defaults.FIELD_TYPE.storeTermVectors()) {
            builder.field("store_term_vectors", fieldType.storeTermVectors());
        }
        if (includeDefaults || fieldType.storeTermVectorOffsets() != Defaults.FIELD_TYPE.storeTermVectorOffsets()) {
            builder.field("store_term_vector_offsets", fieldType.storeTermVectorOffsets());
        }
        if (includeDefaults || fieldType.storeTermVectorPositions() != Defaults.FIELD_TYPE.storeTermVectorPositions()) {
            builder.field("store_term_vector_positions", fieldType.storeTermVectorPositions());
        }
        if (includeDefaults || fieldType.storeTermVectorPayloads() != Defaults.FIELD_TYPE.storeTermVectorPayloads()) {
            builder.field("store_term_vector_payloads", fieldType.storeTermVectorPayloads());
        }
        if (includeDefaults || fieldType.omitNorms() != Defaults.FIELD_TYPE.omitNorms()) {
            builder.field("omit_norms", fieldType.omitNorms());
        }
        
        doXContentAnalyzers(builder, includeDefaults);

        if (similarity() != null) {
            builder.field("similarity", similarity().name());
        } else if (includeDefaults) {
            builder.field("similarity", SimilarityLookupService.DEFAULT_SIMILARITY);
        }

        if (customFieldDataSettings != null) {
            builder.field("fielddata", (Map) customFieldDataSettings.getAsMap());
        } else if (includeDefaults) {
            builder.field("fielddata", (Map) fieldDataType.getSettings().getAsMap());
        }
    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        if (((AllFieldMapper)mergeWith).enabled() != this.enabled() && ((AllFieldMapper)mergeWith).enabledState != Defaults.ENABLED) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] enabled is " + this.enabled() + " now encountering "+ ((AllFieldMapper)mergeWith).enabled());
        }
        super.merge(mergeWith, mergeResult);
    }

    @Override
    public boolean isGenerated() {
        return true;
    }
}
