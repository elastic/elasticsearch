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

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.all.AllEntries;
import org.elasticsearch.common.lucene.all.AllField;
import org.elasticsearch.common.lucene.all.AllTermQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.similarity.SimilarityService;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeMapValue;
import static org.elasticsearch.index.mapper.TypeParsers.parseTextField;

public class AllFieldMapper extends MetadataFieldMapper {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(Loggers.getLogger(AllFieldMapper.class));

    public static final String NAME = "_all";

    public static final String CONTENT_TYPE = "_all";

    public static class Defaults {
        public static final String NAME = AllFieldMapper.NAME;
        public static final String INDEX_NAME = AllFieldMapper.NAME;
        public static final EnabledAttributeMapper ENABLED = EnabledAttributeMapper.UNSET_DISABLED;
        public static final int POSITION_INCREMENT_GAP = 100;

        public static final MappedFieldType FIELD_TYPE = new AllFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            FIELD_TYPE.setTokenized(true);
            FIELD_TYPE.setName(NAME);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends MetadataFieldMapper.Builder<Builder, AllFieldMapper> {

        private EnabledAttributeMapper enabled = Defaults.ENABLED;

        public Builder(MappedFieldType existing) {
            super(Defaults.NAME, existing == null ? Defaults.FIELD_TYPE : existing, Defaults.FIELD_TYPE);
            builder = this;
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
            } else {
                fieldType.setIndexAnalyzer(new NamedAnalyzer(fieldType.indexAnalyzer(),
                    Defaults.POSITION_INCREMENT_GAP));
                fieldType.setSearchAnalyzer(new NamedAnalyzer(fieldType.searchAnalyzer(),
                    Defaults.POSITION_INCREMENT_GAP));
                fieldType.setSearchQuoteAnalyzer(new NamedAnalyzer(fieldType.searchQuoteAnalyzer(),
                    Defaults.POSITION_INCREMENT_GAP));
            }
            fieldType.setTokenized(true);

            return new AllFieldMapper(fieldType, enabled, context.indexSettings());
        }
    }

    public static class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?,?> parse(String name, Map<String, Object> node,
                                                 ParserContext parserContext) throws MapperParsingException {
            if (node.isEmpty() == false &&
                    parserContext.indexVersionCreated().onOrAfter(Version.V_6_0_0_alpha1)) {
                deprecationLogger.deprecated("[_all] is deprecated in 6.0+ and will be removed in 7.0. " +
                                "As a replacement, you can use [copy_to] on " +
                                "mapping fields to create your own catch all field.");
            }
            Builder builder = new Builder(parserContext.mapperService().fullName(NAME));
            builder.fieldType().setIndexAnalyzer(parserContext.getIndexAnalyzers().getDefaultIndexAnalyzer());
            builder.fieldType().setSearchAnalyzer(parserContext.getIndexAnalyzers().getDefaultSearchAnalyzer());
            builder.fieldType().setSearchQuoteAnalyzer(parserContext.getIndexAnalyzers().getDefaultSearchQuoteAnalyzer());

            // parseField below will happily parse the doc_values setting, but it is then never passed to
            // the AllFieldMapper ctor in the builder since it is not valid. Here we validate
            // the doc values settings (old and new) are rejected
            Object docValues = node.get("doc_values");
            if (docValues != null && TypeParsers.nodeBooleanValueLenient(name, "doc_values", docValues)) {
                throw new MapperParsingException("Field [" + name +
                    "] is always tokenized and cannot have doc values");
            }
            // convoluted way of specifying doc values
            Object fielddata = node.get("fielddata");
            if (fielddata != null) {
                Map<String, Object> fielddataMap = nodeMapValue(fielddata, "fielddata");
                Object format = fielddataMap.get("format");
                if ("doc_values".equals(format)) {
                    throw new MapperParsingException("Field [" + name +
                        "] is always tokenized and cannot have doc values");
                }
            }

            parseTextField(builder, builder.name, node, parserContext);
            boolean enabledSet = false;
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    boolean enabled = TypeParsers.nodeBooleanValueLenient(name, "enabled", fieldNode);
                    if (enabled && node.isEmpty() == false &&
                            parserContext.indexVersionCreated().onOrAfter(Version.V_6_0_0_alpha1)) {
                        throw new IllegalArgumentException("Enabling [_all] is disabled in 6.0. " +
                                        "As a replacement, you can use [copy_to] " +
                                        "on mapping fields to create your own catch all field.");
                    }
                    builder.enabled(enabled ? EnabledAttributeMapper.ENABLED : EnabledAttributeMapper.DISABLED);
                    enabledSet = true;
                    iterator.remove();
                }
            }
            if (enabledSet == false && parserContext.indexVersionCreated().before(Version.V_6_0_0_alpha1)) {
                // So there is no "enabled" field, however, the index was created prior to 6.0,
                // and therefore the default for this particular index should be "true" for
                // enabling _all
                builder.enabled(EnabledAttributeMapper.ENABLED);
            }
            return builder;
        }

        @Override
        public MetadataFieldMapper getDefault(MappedFieldType fieldType, ParserContext context) {
            final Settings indexSettings = context.mapperService().getIndexSettings().getSettings();
            if (fieldType != null) {
                if (context.indexVersionCreated().before(Version.V_6_0_0_alpha1)) {
                    // The index was created prior to 6.0, and therefore the default for this
                    // particular index should be "true" for enabling _all
                    return new AllFieldMapper(fieldType.clone(), EnabledAttributeMapper.ENABLED, indexSettings);
                } else {
                    return new AllFieldMapper(indexSettings, fieldType);
                }
            } else {
                return parse(NAME, Collections.emptyMap(), context)
                        .build(new BuilderContext(indexSettings, new ContentPath(1)));
            }
        }
    }

    static final class AllFieldType extends StringFieldType {

        AllFieldType() {
        }

        protected AllFieldType(AllFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new AllFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query queryStringTermQuery(Term term) {
            return new AllTermQuery(term);
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            return queryStringTermQuery(new Term(name(), indexedValueForSearch(value)));
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
        }
    }

    private EnabledAttributeMapper enabledState;

    private AllFieldMapper(Settings indexSettings, MappedFieldType existing) {
        this(existing.clone(), Defaults.ENABLED, indexSettings);
    }

    private AllFieldMapper(MappedFieldType fieldType, EnabledAttributeMapper enabled, Settings indexSettings) {
        super(NAME, fieldType, Defaults.FIELD_TYPE, indexSettings);
        this.enabledState = enabled;
    }

    public boolean enabled() {
        return this.enabledState.enabled;
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
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        if (!enabledState.enabled) {
            return;
        }
        for (AllEntries.Entry entry : context.allEntries().entries()) {
            fields.add(new AllField(fieldType().name(), entry.value(), entry.boost(), fieldType()));
        }
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
        if (enabled() == false) {
            return;
        }
        if (includeDefaults || fieldType().stored() != Defaults.FIELD_TYPE.stored()) {
            builder.field("store", fieldType().stored());
        }
        if (includeDefaults || fieldType().storeTermVectors() != Defaults.FIELD_TYPE.storeTermVectors()) {
            builder.field("store_term_vectors", fieldType().storeTermVectors());
        }
        if (includeDefaults || fieldType().storeTermVectorOffsets() != Defaults.FIELD_TYPE.storeTermVectorOffsets()) {
            builder.field("store_term_vector_offsets", fieldType().storeTermVectorOffsets());
        }
        if (includeDefaults ||
            fieldType().storeTermVectorPositions() != Defaults.FIELD_TYPE.storeTermVectorPositions()) {
            builder.field("store_term_vector_positions", fieldType().storeTermVectorPositions());
        }
        if (includeDefaults ||
            fieldType().storeTermVectorPayloads() != Defaults.FIELD_TYPE.storeTermVectorPayloads()) {
            builder.field("store_term_vector_payloads", fieldType().storeTermVectorPayloads());
        }
        if (includeDefaults || fieldType().omitNorms() != Defaults.FIELD_TYPE.omitNorms()) {
            builder.field("norms", !fieldType().omitNorms());
        }

        doXContentAnalyzers(builder, includeDefaults);

        if (fieldType().similarity() != null) {
            builder.field("similarity", fieldType().similarity().name());
        } else if (includeDefaults) {
            builder.field("similarity", SimilarityService.DEFAULT_SIMILARITY);
        }
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        if (((AllFieldMapper)mergeWith).enabled() != this.enabled() &&
            ((AllFieldMapper)mergeWith).enabledState != Defaults.ENABLED) {
            throw new IllegalArgumentException("mapper [" + fieldType().name() +
                "] enabled is " + this.enabled() + " now encountering "+ ((AllFieldMapper)mergeWith).enabled());
        }
        super.doMerge(mergeWith, updateAllTypes);
    }

}
