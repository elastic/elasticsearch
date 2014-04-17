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
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.all.AllField;
import org.elasticsearch.common.lucene.all.AllTermQuery;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.similarity.SimilarityLookupService;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.MapperBuilders.all;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;

/**
 *
 */
public class AllFieldMapper extends AbstractFieldMapper<Void> implements InternalMapper, RootMapper {

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
        public static final boolean ENABLED = true;

        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setIndexed(true);
            FIELD_TYPE.setTokenized(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, AllFieldMapper> {

        private boolean enabled = Defaults.ENABLED;

        // an internal flag, automatically set if we encounter boosting
        boolean autoBoost = false;

        public Builder() {
            super(Defaults.NAME, new FieldType(Defaults.FIELD_TYPE));
            builder = this;
            indexName = Defaults.INDEX_NAME;
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        @Override
        public AllFieldMapper build(BuilderContext context) {
            // In case the mapping overrides these
            fieldType.setIndexed(true);
            fieldType.setTokenized(true);

            return new AllFieldMapper(name, fieldType, indexAnalyzer, searchAnalyzer, enabled, autoBoost, postingsProvider, docValuesProvider, similarity, normsLoading, fieldDataSettings, context.indexSettings());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            AllFieldMapper.Builder builder = all();
            parseField(builder, builder.name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    builder.enabled(nodeBooleanValue(fieldNode));
                } else if (fieldName.equals("auto_boost")) {
                    builder.autoBoost = nodeBooleanValue(fieldNode);
                }
            }
            return builder;
        }
    }


    private boolean enabled;
    // The autoBoost flag is automatically set based on indexed docs on the mappings
    // if a doc is indexed with a specific boost value and part of _all, it is automatically
    // set to true. This allows to optimize (automatically, which we like) for the common case
    // where fields don't usually have boost associated with them, and we don't need to use the
    // special SpanTermQuery to look at payloads
    private volatile boolean autoBoost;

    public AllFieldMapper() {
        this(Defaults.NAME, new FieldType(Defaults.FIELD_TYPE), null, null, Defaults.ENABLED, false, null, null, null, null, null, ImmutableSettings.EMPTY);
    }

    protected AllFieldMapper(String name, FieldType fieldType, NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer,
                             boolean enabled, boolean autoBoost, PostingsFormatProvider postingsProvider,
                             DocValuesFormatProvider docValuesProvider, SimilarityProvider similarity, Loading normsLoading,
                             @Nullable Settings fieldDataSettings, Settings indexSettings) {
        super(new Names(name, name, name, name), 1.0f, fieldType, null, indexAnalyzer, searchAnalyzer, postingsProvider, docValuesProvider,
                similarity, normsLoading, fieldDataSettings, indexSettings);
        this.enabled = enabled;
        this.autoBoost = autoBoost;

    }

    public boolean enabled() {
        return this.enabled;
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
        if (!autoBoost) {
            return new TermQuery(term);
        }
        if (fieldType.indexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
            return new AllTermQuery(term);
        }
        return new TermQuery(term);
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
    public void parse(ParseContext context) throws IOException {
        // we parse in post parse
    }

    @Override
    public boolean includeInObject() {
        return true;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        if (!enabled) {
            return;
        }
        // reset the entries
        context.allEntries().reset();

        // if the autoBoost flag is not set, and we indexed a doc with custom boost, make
        // sure to update the flag, and notify mappings on change
        if (!autoBoost && context.allEntries().customBoost()) {
            autoBoost = true;
            context.setMappingsModified();
        }

        Analyzer analyzer = findAnalyzer(context);
        fields.add(new AllField(names.indexName(), context.allEntries(), analyzer, fieldType));
    }

    private Analyzer findAnalyzer(ParseContext context) {
        Analyzer analyzer = indexAnalyzer;
        if (analyzer == null) {
            analyzer = context.analyzer();
            if (analyzer == null) {
                analyzer = context.docMapper().indexAnalyzer();
                if (analyzer == null) {
                    // This should not happen, should we log warn it?
                    analyzer = Lucene.STANDARD_ANALYZER;
                }
            }
        }
        return analyzer;
    }

    @Override
    public Void value(Object value) {
        return null;
    }

    @Override
    public Object valueForSearch(Object value) {
        return null;
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
        if (includeDefaults || enabled != Defaults.ENABLED) {
            builder.field("enabled", enabled);
        }
        if (includeDefaults || autoBoost != false) {
            builder.field("auto_boost", autoBoost);
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


        if (indexAnalyzer == null && searchAnalyzer == null) {
            if (includeDefaults) {
                builder.field("analyzer", "default");
            }
        } else if (indexAnalyzer == null) {
            // searchAnalyzer != null
            if (includeDefaults || !searchAnalyzer.name().startsWith("_")) {
                builder.field("search_analyzer", searchAnalyzer.name());
            }
        } else if (searchAnalyzer == null) {
            // indexAnalyzer != null
            if (includeDefaults || !indexAnalyzer.name().startsWith("_")) {
                builder.field("index_analyzer", indexAnalyzer.name());
            }
        } else if (indexAnalyzer.name().equals(searchAnalyzer.name())) {
            // indexAnalyzer == searchAnalyzer
            if (includeDefaults || !indexAnalyzer.name().startsWith("_")) {
                builder.field("analyzer", indexAnalyzer.name());
            }
        } else {
            // both are there but different
            if (includeDefaults || !indexAnalyzer.name().startsWith("_")) {
                builder.field("index_analyzer", indexAnalyzer.name());
            }
            if (includeDefaults || !searchAnalyzer.name().startsWith("_")) {
                builder.field("search_analyzer", searchAnalyzer.name());
            }
        }

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
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        // do nothing here, no merging, but also no exception
    }

    @Override
    public boolean hasDocValues() {
        return false;
    }
}
