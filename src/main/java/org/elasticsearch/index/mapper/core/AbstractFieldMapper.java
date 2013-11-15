/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper.core;

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.google.common.base.Objects;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.RegexpFilter;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.postingsformat.PostingFormats;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatService;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.search.FieldDataTermsFilter;
import org.elasticsearch.index.similarity.SimilarityLookupService;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public abstract class AbstractFieldMapper<T> implements FieldMapper<T> {

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setIndexed(true);
            FIELD_TYPE.setTokenized(true);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setStoreTermVectors(false);
            FIELD_TYPE.setOmitNorms(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            FIELD_TYPE.freeze();
        }

        public static final float BOOST = 1.0f;
    }

    public abstract static class OpenBuilder<T extends Builder, Y extends AbstractFieldMapper> extends AbstractFieldMapper.Builder<T, Y> {

        protected OpenBuilder(String name, FieldType fieldType) {
            super(name, fieldType);
        }

        @Override
        public T index(boolean index) {
            return super.index(index);
        }

        @Override
        public T store(boolean store) {
            return super.store(store);
        }

        @Override
        public T storeTermVectors(boolean termVectors) {
            return super.storeTermVectors(termVectors);
        }

        @Override
        public T storeTermVectorOffsets(boolean termVectorOffsets) {
            return super.storeTermVectorOffsets(termVectorOffsets);
        }

        @Override
        public T storeTermVectorPositions(boolean termVectorPositions) {
            return super.storeTermVectorPositions(termVectorPositions);
        }

        @Override
        public T storeTermVectorPayloads(boolean termVectorPayloads) {
            return super.storeTermVectorPayloads(termVectorPayloads);
        }

        @Override
        public T tokenized(boolean tokenized) {
            return super.tokenized(tokenized);
        }

        @Override
        public T boost(float boost) {
            return super.boost(boost);
        }

        @Override
        public T omitNorms(boolean omitNorms) {
            return super.omitNorms(omitNorms);
        }

        @Override
        public T indexOptions(IndexOptions indexOptions) {
            return super.indexOptions(indexOptions);
        }

        @Override
        public T indexName(String indexName) {
            return super.indexName(indexName);
        }

        @Override
        public T indexAnalyzer(NamedAnalyzer indexAnalyzer) {
            return super.indexAnalyzer(indexAnalyzer);
        }

        @Override
        public T searchAnalyzer(NamedAnalyzer searchAnalyzer) {
            return super.searchAnalyzer(searchAnalyzer);
        }

        @Override
        public T similarity(SimilarityProvider similarity) {
            return super.similarity(similarity);
        }

        public T fieldDataSettings(Settings settings) {
            return super.fieldDataSettings(settings);
        }
    }

    public abstract static class Builder<T extends Builder, Y extends AbstractFieldMapper> extends Mapper.Builder<T, Y> {

        protected final FieldType fieldType;
        protected float boost = Defaults.BOOST;
        protected boolean omitNormsSet = false;
        protected String indexName;
        protected NamedAnalyzer indexAnalyzer;
        protected NamedAnalyzer searchAnalyzer;
        protected Boolean includeInAll;
        protected boolean indexOptionsSet = false;
        protected PostingsFormatProvider provider;
        protected SimilarityProvider similarity;
        @Nullable
        protected Settings fieldDataSettings;

        protected Builder(String name, FieldType fieldType) {
            super(name);
            this.fieldType = fieldType;
        }

        protected T index(boolean index) {
            this.fieldType.setIndexed(index);
            return builder;
        }

        protected T store(boolean store) {
            this.fieldType.setStored(store);
            return builder;
        }

        protected T storeTermVectors(boolean termVectors) {
            this.fieldType.setStoreTermVectors(termVectors);
            return builder;
        }

        protected T storeTermVectorOffsets(boolean termVectorOffsets) {
            this.fieldType.setStoreTermVectors(termVectorOffsets);
            this.fieldType.setStoreTermVectorOffsets(termVectorOffsets);
            return builder;
        }

        protected T storeTermVectorPositions(boolean termVectorPositions) {
            this.fieldType.setStoreTermVectors(termVectorPositions);
            this.fieldType.setStoreTermVectorPositions(termVectorPositions);
            return builder;
        }

        protected T storeTermVectorPayloads(boolean termVectorPayloads) {
            this.fieldType.setStoreTermVectors(termVectorPayloads);
            this.fieldType.setStoreTermVectorPayloads(termVectorPayloads);
            return builder;
        }

        protected T tokenized(boolean tokenized) {
            this.fieldType.setTokenized(tokenized);
            return builder;
        }

        protected T boost(float boost) {
            this.boost = boost;
            return builder;
        }

        protected T omitNorms(boolean omitNorms) {
            this.fieldType.setOmitNorms(omitNorms);
            this.omitNormsSet = true;
            return builder;
        }

        protected T indexOptions(IndexOptions indexOptions) {
            this.fieldType.setIndexOptions(indexOptions);
            this.indexOptionsSet = true;
            return builder;
        }

        protected T indexName(String indexName) {
            this.indexName = indexName;
            return builder;
        }

        protected T indexAnalyzer(NamedAnalyzer indexAnalyzer) {
            this.indexAnalyzer = indexAnalyzer;
            return builder;
        }

        protected T searchAnalyzer(NamedAnalyzer searchAnalyzer) {
            this.searchAnalyzer = searchAnalyzer;
            return builder;
        }

        protected T includeInAll(Boolean includeInAll) {
            this.includeInAll = includeInAll;
            return builder;
        }

        protected T postingsFormat(PostingsFormatProvider postingsFormat) {
            this.provider = postingsFormat;
            return builder;
        }

        protected T similarity(SimilarityProvider similarity) {
            this.similarity = similarity;
            return builder;
        }

        protected T fieldDataSettings(Settings settings) {
            this.fieldDataSettings = settings;
            return builder;
        }

        protected Names buildNames(BuilderContext context) {
            return new Names(name, buildIndexName(context), indexName == null ? name : indexName, buildFullName(context), context.path().sourcePath());
        }

        protected String buildIndexName(BuilderContext context) {
            String actualIndexName = indexName == null ? name : indexName;
            return context.path().pathAsText(actualIndexName);
        }

        protected String buildFullName(BuilderContext context) {
            return context.path().fullPathAsText(name);
        }
    }

    protected final Names names;
    protected float boost;
    protected final FieldType fieldType;
    protected final NamedAnalyzer indexAnalyzer;
    protected NamedAnalyzer searchAnalyzer;
    protected PostingsFormatProvider postingsFormat;
    protected final SimilarityProvider similarity;

    protected Settings customFieldDataSettings;
    protected FieldDataType fieldDataType;

    protected AbstractFieldMapper(Names names, float boost, FieldType fieldType, NamedAnalyzer indexAnalyzer,
                                  NamedAnalyzer searchAnalyzer, PostingsFormatProvider postingsFormat, SimilarityProvider similarity,
                                  @Nullable Settings fieldDataSettings) {
        this.names = names;
        this.boost = boost;
        this.fieldType = fieldType;
        this.fieldType.freeze();

        // automatically set to keyword analyzer if its indexed and not analyzed
        if (indexAnalyzer == null && !this.fieldType.tokenized() && this.fieldType.indexed()) {
            this.indexAnalyzer = Lucene.KEYWORD_ANALYZER;
        } else {
            this.indexAnalyzer = indexAnalyzer;
        }
        // automatically set to keyword analyzer if its indexed and not analyzed
        if (searchAnalyzer == null && !this.fieldType.tokenized() && this.fieldType.indexed()) {
            this.searchAnalyzer = Lucene.KEYWORD_ANALYZER;
        } else {
            this.searchAnalyzer = searchAnalyzer;
        }
        if (postingsFormat == null) {
            if (defaultPostingFormat() != null) {
                postingsFormat = PostingFormats.getAsProvider(defaultPostingFormat());
            }
        }
        this.postingsFormat = postingsFormat;
        this.similarity = similarity;

        this.customFieldDataSettings = fieldDataSettings;
        if (fieldDataSettings == null) {
            this.fieldDataType = defaultFieldDataType();
        } else {
            // create a new field data type, with the default settings as well as the "new ones"
            this.fieldDataType = new FieldDataType(defaultFieldDataType().getType(),
                    ImmutableSettings.builder().put(defaultFieldDataType().getSettings()).put(fieldDataSettings)
            );
        }
    }

    @Nullable
    protected String defaultPostingFormat() {
        return null;
    }

    @Override
    public String name() {
        return names.name();
    }

    @Override
    public Names names() {
        return this.names;
    }

    public abstract FieldType defaultFieldType();

    public abstract FieldDataType defaultFieldDataType();

    @Override
    public final FieldDataType fieldDataType() {
        return fieldDataType;
    }

    @Override
    public FieldType fieldType() {
        return fieldType;
    }

    @Override
    public float boost() {
        return this.boost;
    }

    @Override
    public Analyzer indexAnalyzer() {
        return this.indexAnalyzer;
    }

    @Override
    public Analyzer searchAnalyzer() {
        return this.searchAnalyzer;
    }

    @Override
    public Analyzer searchQuoteAnalyzer() {
        return this.searchAnalyzer;
    }

    @Override
    public SimilarityProvider similarity() {
        return similarity;
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        try {
            Field field = parseCreateField(context);
            if (field == null) {
                return;
            }
            if (!customBoost()) {
                field.setBoost(boost);
            }
            if (context.listener().beforeFieldAdded(this, field, context)) {
                context.doc().add(field);
            }
        } catch (Exception e) {
            throw new MapperParsingException("failed to parse [" + names.fullName() + "]", e);
        }
    }

    protected abstract Field parseCreateField(ParseContext context) throws IOException;

    /**
     * Derived classes can override it to specify that boost value is set by derived classes.
     */
    protected boolean customBoost() {
        return false;
    }

    @Override
    public void traverse(FieldMapperListener fieldMapperListener) {
        fieldMapperListener.fieldMapper(this);
    }

    @Override
    public void traverse(ObjectMapperListener objectMapperListener) {
        // nothing to do here...
    }

    @Override
    public Object valueForSearch(Object value) {
        return value;
    }

    @Override
    public BytesRef indexedValueForSearch(Object value) {
        return BytesRefs.toBytesRef(value);
    }

    @Override
    public Query queryStringTermQuery(Term term) {
        return null;
    }

    @Override
    public boolean useTermQueryWithQueryString() {
        return false;
    }

    @Override
    public Query termQuery(Object value, @Nullable QueryParseContext context) {
        return new TermQuery(names().createIndexNameTerm(indexedValueForSearch(value)));
    }

    @Override
    public Filter termFilter(Object value, @Nullable QueryParseContext context) {
        return new TermFilter(names().createIndexNameTerm(indexedValueForSearch(value)));
    }

    @Override
    public Filter termsFilter(List values, @Nullable QueryParseContext context) {
        BytesRef[] bytesRefs = new BytesRef[values.size()];
        for (int i = 0; i < bytesRefs.length; i++) {
            bytesRefs[i] = indexedValueForSearch(values.get(i));
        }
        return new TermsFilter(names.indexName(), bytesRefs);
    }

    /**
     * A terms filter based on the field data cache
     */
    @Override
    public Filter termsFilter(IndexFieldDataService fieldDataService, List values, @Nullable QueryParseContext context) {
        // create with initial size large enough to avoid rehashing
        ObjectOpenHashSet<BytesRef> terms =
                new ObjectOpenHashSet<BytesRef>((int) (values.size() * (1 + ObjectOpenHashSet.DEFAULT_LOAD_FACTOR)));
        for (int i = 0, len = values.size(); i < len; i++) {
            terms.add(indexedValueForSearch(values.get(i)));
        }

        return FieldDataTermsFilter.newBytes(fieldDataService.getForField(this), terms);
    }

    @Override
    public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return new TermRangeQuery(names.indexName(),
                lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
                upperTerm == null ? null : indexedValueForSearch(upperTerm),
                includeLower, includeUpper);
    }

    @Override
    public Filter rangeFilter(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return new TermRangeFilter(names.indexName(),
                lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
                upperTerm == null ? null : indexedValueForSearch(upperTerm),
                includeLower, includeUpper);
    }

    @Override
    public Query fuzzyQuery(String value, String minSim, int prefixLength, int maxExpansions, boolean transpositions) {
        int edits = FuzzyQuery.floatToEdits(Float.parseFloat(minSim), value.codePointCount(0, value.length()));
        return new FuzzyQuery(names.createIndexNameTerm(indexedValueForSearch(value)), edits, prefixLength, maxExpansions, transpositions);
    }

    @Override
    public Query prefixQuery(Object value, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context) {
        PrefixQuery query = new PrefixQuery(names().createIndexNameTerm(indexedValueForSearch(value)));
        if (method != null) {
            query.setRewriteMethod(method);
        }
        return query;
    }

    @Override
    public Filter prefixFilter(Object value, @Nullable QueryParseContext context) {
        return new PrefixFilter(names().createIndexNameTerm(indexedValueForSearch(value)));
    }

    @Override
    public Query regexpQuery(Object value, int flags, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context) {
        RegexpQuery query = new RegexpQuery(names().createIndexNameTerm(indexedValueForSearch(value)), flags);
        if (method != null) {
            query.setRewriteMethod(method);
        }
        return query;
    }

    @Override
    public Filter regexpFilter(Object value, int flags, @Nullable QueryParseContext parseContext) {
        return new RegexpFilter(names().createIndexNameTerm(indexedValueForSearch(value)), flags);
    }

    @Override
    public Filter nullValueFilter() {
        return null;
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        if (!this.getClass().equals(mergeWith.getClass())) {
            String mergedType = mergeWith.getClass().getSimpleName();
            if (mergeWith instanceof AbstractFieldMapper) {
                mergedType = ((AbstractFieldMapper) mergeWith).contentType();
            }
            mergeContext.addConflict("mapper [" + names.fullName() + "] of different type, current_type [" + contentType() + "], merged_type [" + mergedType + "]");
            // different types, return
            return;
        }
        AbstractFieldMapper fieldMergeWith = (AbstractFieldMapper) mergeWith;
        if (this.fieldType().indexed() != fieldMergeWith.fieldType().indexed() || this.fieldType().tokenized() != fieldMergeWith.fieldType().tokenized()) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different index values");
        }
        if (this.fieldType().stored() != fieldMergeWith.fieldType().stored()) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different store values");
        }
        if (this.fieldType().tokenized() != fieldMergeWith.fieldType().tokenized()) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different tokenize values");
        }
        if (this.fieldType().storeTermVectors() != fieldMergeWith.fieldType().storeTermVectors()) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different store_term_vector values");
        }
        if (this.fieldType().storeTermVectorOffsets() != fieldMergeWith.fieldType().storeTermVectorOffsets()) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different store_term_vector_offsets values");
        }
        if (this.fieldType().storeTermVectorPositions() != fieldMergeWith.fieldType().storeTermVectorPositions()) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different store_term_vector_positions values");
        }
        if (this.fieldType().storeTermVectorPayloads() != fieldMergeWith.fieldType().storeTermVectorPayloads()) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different store_term_vector_payloads values");
        }
        if (this.indexAnalyzer == null) {
            if (fieldMergeWith.indexAnalyzer != null) {
                mergeContext.addConflict("mapper [" + names.fullName() + "] has different index_analyzer");
            }
        } else if (fieldMergeWith.indexAnalyzer == null) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different index_analyzer");
        } else if (!this.indexAnalyzer.name().equals(fieldMergeWith.indexAnalyzer.name())) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different index_analyzer");
        }

        if (this.similarity == null) {
            if (fieldMergeWith.similarity() != null) {
                mergeContext.addConflict("mapper [" + names.fullName() + "] has different similarity");
            }
        } else if (fieldMergeWith.similarity() == null) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different similarity");
        } else if (!this.similarity().equals(fieldMergeWith.similarity())) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different similarity");
        }

        if (!mergeContext.mergeFlags().simulate()) {
            // apply changeable values
            this.boost = fieldMergeWith.boost;
            if (fieldMergeWith.postingsFormat != null) {
                this.postingsFormat = fieldMergeWith.postingsFormat;
            }
            if (fieldMergeWith.searchAnalyzer != null) {
                this.searchAnalyzer = fieldMergeWith.searchAnalyzer;
            }
            if (fieldMergeWith.customFieldDataSettings != null) {
                if (!Objects.equal(fieldMergeWith.customFieldDataSettings, this.customFieldDataSettings)) {
                    this.customFieldDataSettings = fieldMergeWith.customFieldDataSettings;
                    this.fieldDataType = new FieldDataType(defaultFieldDataType().getType(),
                            ImmutableSettings.builder().put(defaultFieldDataType().getSettings()).put(this.customFieldDataSettings)
                    );
                }
            }
        }
    }

    @Override
    public PostingsFormatProvider postingsFormatProvider() {
        return postingsFormat;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(names.name());
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);
        doXContentBody(builder, includeDefaults, params);
        return builder.endObject();
    }

    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {

        builder.field("type", contentType());
        if (includeDefaults || !names.name().equals(names.indexNameClean())) {
            builder.field("index_name", names.indexNameClean());
        }

        if (includeDefaults || boost != 1.0f) {
            builder.field("boost", boost);
        }

        FieldType defaultFieldType = defaultFieldType();
        if (includeDefaults || fieldType.indexed() != defaultFieldType.indexed() ||
                fieldType.tokenized() != defaultFieldType.tokenized()) {
            builder.field("index", indexTokenizeOptionToString(fieldType.indexed(), fieldType.tokenized()));
        }
        if (includeDefaults || fieldType.stored() != defaultFieldType.stored()) {
            builder.field("store", fieldType.stored());
        }
        if (includeDefaults || fieldType.storeTermVectors() != defaultFieldType.storeTermVectors()) {
            builder.field("term_vector", termVectorOptionsToString(fieldType));
        }
        if (includeDefaults || fieldType.omitNorms() != defaultFieldType.omitNorms()) {
            builder.field("omit_norms", fieldType.omitNorms());
        }
        if (includeDefaults || fieldType.indexOptions() != defaultFieldType.indexOptions()) {
            builder.field("index_options", indexOptionToString(fieldType.indexOptions()));
        }

        if (indexAnalyzer == null && searchAnalyzer == null) {
            if (includeDefaults) {
                builder.field("analyzer", "default");
            }
        } else if (indexAnalyzer == null) {
            // searchAnalyzer != null
            if (includeDefaults || (!searchAnalyzer.name().startsWith("_") && !searchAnalyzer.name().equals("default"))) {
                builder.field("search_analyzer", searchAnalyzer.name());
            }
        } else if (searchAnalyzer == null) {
            // indexAnalyzer != null
            if (includeDefaults || (!indexAnalyzer.name().startsWith("_") && !indexAnalyzer.name().equals("default"))) {
                builder.field("index_analyzer", indexAnalyzer.name());
            }
        } else if (indexAnalyzer.name().equals(searchAnalyzer.name())) {
            // indexAnalyzer == searchAnalyzer
            if (includeDefaults || (!indexAnalyzer.name().startsWith("_") && !indexAnalyzer.name().equals("default"))) {
                builder.field("analyzer", indexAnalyzer.name());
            }
        } else {
            // both are there but different
            if (includeDefaults || (!indexAnalyzer.name().startsWith("_") && !indexAnalyzer.name().equals("default"))) {
                builder.field("index_analyzer", indexAnalyzer.name());
            }
            if (includeDefaults || (!searchAnalyzer.name().startsWith("_") && !searchAnalyzer.name().equals("default"))) {
                builder.field("search_analyzer", searchAnalyzer.name());
            }
        }

        if (postingsFormat != null) {
            if (includeDefaults || !postingsFormat.name().equals(defaultPostingFormat())) {
                builder.field("postings_format", postingsFormat.name());
            }
        } else if (includeDefaults) {
            String format = defaultPostingFormat();
            if (format == null) {
                format = PostingsFormatService.DEFAULT_FORMAT;
            }
            builder.field("postings_format", format);
        }

        if (similarity() != null) {
            builder.field("similarity", similarity().name());
        } else if (includeDefaults) {
            builder.field("similariry", SimilarityLookupService.DEFAULT_SIMILARITY);
        }

        if (customFieldDataSettings != null) {
            builder.field("fielddata", (Map) customFieldDataSettings.getAsMap());
        } else if (includeDefaults) {
            builder.field("fielddata", (Map) fieldDataType.getSettings().getAsMap());
        }
    }

    protected static String indexOptionToString(IndexOptions indexOption) {
        switch (indexOption) {
            case DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS:
                return TypeParsers.INDEX_OPTIONS_OFFSETS;
            case DOCS_AND_FREQS:
                return TypeParsers.INDEX_OPTIONS_FREQS;
            case DOCS_AND_FREQS_AND_POSITIONS:
                return TypeParsers.INDEX_OPTIONS_POSITIONS;
            case DOCS_ONLY:
                return TypeParsers.INDEX_OPTIONS_DOCS;
            default:
                throw new ElasticSearchIllegalArgumentException("Unknown IndexOptions [" + indexOption + "]");
        }
    }

    protected static String termVectorOptionsToString(FieldType fieldType) {
        if (!fieldType.storeTermVectors()) {
            return "no";
        } else if (!fieldType.storeTermVectorOffsets() && !fieldType.storeTermVectorPositions()) {
            return "yes";
        } else if (fieldType.storeTermVectorOffsets() && !fieldType.storeTermVectorPositions()) {
            return "with_offsets";
        } else {
            StringBuilder builder = new StringBuilder("with");
            if (fieldType.storeTermVectorPositions()) {
                builder.append("_positions");
            }
            if (fieldType.storeTermVectorOffsets()) {
                builder.append("_offsets");
            }
            if (fieldType.storeTermVectorPayloads()) {
                builder.append("_payloads");
            }
            return builder.toString();
        }
    }

    protected static String indexTokenizeOptionToString(boolean indexed, boolean tokenized) {
        if (!indexed) {
            return "no";
        } else if (tokenized) {
            return "analyzed";
        } else {
            return "not_analyzed";
        }
    }


    protected abstract String contentType();

    @Override
    public void close() {
        // nothing to do here, sub classes to override if needed
    }

    @Override
    public boolean isNumeric() {
        return false;
    }

    @Override
    public boolean isSortable() {
        return true;
    }
}
