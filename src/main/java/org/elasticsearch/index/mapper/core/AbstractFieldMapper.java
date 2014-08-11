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

package org.elasticsearch.index.mapper.core;

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.RegexpFilter;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatService;
import org.elasticsearch.index.codec.postingsformat.PostingFormats;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatService;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.search.FieldDataTermsFilter;
import org.elasticsearch.index.similarity.SimilarityLookupService;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public abstract class AbstractFieldMapper<T> implements FieldMapper<T> {

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();
        public static final boolean DOC_VALUES = false;

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
        public static final ContentPath.Type PATH_TYPE = ContentPath.Type.FULL;
    }

    public abstract static class Builder<T extends Builder, Y extends AbstractFieldMapper> extends Mapper.Builder<T, Y> {

        protected final FieldType fieldType;
        protected Boolean docValues;
        protected float boost = Defaults.BOOST;
        protected boolean omitNormsSet = false;
        protected String indexName;
        protected NamedAnalyzer indexAnalyzer;
        protected NamedAnalyzer searchAnalyzer;
        protected Boolean includeInAll;
        protected boolean indexOptionsSet = false;
        protected PostingsFormatProvider postingsProvider;
        protected DocValuesFormatProvider docValuesProvider;
        protected SimilarityProvider similarity;
        protected Loading normsLoading;
        @Nullable
        protected Settings fieldDataSettings;
        protected final MultiFields.Builder multiFieldsBuilder;
        protected CopyTo copyTo;

        protected Builder(String name, FieldType fieldType) {
            super(name);
            this.fieldType = fieldType;
            multiFieldsBuilder = new MultiFields.Builder();
        }

        public T index(boolean index) {
            this.fieldType.setIndexed(index);
            return builder;
        }

        public T store(boolean store) {
            this.fieldType.setStored(store);
            return builder;
        }

        public T docValues(boolean docValues) {
            this.docValues = docValues;
            return builder;
        }

        public T storeTermVectors(boolean termVectors) {
            if (termVectors) {
                this.fieldType.setStoreTermVectors(termVectors);
            } // don't set it to false, it is default and might be flipped by a more specific option
            return builder;
        }

        public T storeTermVectorOffsets(boolean termVectorOffsets) {
            if (termVectorOffsets) {
                this.fieldType.setStoreTermVectors(termVectorOffsets);
            }
            this.fieldType.setStoreTermVectorOffsets(termVectorOffsets);
            return builder;
        }

        public T storeTermVectorPositions(boolean termVectorPositions) {
            if (termVectorPositions) {
                this.fieldType.setStoreTermVectors(termVectorPositions);
            }
            this.fieldType.setStoreTermVectorPositions(termVectorPositions);
            return builder;
        }

        public T storeTermVectorPayloads(boolean termVectorPayloads) {
            if (termVectorPayloads) {
                this.fieldType.setStoreTermVectors(termVectorPayloads);
            }
            this.fieldType.setStoreTermVectorPayloads(termVectorPayloads);
            return builder;
        }

        public T tokenized(boolean tokenized) {
            this.fieldType.setTokenized(tokenized);
            return builder;
        }

        public T boost(float boost) {
            this.boost = boost;
            return builder;
        }

        public T omitNorms(boolean omitNorms) {
            this.fieldType.setOmitNorms(omitNorms);
            this.omitNormsSet = true;
            return builder;
        }

        public T indexOptions(IndexOptions indexOptions) {
            this.fieldType.setIndexOptions(indexOptions);
            this.indexOptionsSet = true;
            return builder;
        }

        public T indexName(String indexName) {
            this.indexName = indexName;
            return builder;
        }

        public T indexAnalyzer(NamedAnalyzer indexAnalyzer) {
            this.indexAnalyzer = indexAnalyzer;
            return builder;
        }

        public T searchAnalyzer(NamedAnalyzer searchAnalyzer) {
            this.searchAnalyzer = searchAnalyzer;
            return builder;
        }

        public T includeInAll(Boolean includeInAll) {
            this.includeInAll = includeInAll;
            return builder;
        }

        public T postingsFormat(PostingsFormatProvider postingsFormat) {
            this.postingsProvider = postingsFormat;
            return builder;
        }

        public T docValuesFormat(DocValuesFormatProvider docValuesFormat) {
            this.docValuesProvider = docValuesFormat;
            return builder;
        }

        public T similarity(SimilarityProvider similarity) {
            this.similarity = similarity;
            return builder;
        }

        public T normsLoading(Loading normsLoading) {
            this.normsLoading = normsLoading;
            return builder;
        }

        public T fieldDataSettings(Settings settings) {
            this.fieldDataSettings = settings;
            return builder;
        }

        public T multiFieldPathType(ContentPath.Type pathType) {
            multiFieldsBuilder.pathType(pathType);
            return builder;
        }

        public T addMultiField(Mapper.Builder mapperBuilder) {
            multiFieldsBuilder.add(mapperBuilder);
            return builder;
        }

        public T copyTo(CopyTo copyTo) {
            this.copyTo = copyTo;
            return builder;
        }

        public Names buildNames(BuilderContext context) {
            return new Names(name, buildIndexName(context), indexName == null ? name : indexName, buildFullName(context), context.path().sourcePath());
        }

        public String buildIndexName(BuilderContext context) {
            String actualIndexName = indexName == null ? name : indexName;
            return context.path().pathAsText(actualIndexName);
        }

        public String buildFullName(BuilderContext context) {
            return context.path().fullPathAsText(name);
        }
    }

    private static final ThreadLocal<List<Field>> FIELD_LIST = new ThreadLocal<List<Field>>() {
        protected List<Field> initialValue() {
            return new ArrayList<>(2);
        }
    };

    protected final Names names;
    protected float boost;
    protected FieldType fieldType;
    private final boolean docValues;
    protected final NamedAnalyzer indexAnalyzer;
    protected NamedAnalyzer searchAnalyzer;
    protected PostingsFormatProvider postingsFormat;
    protected DocValuesFormatProvider docValuesFormat;
    protected final SimilarityProvider similarity;
    protected Loading normsLoading;
    protected Settings customFieldDataSettings;
    protected FieldDataType fieldDataType;
    protected final MultiFields multiFields;
    protected CopyTo copyTo;

    protected AbstractFieldMapper(Names names, float boost, FieldType fieldType, Boolean docValues, NamedAnalyzer indexAnalyzer,
                                  NamedAnalyzer searchAnalyzer, PostingsFormatProvider postingsFormat,
                                  DocValuesFormatProvider docValuesFormat, SimilarityProvider similarity,
                                  Loading normsLoading, @Nullable Settings fieldDataSettings, Settings indexSettings) {
        this(names, boost, fieldType, docValues, indexAnalyzer, searchAnalyzer, postingsFormat, docValuesFormat, similarity,
                normsLoading, fieldDataSettings, indexSettings, MultiFields.empty(), null);
    }

    protected AbstractFieldMapper(Names names, float boost, FieldType fieldType, Boolean docValues, NamedAnalyzer indexAnalyzer,
                                  NamedAnalyzer searchAnalyzer, PostingsFormatProvider postingsFormat,
                                  DocValuesFormatProvider docValuesFormat, SimilarityProvider similarity,
                                  Loading normsLoading, @Nullable Settings fieldDataSettings, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
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
        this.docValuesFormat = docValuesFormat;
        this.similarity = similarity;
        this.normsLoading = normsLoading;

        this.customFieldDataSettings = fieldDataSettings;
        if (fieldDataSettings == null) {
            this.fieldDataType = defaultFieldDataType();
        } else {
            // create a new field data type, with the default settings as well as the "new ones"
            this.fieldDataType = new FieldDataType(defaultFieldDataType().getType(),
                    ImmutableSettings.builder().put(defaultFieldDataType().getSettings()).put(fieldDataSettings)
            );
        }
        if (docValues != null) {
            this.docValues = docValues;
        } else if (fieldDataType == null) {
            this.docValues = false;
        } else {
            this.docValues = FieldDataType.DOC_VALUES_FORMAT_VALUE.equals(fieldDataType.getFormat(indexSettings));
        }
        this.multiFields = multiFields;
        this.copyTo = copyTo;
    }

    @Nullable
    protected String defaultPostingFormat() {
        return null;
    }

    @Nullable
    protected String defaultDocValuesFormat() {
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
    public CopyTo copyTo() {
        return copyTo;
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        final List<Field> fields = FIELD_LIST.get();
        assert fields.isEmpty();
        try {
            parseCreateField(context, fields);
            for (Field field : fields) {
                if (!customBoost()) {
                    field.setBoost(boost);
                }
                if (context.listener().beforeFieldAdded(this, field, context)) {
                    context.doc().add(field);
                }
            }
        } catch (Exception e) {
            throw new MapperParsingException("failed to parse [" + names.fullName() + "]", e);
        } finally {
            fields.clear();
        }
        multiFields.parse(this, context);
        if (copyTo != null) {
            copyTo.parse(context);
        }
    }

    /**
     * Parse the field value and populate <code>fields</code>.
     */
    protected abstract void parseCreateField(ParseContext context, List<Field> fields) throws IOException;

    /**
     * Derived classes can override it to specify that boost value is set by derived classes.
     */
    protected boolean customBoost() {
        return false;
    }

    @Override
    public void traverse(FieldMapperListener fieldMapperListener) {
        fieldMapperListener.fieldMapper(this);
        multiFields.traverse(fieldMapperListener);
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
    public Filter fieldDataTermsFilter(List values, @Nullable QueryParseContext context) {
        // create with initial size large enough to avoid rehashing
        ObjectOpenHashSet<BytesRef> terms =
                new ObjectOpenHashSet<>((int) (values.size() * (1 + ObjectOpenHashSet.DEFAULT_LOAD_FACTOR)));
        for (int i = 0, len = values.size(); i < len; i++) {
            terms.add(indexedValueForSearch(values.get(i)));
        }

        return FieldDataTermsFilter.newBytes(context.getForField(this), terms);
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
    public Query fuzzyQuery(String value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions) {
        return new FuzzyQuery(names.createIndexNameTerm(indexedValueForSearch(value)), fuzziness.asDistance(value), prefixLength, maxExpansions, transpositions);
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
        if (!this.hasDocValues() && fieldMergeWith.hasDocValues()) {
            // don't add conflict if this mapper has doc values while the mapper to merge doesn't since doc values are implicitely set
            // when the doc_values field data format is configured
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different " + TypeParsers.DOC_VALUES + " values");
        }
        if (this.fieldType().omitNorms() && !fieldMergeWith.fieldType.omitNorms()) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] cannot enable norms (`norms.enabled`)");
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
        if (!this.names().equals(fieldMergeWith.names())) {
            mergeContext.addConflict("mapper [" + names.fullName() + "] has different index_name");
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
        multiFields.merge(mergeWith, mergeContext);

        if (!mergeContext.mergeFlags().simulate()) {
            // apply changeable values
            this.fieldType = new FieldType(this.fieldType);
            this.fieldType.setOmitNorms(fieldMergeWith.fieldType.omitNorms());
            this.fieldType.freeze();
            this.boost = fieldMergeWith.boost;
            this.normsLoading = fieldMergeWith.normsLoading;
            this.copyTo = fieldMergeWith.copyTo;
            if (fieldMergeWith.postingsFormat != null) {
                this.postingsFormat = fieldMergeWith.postingsFormat;
            }
            if (fieldMergeWith.docValuesFormat != null) {
                this.docValuesFormat = fieldMergeWith.docValuesFormat;
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
    public DocValuesFormatProvider docValuesFormatProvider() {
        return docValuesFormat;
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
        if (includeDefaults || hasDocValues() != Defaults.DOC_VALUES) {
            builder.field(TypeParsers.DOC_VALUES, docValues);
        }
        if (includeDefaults || fieldType.storeTermVectors() != defaultFieldType.storeTermVectors()) {
            builder.field("term_vector", termVectorOptionsToString(fieldType));
        }
        if (includeDefaults || fieldType.omitNorms() != defaultFieldType.omitNorms() || normsLoading != null) {
            builder.startObject("norms");
            if (includeDefaults || fieldType.omitNorms() != defaultFieldType.omitNorms()) {
                builder.field("enabled", !fieldType.omitNorms());
            }
            if (normsLoading != null) {
                builder.field(Loading.KEY, normsLoading);
            }
            builder.endObject();
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

        if (docValuesFormat != null) {
            if (includeDefaults || !docValuesFormat.name().equals(defaultDocValuesFormat())) {
                builder.field(DOC_VALUES_FORMAT, docValuesFormat.name());
            }
        } else if (includeDefaults) {
            String format = defaultDocValuesFormat();
            if (format == null) {
                format = DocValuesFormatService.DEFAULT_FORMAT;
            }
            builder.field(DOC_VALUES_FORMAT, format);
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
        multiFields.toXContent(builder, params);

        if (copyTo != null) {
            copyTo.toXContent(builder, params);
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
                throw new ElasticsearchIllegalArgumentException("Unknown IndexOptions [" + indexOption + "]");
        }
    }

    public static String termVectorOptionsToString(FieldType fieldType) {
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
        multiFields.close();
    }

    @Override
    public boolean isNumeric() {
        return false;
    }

    @Override
    public boolean isSortable() {
        return true;
    }

    @Override
    public boolean supportsNullValue() {
        return true;
    }

    public boolean hasDocValues() {
        return docValues;
    }

    @Override
    public Loading normsLoading(Loading defaultLoading) {
        return normsLoading == null ? defaultLoading : normsLoading;
    }

    public static class MultiFields {

        public static MultiFields empty() {
            return new MultiFields(Defaults.PATH_TYPE, ImmutableOpenMap.<String, Mapper>of());
        }

        public static class Builder {

            private final ImmutableOpenMap.Builder<String, Mapper.Builder> mapperBuilders = ImmutableOpenMap.builder();
            private ContentPath.Type pathType = Defaults.PATH_TYPE;

            public Builder pathType(ContentPath.Type pathType) {
                this.pathType = pathType;
                return this;
            }

            public Builder add(Mapper.Builder builder) {
                mapperBuilders.put(builder.name(), builder);
                return this;
            }

            @SuppressWarnings("unchecked")
            public MultiFields build(AbstractFieldMapper.Builder mainFieldBuilder, BuilderContext context) {
                if (pathType == Defaults.PATH_TYPE && mapperBuilders.isEmpty()) {
                    return empty();
                } else if (mapperBuilders.isEmpty()) {
                    return new MultiFields(pathType, ImmutableOpenMap.<String, Mapper>of());
                } else {
                    ContentPath.Type origPathType = context.path().pathType();
                    context.path().pathType(pathType);
                    context.path().add(mainFieldBuilder.name());
                    ImmutableOpenMap.Builder mapperBuilders = this.mapperBuilders;
                    for (ObjectObjectCursor<String, Mapper.Builder> cursor : this.mapperBuilders) {
                        String key = cursor.key;
                        Mapper.Builder value = cursor.value;
                        mapperBuilders.put(key, value.build(context));
                    }
                    context.path().remove();
                    context.path().pathType(origPathType);
                    ImmutableOpenMap.Builder<String, Mapper> mappers = mapperBuilders.cast();
                    return new MultiFields(pathType, mappers.build());
                }
            }

        }

        private final ContentPath.Type pathType;
        private volatile ImmutableOpenMap<String, Mapper> mappers;

        public MultiFields(ContentPath.Type pathType, ImmutableOpenMap<String, Mapper> mappers) {
            this.pathType = pathType;
            this.mappers = mappers;
            // we disable the all in multi-field mappers
            for (ObjectCursor<Mapper> cursor : mappers.values()) {
                Mapper mapper = cursor.value;
                if (mapper instanceof AllFieldMapper.IncludeInAll) {
                    ((AllFieldMapper.IncludeInAll) mapper).unsetIncludeInAll();
                }
            }
        }

        public void parse(AbstractFieldMapper mainField, ParseContext context) throws IOException {
            if (mappers.isEmpty()) {
                return;
            }

            context = context.createMultiFieldContext();

            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(pathType);

            context.path().add(mainField.name());
            for (ObjectCursor<Mapper> cursor : mappers.values()) {
                cursor.value.parse(context);
            }
            context.path().remove();
            context.path().pathType(origPathType);
        }

        // No need for locking, because locking is taken care of in ObjectMapper#merge and DocumentMapper#merge
        public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
            AbstractFieldMapper mergeWithMultiField = (AbstractFieldMapper) mergeWith;

            List<FieldMapper> newFieldMappers = null;
            ImmutableOpenMap.Builder<String, Mapper> newMappersBuilder = null;

            for (ObjectCursor<Mapper> cursor : mergeWithMultiField.multiFields.mappers.values()) {
                Mapper mergeWithMapper = cursor.value;
                Mapper mergeIntoMapper = mappers.get(mergeWithMapper.name());
                if (mergeIntoMapper == null) {
                    // no mapping, simply add it if not simulating
                    if (!mergeContext.mergeFlags().simulate()) {
                        // we disable the all in multi-field mappers
                        if (mergeWithMapper instanceof AllFieldMapper.IncludeInAll) {
                            ((AllFieldMapper.IncludeInAll) mergeWithMapper).unsetIncludeInAll();
                        }
                        if (newMappersBuilder == null) {
                            newMappersBuilder = ImmutableOpenMap.builder(mappers);
                        }
                        newMappersBuilder.put(mergeWithMapper.name(), mergeWithMapper);
                        if (mergeWithMapper instanceof AbstractFieldMapper) {
                            if (newFieldMappers == null) {
                                newFieldMappers = new ArrayList<>(2);
                            }
                            newFieldMappers.add((FieldMapper) mergeWithMapper);
                        }
                    }
                } else {
                    mergeIntoMapper.merge(mergeWithMapper, mergeContext);
                }
            }

            // first add all field mappers
            if (newFieldMappers != null) {
                mergeContext.docMapper().addFieldMappers(newFieldMappers);
            }
            // now publish mappers
            if (newMappersBuilder != null) {
                mappers = newMappersBuilder.build();
            }
        }

        public void traverse(FieldMapperListener fieldMapperListener) {
            for (ObjectCursor<Mapper> cursor : mappers.values()) {
                cursor.value.traverse(fieldMapperListener);
            }
        }

        public void close() {
            for (ObjectCursor<Mapper> cursor : mappers.values()) {
                cursor.value.close();
            }
        }

        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (pathType != Defaults.PATH_TYPE) {
                builder.field("path", pathType.name().toLowerCase(Locale.ROOT));
            }
            if (!mappers.isEmpty()) {
                // sort the mappers so we get consistent serialization format
                Mapper[] sortedMappers = mappers.values().toArray(Mapper.class);
                Arrays.sort(sortedMappers, new Comparator<Mapper>() {
                    @Override
                    public int compare(Mapper o1, Mapper o2) {
                        return o1.name().compareTo(o2.name());
                    }
                });
                builder.startObject("fields");
                for (Mapper mapper : sortedMappers) {
                    mapper.toXContent(builder, params);
                }
                builder.endObject();
            }
            return builder;
        }
    }

    /**
     * Represents a list of fields with optional boost factor where the current field should be copied to
     */
    public static class CopyTo {

        private final ImmutableList<String> copyToFields;

        private CopyTo(ImmutableList<String> copyToFields) {
            this.copyToFields = copyToFields;
        }

        /**
         * Creates instances of the fields that the current field should be copied to
         */
        public void parse(ParseContext context) throws IOException {
            if (!context.isWithinCopyTo() && copyToFields.isEmpty() == false) {
                context = context.createCopyToContext();
                for (String field : copyToFields) {
                    // In case of a hierarchy of nested documents, we need to figure out
                    // which document the field should go to
                    Document targetDoc = null;
                    for (Document doc = context.doc(); doc != null; doc = doc.getParent()) {
                        if (field.startsWith(doc.getPrefix())) {
                            targetDoc = doc;
                            break;
                        }
                    }
                    assert targetDoc != null;
                    final ParseContext copyToContext;
                    if (targetDoc == context.doc()) {
                        copyToContext = context;
                    } else {
                        copyToContext = context.switchDoc(targetDoc);
                    }
                    parse(field, copyToContext);
                }
            }
        }

        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (!copyToFields.isEmpty()) {
                builder.startArray("copy_to");
                for (String field : copyToFields) {
                    builder.value(field);
                }
                builder.endArray();
            }
            return builder;
        }

        public static class Builder {
            private final ImmutableList.Builder<String> copyToBuilders = ImmutableList.builder();

            public Builder add(String field) {
                copyToBuilders.add(field);
                return this;
            }

            public CopyTo build() {
                return new CopyTo(copyToBuilders.build());
            }
        }

        public ImmutableList<String> copyToFields() {
            return copyToFields;
        }

        /**
         * Creates an copy of the current field with given field name and boost
         */
        public void parse(String field, ParseContext context) throws IOException {
            FieldMappers mappers = context.docMapper().mappers().indexName(field);
            if (mappers != null && !mappers.isEmpty()) {
                mappers.mapper().parse(context);
            } else {
                // The path of the dest field might be completely different from the current one so we need to reset it
                context = context.overridePath(new ContentPath(0));

                int posDot = field.lastIndexOf('.');
                if (posDot > 0) {
                    // Compound name
                    String objectPath = field.substring(0, posDot);
                    String fieldPath = field.substring(posDot + 1);
                    ObjectMapper mapper = context.docMapper().objectMappers().get(objectPath);
                    if (mapper == null) {
                        //TODO: Create an object dynamically?
                        throw new MapperParsingException("attempt to copy value to non-existing object [" + field + "]");
                    }

                    context.path().add(objectPath);

                    // We might be in dynamically created field already, so need to clean withinNewMapper flag
                    // and then restore it, so we wouldn't miss new mappers created from copy_to fields
                    boolean origWithinNewMapper = context.isWithinNewMapper();
                    context.clearWithinNewMapper();

                    try {
                        mapper.parseDynamicValue(context, fieldPath, context.parser().currentToken());
                    } finally {
                        if (origWithinNewMapper) {
                            context.setWithinNewMapper();
                        } else {
                            context.clearWithinNewMapper();
                        }
                    }

                } else {
                    // We might be in dynamically created field already, so need to clean withinNewMapper flag
                    // and then restore it, so we wouldn't miss new mappers created from copy_to fields
                    boolean origWithinNewMapper = context.isWithinNewMapper();
                    context.clearWithinNewMapper();
                    try {
                        context.docMapper().root().parseDynamicValue(context, field, context.parser().currentToken());
                    } finally {
                        if (origWithinNewMapper) {
                            context.setWithinNewMapper();
                        } else {
                            context.clearWithinNewMapper();
                        }
                    }

                }
            }
        }


    }

    /**
     * Returns if this field is only generated when indexing. For example, the field of type token_count
     */
    public boolean isGenerated() {
        return false;
    }

}
