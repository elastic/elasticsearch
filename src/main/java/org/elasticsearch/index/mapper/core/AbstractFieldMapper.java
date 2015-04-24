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
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.Version;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.mapper.object.RootObjectMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.search.FieldDataTermsFilter;
import org.elasticsearch.index.similarity.SimilarityLookupService;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.TreeMap;

/**
 *
 */
public abstract class AbstractFieldMapper<T> implements FieldMapper<T> {

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();
        public static final boolean PRE_2X_DOC_VALUES = false;

        static {
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
        private final IndexOptions defaultOptions;
        protected Boolean docValues;
        protected float boost = Defaults.BOOST;
        protected boolean omitNormsSet = false;
        protected String indexName;
        protected NamedAnalyzer indexAnalyzer;
        protected NamedAnalyzer searchAnalyzer;
        protected Boolean includeInAll;
        protected boolean indexOptionsSet = false;
        protected SimilarityProvider similarity;
        protected Loading normsLoading;
        @Nullable
        protected Settings fieldDataSettings;
        protected final MultiFields.Builder multiFieldsBuilder;
        protected CopyTo copyTo;

        protected Builder(String name, FieldType fieldType) {
            super(name);
            this.fieldType = fieldType;
            this.defaultOptions = fieldType.indexOptions(); // we have to store it the fieldType is mutable
            multiFieldsBuilder = new MultiFields.Builder();
        }

        public T index(boolean index) {
            if (index) {
                if (fieldType.indexOptions() == IndexOptions.NONE) {
                    /*
                     * the logic here is to reset to the default options only if we are not indexed ie. options are null
                     * if the fieldType has a non-null option we are all good it might have been set through a different
                     * call.
                     */
                    final IndexOptions options = getDefaultIndexOption();
                    assert options != IndexOptions.NONE : "default IndexOptions is NONE can't enable indexing";
                    fieldType.setIndexOptions(options);
                }
            } else {
                fieldType.setIndexOptions(IndexOptions.NONE);
            }
            return builder;
        }

        protected IndexOptions getDefaultIndexOption() {
            return defaultOptions;
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

        protected Names buildNames(BuilderContext context) {
            return new Names(name, buildIndexName(context), buildIndexNameClean(context), buildFullName(context), context.path().sourcePath());
        }

        protected String buildIndexName(BuilderContext context) {
            if (context.indexCreatedVersion().onOrAfter(Version.V_2_0_0)) {
                return buildFullName(context);
            }
            String actualIndexName = indexName == null ? name : indexName;
            return context.path().pathAsText(actualIndexName);
        }
        
        protected String buildIndexNameClean(BuilderContext context) {
            if (context.indexCreatedVersion().onOrAfter(Version.V_2_0_0)) {
                return buildFullName(context);
            }
            return indexName == null ? name : indexName;
        }

        protected String buildFullName(BuilderContext context) {
            return context.path().fullPathAsText(name);
        }
    }

    protected final Names names;
    protected float boost;
    protected FieldType fieldType;
    protected final Boolean docValues;
    protected final NamedAnalyzer indexAnalyzer;
    protected NamedAnalyzer searchAnalyzer;
    protected final SimilarityProvider similarity;
    protected Loading normsLoading;
    protected Settings customFieldDataSettings;
    protected FieldDataType fieldDataType;
    protected final MultiFields multiFields;
    protected CopyTo copyTo;
    protected final boolean indexCreatedBefore2x;

    protected AbstractFieldMapper(Names names, float boost, FieldType fieldType, Boolean docValues, NamedAnalyzer indexAnalyzer,
                                  NamedAnalyzer searchAnalyzer, SimilarityProvider similarity,
                                  Loading normsLoading, @Nullable Settings fieldDataSettings, Settings indexSettings) {
        this(names, boost, fieldType, docValues, indexAnalyzer, searchAnalyzer, similarity,
                normsLoading, fieldDataSettings, indexSettings, MultiFields.empty(), null);
    }

    protected AbstractFieldMapper(Names names, float boost, FieldType fieldType, Boolean docValues, NamedAnalyzer indexAnalyzer,
                                  NamedAnalyzer searchAnalyzer, SimilarityProvider similarity,
                                  Loading normsLoading, @Nullable Settings fieldDataSettings, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        assert indexSettings != null;
        this.names = names;
        this.boost = boost;
        this.fieldType = fieldType;
        this.fieldType.freeze();
        this.indexCreatedBefore2x = Version.indexCreated(indexSettings).before(Version.V_2_0_0);

        boolean indexedNotAnalyzed = this.fieldType.tokenized() == false && this.fieldType.indexOptions() != IndexOptions.NONE;
        if (indexAnalyzer == null && indexedNotAnalyzed) {
            this.indexAnalyzer = this.searchAnalyzer = Lucene.KEYWORD_ANALYZER;
        } else {
            this.indexAnalyzer = indexAnalyzer;
            this.searchAnalyzer = searchAnalyzer;
        }

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
            // explicitly set
            this.docValues = docValues;
        } else if (fieldDataType != null && FieldDataType.DOC_VALUES_FORMAT_VALUE.equals(fieldDataType.getFormat(indexSettings))) {
            // convoluted way to enable doc values, should be removed in the future
            this.docValues = true;
        } else {
            this.docValues = null; // use the default
        }
        this.multiFields = multiFields;
        this.copyTo = copyTo;
    }
    
    protected boolean defaultDocValues() {
        if (indexCreatedBefore2x) {
            return Defaults.PRE_2X_DOC_VALUES;
        } else {
            return fieldType.tokenized() == false && fieldType.indexOptions() != IndexOptions.NONE;
        }
    }

    @Override
    public final boolean hasDocValues() {
        return docValues == null ? defaultDocValues() : docValues;
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
    public Mapper parse(ParseContext context) throws IOException {
        final List<Field> fields = new ArrayList<>(2);
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
        }
        multiFields.parse(this, context);
        if (copyTo != null) {
            copyTo.parse(context);
        }
        return null;
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
        return Queries.wrap(new TermQuery(names().createIndexNameTerm(indexedValueForSearch(value))));
    }

    @Override
    public Filter termsFilter(List values, @Nullable QueryParseContext context) {
        switch (values.size()) {
        case 0:
            return Queries.newMatchNoDocsFilter();
        case 1:
            // When there is a single term, it's important to return a term filter so that
            // it can return a DocIdSet that is directly backed by a postings list, instead
            // of loading everything into a bit set and returning an iterator based on the
            // bit set
            return termFilter(values.get(0), context);
        default:
            BytesRef[] bytesRefs = new BytesRef[values.size()];
            for (int i = 0; i < bytesRefs.length; i++) {
                bytesRefs[i] = indexedValueForSearch(values.get(i));
            }
            return Queries.wrap(new TermsQuery(names.indexName(), bytesRefs));
            
        }
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
        return Queries.wrap(new TermRangeQuery(names.indexName(),
                lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
                upperTerm == null ? null : indexedValueForSearch(upperTerm),
                includeLower, includeUpper));
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
        return Queries.wrap(new PrefixQuery(names().createIndexNameTerm(indexedValueForSearch(value))));
    }

    @Override
    public Query regexpQuery(Object value, int flags, int maxDeterminizedStates, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context) {
        RegexpQuery query = new RegexpQuery(names().createIndexNameTerm(indexedValueForSearch(value)), flags, maxDeterminizedStates);
        if (method != null) {
            query.setRewriteMethod(method);
        }
        return query;
    }

    @Override
    public Filter regexpFilter(Object value, int flags, int maxDeterminizedStates, @Nullable QueryParseContext parseContext) {
        return Queries.wrap(new RegexpQuery(names().createIndexNameTerm(indexedValueForSearch(value)), flags, maxDeterminizedStates));
    }

    @Override
    public Filter nullValueFilter() {
        return null;
    }

    @Override
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        if (!this.getClass().equals(mergeWith.getClass())) {
            String mergedType = mergeWith.getClass().getSimpleName();
            if (mergeWith instanceof AbstractFieldMapper) {
                mergedType = ((AbstractFieldMapper) mergeWith).contentType();
            }
            mergeResult.addConflict("mapper [" + names.fullName() + "] of different type, current_type [" + contentType() + "], merged_type [" + mergedType + "]");
            // different types, return
            return;
        }
        AbstractFieldMapper fieldMergeWith = (AbstractFieldMapper) mergeWith;
        boolean indexed =  fieldType.indexOptions() != IndexOptions.NONE;
        boolean mergeWithIndexed = fieldMergeWith.fieldType().indexOptions() != IndexOptions.NONE;
        if (indexed != mergeWithIndexed || this.fieldType().tokenized() != fieldMergeWith.fieldType().tokenized()) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different index values");
        }
        if (this.fieldType().stored() != fieldMergeWith.fieldType().stored()) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different store values");
        }
        if (!this.hasDocValues() && fieldMergeWith.hasDocValues()) {
            // don't add conflict if this mapper has doc values while the mapper to merge doesn't since doc values are implicitely set
            // when the doc_values field data format is configured
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different " + TypeParsers.DOC_VALUES + " values");
        }
        if (this.fieldType().omitNorms() && !fieldMergeWith.fieldType.omitNorms()) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] cannot enable norms (`norms.enabled`)");
        }
        if (this.fieldType().tokenized() != fieldMergeWith.fieldType().tokenized()) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different tokenize values");
        }
        if (this.fieldType().storeTermVectors() != fieldMergeWith.fieldType().storeTermVectors()) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different store_term_vector values");
        }
        if (this.fieldType().storeTermVectorOffsets() != fieldMergeWith.fieldType().storeTermVectorOffsets()) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different store_term_vector_offsets values");
        }
        if (this.fieldType().storeTermVectorPositions() != fieldMergeWith.fieldType().storeTermVectorPositions()) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different store_term_vector_positions values");
        }
        if (this.fieldType().storeTermVectorPayloads() != fieldMergeWith.fieldType().storeTermVectorPayloads()) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different store_term_vector_payloads values");
        }
        
        // null and "default"-named index analyzers both mean the default is used
        if (this.indexAnalyzer == null || "default".equals(this.indexAnalyzer.name())) {
            if (fieldMergeWith.indexAnalyzer != null && !"default".equals(fieldMergeWith.indexAnalyzer.name())) {
                mergeResult.addConflict("mapper [" + names.fullName() + "] has different analyzer");
            }
        } else if (fieldMergeWith.indexAnalyzer == null || "default".equals(fieldMergeWith.indexAnalyzer.name())) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different analyzer");
        } else if (!this.indexAnalyzer.name().equals(fieldMergeWith.indexAnalyzer.name())) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different analyzer");
        }
        
        if (!this.names().equals(fieldMergeWith.names())) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different index_name");
        }

        if (this.similarity == null) {
            if (fieldMergeWith.similarity() != null) {
                mergeResult.addConflict("mapper [" + names.fullName() + "] has different similarity");
            }
        } else if (fieldMergeWith.similarity() == null) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different similarity");
        } else if (!this.similarity().equals(fieldMergeWith.similarity())) {
            mergeResult.addConflict("mapper [" + names.fullName() + "] has different similarity");
        }
        multiFields.merge(mergeWith, mergeResult);

        if (!mergeResult.simulate()) {
            // apply changeable values
            this.fieldType = new FieldType(this.fieldType);
            this.fieldType.setOmitNorms(fieldMergeWith.fieldType.omitNorms());
            this.fieldType.freeze();
            this.boost = fieldMergeWith.boost;
            this.normsLoading = fieldMergeWith.normsLoading;
            this.copyTo = fieldMergeWith.copyTo;
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
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(names.name());
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);
        doXContentBody(builder, includeDefaults, params);
        return builder.endObject();
    }

    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {

        builder.field("type", contentType());
        if (indexCreatedBefore2x && (includeDefaults || !names.name().equals(names.indexNameClean()))) {
            builder.field("index_name", names.indexNameClean());
        }

        if (includeDefaults || boost != 1.0f) {
            builder.field("boost", boost);
        }

        FieldType defaultFieldType = defaultFieldType();
        boolean indexed =  fieldType.indexOptions() != IndexOptions.NONE;
        boolean defaultIndexed = defaultFieldType.indexOptions() != IndexOptions.NONE;
        if (includeDefaults || indexed != defaultIndexed ||
                fieldType.tokenized() != defaultFieldType.tokenized()) {
            builder.field("index", indexTokenizeOptionToString(indexed, fieldType.tokenized()));
        }
        if (includeDefaults || fieldType.stored() != defaultFieldType.stored()) {
            builder.field("store", fieldType.stored());
        }
        doXContentDocValues(builder, includeDefaults);
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
        if (indexed && (includeDefaults || fieldType.indexOptions() != defaultFieldType.indexOptions())) {
            builder.field("index_options", indexOptionToString(fieldType.indexOptions()));
        }

        doXContentAnalyzers(builder, includeDefaults);

        if (similarity() != null) {
            builder.field("similarity", similarity().name());
        } else if (includeDefaults) {
            builder.field("similarity", SimilarityLookupService.DEFAULT_SIMILARITY);
        }

        TreeMap<String, Object> orderedFielddataSettings = new TreeMap<>();
        if (customFieldDataSettings != null) {
            orderedFielddataSettings.putAll(customFieldDataSettings.getAsMap());
            builder.field("fielddata", orderedFielddataSettings);
        } else if (includeDefaults) {
            orderedFielddataSettings.putAll(fieldDataType.getSettings().getAsMap());
            builder.field("fielddata", orderedFielddataSettings);
        }
        multiFields.toXContent(builder, params);

        if (copyTo != null) {
            copyTo.toXContent(builder, params);
        }
    }
    
    protected void doXContentAnalyzers(XContentBuilder builder, boolean includeDefaults) throws IOException {
        if (indexAnalyzer == null) {
            if (includeDefaults) {
                builder.field("analyzer", "default");
            }
        } else if (includeDefaults || indexAnalyzer.name().startsWith("_") == false && indexAnalyzer.name().equals("default") == false) {
            builder.field("analyzer", indexAnalyzer.name());
            if (searchAnalyzer.name().equals(indexAnalyzer.name()) == false) {
                builder.field("search_analyzer", searchAnalyzer.name());
            }
        }
    }
    
    protected void doXContentDocValues(XContentBuilder builder, boolean includeDefaults) throws IOException {
        if (includeDefaults || docValues != null) {
            builder.field(TypeParsers.DOC_VALUES, hasDocValues());
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
            case DOCS:
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
        public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
            AbstractFieldMapper mergeWithMultiField = (AbstractFieldMapper) mergeWith;

            List<FieldMapper<?>> newFieldMappers = null;
            ImmutableOpenMap.Builder<String, Mapper> newMappersBuilder = null;

            for (ObjectCursor<Mapper> cursor : mergeWithMultiField.multiFields.mappers.values()) {
                Mapper mergeWithMapper = cursor.value;
                Mapper mergeIntoMapper = mappers.get(mergeWithMapper.name());
                if (mergeIntoMapper == null) {
                    // no mapping, simply add it if not simulating
                    if (!mergeResult.simulate()) {
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
                    mergeIntoMapper.merge(mergeWithMapper, mergeResult);
                }
            }

            // first add all field mappers
            if (newFieldMappers != null) {
                mergeResult.addFieldMappers(newFieldMappers);
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

                ObjectMapper mapper = context.root();
                String objectPath = "";
                String fieldPath = field;
                int posDot = field.lastIndexOf('.');
                if (posDot > 0) {
                    objectPath = field.substring(0, posDot);
                    context.path().add(objectPath);
                    mapper = context.docMapper().objectMappers().get(objectPath);
                    fieldPath = field.substring(posDot + 1);
                }
                if (mapper == null) {
                    //TODO: Create an object dynamically?
                    throw new MapperParsingException("attempt to copy value to non-existing object [" + field + "]");
                }
                ObjectMapper update = mapper.parseDynamicValue(context, fieldPath, context.parser().currentToken());
                assert update != null; // we are parsing a dynamic value so we necessarily created a new mapping

                // propagate the update to the root
                while (objectPath.length() > 0) {
                    String parentPath = "";
                    ObjectMapper parent = context.root();
                    posDot = objectPath.lastIndexOf('.');
                    if (posDot > 0) {
                        parentPath = objectPath.substring(0, posDot);
                        parent = context.docMapper().objectMappers().get(parentPath);
                    }
                    if (parent == null) {
                        throw new ElasticsearchIllegalStateException("[" + objectPath + "] has no parent for path [" + parentPath + "]");
                    }
                    update = parent.mappingUpdate(update);
                    objectPath = parentPath;
                }
                context.addDynamicMappingsUpdate((RootObjectMapper) update);
            }
        }
    }

    /**
     * Returns if this field is only generated when indexing. For example, the field of type token_count
     */
    @Override
    public boolean isGenerated() {
        return false;
    }

    @Override
    public FieldStats stats(Terms terms, int maxDoc) throws IOException {
        return new FieldStats.Text(
                maxDoc, terms.getDocCount(), terms.getSumDocFreq(), terms.getSumTotalTermFreq(), terms.getMin(), terms.getMax()
        );
    }
}
