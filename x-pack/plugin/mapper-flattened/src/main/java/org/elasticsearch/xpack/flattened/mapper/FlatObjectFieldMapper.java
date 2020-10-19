/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.flattened.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.DynamicKeyFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParametrizedFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.TextParams;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A field mapper that accepts a JSON object and flattens it into a single field. This data type
 * can be a useful alternative to an 'object' mapping when the object has a large, unknown set
 * of keys.
 *
 * Currently the mapper extracts all leaf values of the JSON object, converts them to their text
 * representations, and indexes each one as a keyword. It creates both a 'keyed' version of the token
 * to allow searches on particular key-value pairs, as well as a 'root' token without the key
 *
 * As an example, given a flat object field called 'flat_object' and the following input
 *
 * {
 *   "flat_object": {
 *     "key1": "some value",
 *     "key2": {
 *       "key3": true
 *     }
 *   }
 * }
 *
 * the mapper will produce untokenized string fields with the name "flat_object" and values
 * "some value" and "true", as well as string fields called "flat_object._keyed" with values
 * "key\0some value" and "key2.key3\0true". Note that \0 is used as a reserved separator
 *  character (see {@link FlatObjectFieldParser#SEPARATOR}).
 */
public final class FlatObjectFieldMapper extends DynamicKeyFieldMapper {

    public static final String CONTENT_TYPE = "flattened";
    private static final String KEYED_FIELD_SUFFIX = "._keyed";

    private static class Defaults {
        public static final int DEPTH_LIMIT = 20;
    }

    private static Builder builder(Mapper in) {
        return ((FlatObjectFieldMapper)in).builder;
    }

    public static class Builder extends ParametrizedFieldMapper.Builder {

        final Parameter<Integer> depthLimit
            = Parameter.intParam("depth_limit", true, m -> builder(m).depthLimit.get(), Defaults.DEPTH_LIMIT)
            .setValidator(v -> {
                if (v < 0) {
                    throw new IllegalArgumentException("[depth_limit] must be positive, got [" + v + "]");
                }
            });

        private final Parameter<Boolean> indexed = Parameter.indexParam(m -> builder(m).indexed.get(), true);
        private final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> builder(m).hasDocValues.get(), true);

        private final Parameter<String> nullValue
            = Parameter.stringParam("null_value", false, m -> builder(m).nullValue.get(), null).acceptsNull();

        private final Parameter<Boolean> eagerGlobalOrdinals
            = Parameter.boolParam("eager_global_ordinals", true, m -> builder(m).eagerGlobalOrdinals.get(), false);
        private final Parameter<Integer> ignoreAbove
            = Parameter.intParam("ignore_above", true, m -> builder(m).ignoreAbove.get(), Integer.MAX_VALUE);

        private final Parameter<String> indexOptions
            = Parameter.restrictedStringParam("index_options", false, m -> builder(m).indexOptions.get(), "docs", "freqs");
        private final Parameter<SimilarityProvider> similarity = TextParams.similarity(m -> builder(m).similarity.get());

        private final Parameter<Boolean> splitQueriesOnWhitespace
            = Parameter.boolParam("split_queries_on_whitespace", true, m -> builder(m).splitQueriesOnWhitespace.get(), false);

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(indexed, hasDocValues, depthLimit, nullValue, eagerGlobalOrdinals, ignoreAbove,
                indexOptions, similarity, splitQueriesOnWhitespace, meta);
        }

        @Override
        public FlatObjectFieldMapper build(BuilderContext context) {
            MultiFields multiFields = multiFieldsBuilder.build(this, context);
            if (multiFields.iterator().hasNext()) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + name + "] does not support [fields]");
            }
            CopyTo copyTo = this.copyTo.build();
            if (copyTo.copyToFields().isEmpty() == false) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + name + "] does not support [copy_to]");
            }
            MappedFieldType ft = new RootFlatObjectFieldType(
                buildFullName(context), indexed.get(), hasDocValues.get(), meta.get(), splitQueriesOnWhitespace.get());
            if (eagerGlobalOrdinals.get()) {
                ft.setEagerGlobalOrdinals(true);
            }
            return new FlatObjectFieldMapper(name, ft, this);
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    /**
     * A field type that represents the values under a particular JSON key, used
     * when searching under a specific key as in 'my_flat_object.key: some_value'.
     */
    public static final class KeyedFlatObjectFieldType extends StringFieldType {
        private final String key;

        public KeyedFlatObjectFieldType(String name, boolean indexed, boolean hasDocValues, String key,
                                        boolean splitQueriesOnWhitespace, Map<String, String> meta) {
            super(name, indexed, false, hasDocValues,
                splitQueriesOnWhitespace ? TextSearchInfo.WHITESPACE_MATCH_ONLY : TextSearchInfo.SIMPLE_MATCH_ONLY,
                meta);
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            this.key = key;
        }

        private KeyedFlatObjectFieldType(String name, String key, RootFlatObjectFieldType ref) {
            this(name, ref.isSearchable(), ref.hasDocValues(), key, ref.splitQueriesOnWhitespace, ref.meta());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public String key() {
            return key;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            Term term = new Term(name(), FlatObjectFieldParser.createKeyedValue(key, ""));
            return new PrefixQuery(term);
        }

        @Override
        public Query rangeQuery(Object lowerTerm,
                                Object upperTerm,
                                boolean includeLower,
                                boolean includeUpper,
                                QueryShardContext context) {

            // We require range queries to specify both bounds because an unbounded query could incorrectly match
            // values from other keys. For example, a query on the 'first' key with only a lower bound would become
            // ("first\0value", null), which would also match the value "second\0value" belonging to the key 'second'.
            if (lowerTerm == null || upperTerm == null) {
                throw new IllegalArgumentException("[range] queries on keyed [" + CONTENT_TYPE +
                    "] fields must include both an upper and a lower bound.");
            }

            return super.rangeQuery(lowerTerm, upperTerm,
                includeLower, includeUpper, context);
        }

        @Override
        public Query fuzzyQuery(Object value, Fuzziness fuzziness, int prefixLength, int maxExpansions,
                                boolean transpositions, QueryShardContext context) {
            throw new UnsupportedOperationException("[fuzzy] queries are not currently supported on keyed " +
                "[" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query regexpQuery(String value, int syntaxFlags, int matchFlags, int maxDeterminizedStates,
                                 MultiTermQuery.RewriteMethod method, QueryShardContext context) {
            throw new UnsupportedOperationException("[regexp] queries are not currently supported on keyed " +
                "[" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query wildcardQuery(String value,
                                   MultiTermQuery.RewriteMethod method,
                                   boolean caseInsensitive,
                                   QueryShardContext context) {
            throw new UnsupportedOperationException("[wildcard] queries are not currently supported on keyed " +
                "[" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query termQueryCaseInsensitive(Object value, QueryShardContext context) {
            return AutomatonQueries.caseInsensitiveTermQuery(new Term(name(), indexedValueForSearch(value)));
        }

        @Override
        public BytesRef indexedValueForSearch(Object value) {
            if (value == null) {
                return null;
            }

            String stringValue = value instanceof BytesRef
                ? ((BytesRef) value).utf8ToString()
                : value.toString();
            String keyedValue = FlatObjectFieldParser.createKeyedValue(key, stringValue);
            return new BytesRef(keyedValue);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new KeyedFlatObjectFieldData.Builder(name(), key, CoreValuesSourceType.BYTES);
        }

        @Override
        public ValueFetcher valueFetcher(MapperService mapperService, SearchLookup searchLookup, String format) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * A field data implementation that gives access to the values associated with
     * a particular JSON key.
     *
     * This class wraps the field data that is built directly on the keyed flat object field,
     * and filters out values whose prefix doesn't match the requested key. Loading and caching
     * is fully delegated to the wrapped field data, so that different {@link KeyedFlatObjectFieldData}
     * for the same flat object field share the same global ordinals.
     *
     * Because of the code-level complexity it would introduce, it is currently not possible
     * to retrieve the underlying global ordinals map through {@link #getOrdinalMap()}.
     */
    public static class KeyedFlatObjectFieldData implements IndexOrdinalsFieldData {
        private final String key;
        private final IndexOrdinalsFieldData delegate;

        private KeyedFlatObjectFieldData(String key, IndexOrdinalsFieldData delegate) {
            this.delegate = delegate;
            this.key = key;
        }

        public String getKey() {
            return key;
        }

        @Override
        public String getFieldName() {
            return delegate.getFieldName();
        }

        @Override
        public ValuesSourceType getValuesSourceType() {
            return delegate.getValuesSourceType();
        }

        @Override
        public SortField sortField(Object missingValue,
                                   MultiValueMode sortMode,
                                   XFieldComparatorSource.Nested nested,
                                   boolean reverse) {
            XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
            return new SortField(getFieldName(), source, reverse);
        }

        @Override
        public BucketedSort newBucketedSort(BigArrays bigArrays, Object missingValue, MultiValueMode sortMode, Nested nested,
                SortOrder sortOrder, DocValueFormat format, int bucketSize, BucketedSort.ExtraData extra) {
            throw new IllegalArgumentException("only supported on numeric fields");
        }

        @Override
        public LeafOrdinalsFieldData load(LeafReaderContext context) {
            LeafOrdinalsFieldData fieldData = delegate.load(context);
            return new KeyedFlatObjectLeafFieldData(key, fieldData);
        }

        @Override
        public LeafOrdinalsFieldData loadDirect(LeafReaderContext context) throws Exception {
            LeafOrdinalsFieldData fieldData = delegate.loadDirect(context);
            return new KeyedFlatObjectLeafFieldData(key, fieldData);
        }

        @Override
        public IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader) {
            IndexOrdinalsFieldData fieldData = delegate.loadGlobal(indexReader);
            return new KeyedFlatObjectFieldData(key, fieldData);
        }

        @Override
        public IndexOrdinalsFieldData loadGlobalDirect(DirectoryReader indexReader) throws Exception {
            IndexOrdinalsFieldData fieldData = delegate.loadGlobalDirect(indexReader);
            return new KeyedFlatObjectFieldData(key, fieldData);
        }

        @Override
        public OrdinalMap getOrdinalMap() {
            throw new UnsupportedOperationException("The field data for the flat object field ["
                + delegate.getFieldName() + "] does not allow access to the underlying ordinal map.");
        }

        @Override
        public boolean supportsGlobalOrdinalsMapping() {
            return false;
        }

        public static class Builder implements IndexFieldData.Builder {
            private final String fieldName;
            private final String key;
            private final ValuesSourceType valuesSourceType;

            Builder(String fieldName, String key, ValuesSourceType valuesSourceType) {
                this.fieldName = fieldName;
                this.key = key;
                this.valuesSourceType = valuesSourceType;
            }

            @Override
            public IndexFieldData<?> build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
                IndexOrdinalsFieldData delegate = new SortedSetOrdinalsIndexFieldData(
                    cache, fieldName, valuesSourceType, breakerService, AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION);
                return new KeyedFlatObjectFieldData(key, delegate);
            }
        }
    }

    /**
     * A field type that represents all 'root' values. This field type is used in
     * searches on the flat object field itself, e.g. 'my_flat_object: some_value'.
     */
    public static final class RootFlatObjectFieldType extends StringFieldType {
        private final boolean splitQueriesOnWhitespace;

        public RootFlatObjectFieldType(String name, boolean indexed, boolean hasDocValues, Map<String, String> meta,
                                       boolean splitQueriesOnWhitespace) {
            super(name, indexed, false, hasDocValues,
                splitQueriesOnWhitespace ? TextSearchInfo.WHITESPACE_MATCH_ONLY : TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            this.splitQueriesOnWhitespace = splitQueriesOnWhitespace;
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            BytesRef binaryValue = (BytesRef) value;
            return binaryValue.utf8ToString();
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), CoreValuesSourceType.BYTES);
        }

        @Override
        public ValueFetcher valueFetcher(MapperService mapperService, SearchLookup searchLookup, String format) {
            return SourceValueFetcher.identity(name(), mapperService, format);
        }
    }

    private final FlatObjectFieldParser fieldParser;
    private final Builder builder;

    private FlatObjectFieldMapper(String simpleName,
                                  MappedFieldType mappedFieldType,
                                  Builder builder) {
        super(simpleName, mappedFieldType, CopyTo.empty());
        this.builder = builder;
        this.fieldParser = new FlatObjectFieldParser(mappedFieldType.name(), keyedFieldName(),
            mappedFieldType, builder.depthLimit.get(), builder.ignoreAbove.get(), builder.nullValue.get());
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected FlatObjectFieldMapper clone() {
        return (FlatObjectFieldMapper) super.clone();
    }

    int depthLimit() {
        return builder.depthLimit.get();
    }

    int ignoreAbove() {
        return builder.ignoreAbove.get();
    }

    @Override
    public RootFlatObjectFieldType fieldType() {
        return (RootFlatObjectFieldType) super.fieldType();
    }

    @Override
    public KeyedFlatObjectFieldType keyedFieldType(String key) {
        return new KeyedFlatObjectFieldType(keyedFieldName(), key, fieldType());
    }

    public String keyedFieldName() {
        return mappedFieldType.name() + KEYED_FIELD_SUFFIX;
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        if (context.parser().currentToken() == XContentParser.Token.VALUE_NULL) {
            return;
        }

        if (mappedFieldType.isSearchable() == false && mappedFieldType.hasDocValues() == false) {
            context.parser().skipChildren();
            return;
        }

        XContentParser xContentParser = context.parser();
        context.doc().addAll(fieldParser.parse(xContentParser));

        if (mappedFieldType.hasDocValues() == false) {
            createFieldNamesField(context);
        }
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }
}
