/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
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
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.DynamicFieldType;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.TextParams;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
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
 * As an example, given a flattened field called 'field' and the following input
 *
 * {
 *   "field": {
 *     "key1": "some value",
 *     "key2": {
 *       "key3": true
 *     }
 *   }
 * }
 *
 * the mapper will produce untokenized string fields with the name "field" and values
 * "some value" and "true", as well as string fields called "field._keyed" with values
 * "key\0some value" and "key2.key3\0true". Note that \0 is used as a reserved separator
 *  character (see {@link FlattenedFieldParser#SEPARATOR}).
 */
public final class FlattenedFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "flattened";
    private static final String KEYED_FIELD_SUFFIX = "._keyed";

    private static class Defaults {
        public static final int DEPTH_LIMIT = 20;
    }

    private static Builder builder(Mapper in) {
        return ((FlattenedFieldMapper)in).builder;
    }

    public static class Builder extends FieldMapper.Builder {

        final Parameter<Integer> depthLimit
            = Parameter.intParam("depth_limit", true, m -> builder(m).depthLimit.get(), Defaults.DEPTH_LIMIT)
            .addValidator(v -> {
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
        public FlattenedFieldMapper build(ContentPath contentPath) {
            MultiFields multiFields = multiFieldsBuilder.build(this, contentPath);
            if (multiFields.iterator().hasNext()) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + name + "] does not support [fields]");
            }
            CopyTo copyTo = this.copyTo.build();
            if (copyTo.copyToFields().isEmpty() == false) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + name + "] does not support [copy_to]");
            }
            MappedFieldType ft = new RootFlattenedFieldType(
                buildFullName(contentPath),
                indexed.get(),
                hasDocValues.get(),
                meta.get(),
                splitQueriesOnWhitespace.get(),
                eagerGlobalOrdinals.get());
            return new FlattenedFieldMapper(name, ft, this);
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    /**
     * A field type that represents the values under a particular JSON key, used
     * when searching under a specific key as in 'my_flattened.key: some_value'.
     */
    public static final class KeyedFlattenedFieldType extends StringFieldType {
        private final String key;
        private final String rootName;

        KeyedFlattenedFieldType(String rootName, boolean indexed, boolean hasDocValues, String key,
                                       boolean splitQueriesOnWhitespace, Map<String, String> meta) {
            super(rootName + KEYED_FIELD_SUFFIX, indexed, false, hasDocValues,
                splitQueriesOnWhitespace ? TextSearchInfo.WHITESPACE_MATCH_ONLY : TextSearchInfo.SIMPLE_MATCH_ONLY,
                meta);
            this.key = key;
            this.rootName = rootName;
        }

        private KeyedFlattenedFieldType(String rootName, String key, RootFlattenedFieldType ref) {
            this(rootName, ref.isSearchable(), ref.hasDocValues(), key, ref.splitQueriesOnWhitespace, ref.meta());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public String key() {
            return key;
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            Term term = new Term(name(), FlattenedFieldParser.createKeyedValue(key, ""));
            return new PrefixQuery(term);
        }

        @Override
        public Query rangeQuery(Object lowerTerm,
                                Object upperTerm,
                                boolean includeLower,
                                boolean includeUpper,
                                SearchExecutionContext context) {

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
                                boolean transpositions, SearchExecutionContext context) {
            throw new UnsupportedOperationException("[fuzzy] queries are not currently supported on keyed " +
                "[" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query regexpQuery(String value, int syntaxFlags, int matchFlags, int maxDeterminizedStates,
                                 MultiTermQuery.RewriteMethod method, SearchExecutionContext context) {
            throw new UnsupportedOperationException("[regexp] queries are not currently supported on keyed " +
                "[" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query wildcardQuery(String value,
                                   MultiTermQuery.RewriteMethod method,
                                   boolean caseInsensitive,
                                   SearchExecutionContext context) {
            throw new UnsupportedOperationException("[wildcard] queries are not currently supported on keyed " +
                "[" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query termQueryCaseInsensitive(Object value, SearchExecutionContext context) {
            return AutomatonQueries.caseInsensitiveTermQuery(new Term(name(), indexedValueForSearch(value)));
        }

        @Override
        public TermsEnum getTerms(boolean caseInsensitive, String string, SearchExecutionContext queryShardContext, String searchAfter)
            throws IOException {
            IndexReader reader = queryShardContext.searcher().getTopReaderContext().reader();
            Terms terms = MultiTerms.getTerms(reader, name());
            if (terms == null) {
                // Field does not exist on this shard.
                return null;
            }

            Automaton a = Automata.makeString(key + FlattenedFieldParser.SEPARATOR);
            if (caseInsensitive) {
                a = Operations.concatenate(a, AutomatonQueries.caseInsensitivePrefix(string));
            } else {
                a = Operations.concatenate(a, Automata.makeString(string));
                a = Operations.concatenate(a, Automata.makeAnyString());
            }
            a = MinimizationOperations.minimize(a, Integer.MAX_VALUE);

            CompiledAutomaton automaton = new CompiledAutomaton(a);
            if (searchAfter != null) {
                BytesRef searchAfterWithFieldName = new BytesRef(key + FlattenedFieldParser.SEPARATOR + searchAfter);
                TermsEnum seekedEnum = terms.intersect(automaton, searchAfterWithFieldName);
                return new TranslatingTermsEnum(seekedEnum);
            } else {
                return new TranslatingTermsEnum(automaton.getTermsEnum(terms));
            }
        }

        @Override
        public BytesRef indexedValueForSearch(Object value) {
            if (value == null) {
                return null;
            }

            String stringValue = value instanceof BytesRef
                ? ((BytesRef) value).utf8ToString()
                : value.toString();
            String keyedValue = FlattenedFieldParser.createKeyedValue(key, stringValue);
            return new BytesRef(keyedValue);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new KeyedFlattenedFieldData.Builder(name(), key, CoreValuesSourceType.KEYWORD);
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + rootName + "." + key + "] of type [" + typeName() +
                    "] doesn't support formats.");
            }
            return SourceValueFetcher.identity(rootName + "." + key, context, format);
        }
    }


    // Wraps a raw Lucene TermsEnum to strip values of fieldnames
    static class TranslatingTermsEnum extends TermsEnum {
        TermsEnum delegate;

        TranslatingTermsEnum(TermsEnum delegate) {
            this.delegate = delegate;
        }

        @Override
        public BytesRef next() throws IOException {
            // Strip the term of the fieldname value
            BytesRef result = delegate.next();
            if (result != null) {
                result = FlattenedFieldParser.extractValue(result);
            }
            return result;
        }

        @Override
        public BytesRef term() throws IOException {
            // Strip the term of the fieldname value
            BytesRef result = delegate.term();
            if (result != null) {
                result = FlattenedFieldParser.extractValue(result);
            }
            return result;
        }


        @Override
        public int docFreq() throws IOException {
            return delegate.docFreq();
        }

        //===============  All other TermsEnum methods not supported =================

        @Override
        public AttributeSource attributes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean seekExact(BytesRef text) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void seekExact(long ord) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void seekExact(BytesRef term, TermState state) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long ord() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long totalTermFreq() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public ImpactsEnum impacts(int flags) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public TermState termState() throws IOException {
            throw new UnsupportedOperationException();
        }

    }

    /**
     * A field data implementation that gives access to the values associated with
     * a particular JSON key.
     *
     * This class wraps the field data that is built directly on the keyed flattened field,
     * and filters out values whose prefix doesn't match the requested key. Loading and caching
     * is fully delegated to the wrapped field data, so that different {@link KeyedFlattenedFieldData}
     * for the same flattened field share the same global ordinals.
     *
     * Because of the code-level complexity it would introduce, it is currently not possible
     * to retrieve the underlying global ordinals map through {@link #getOrdinalMap()}.
     */
    public static class KeyedFlattenedFieldData implements IndexOrdinalsFieldData {
        private final String key;
        private final IndexOrdinalsFieldData delegate;

        private KeyedFlattenedFieldData(String key, IndexOrdinalsFieldData delegate) {
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
            return new KeyedFlattenedLeafFieldData(key, fieldData);
        }

        @Override
        public LeafOrdinalsFieldData loadDirect(LeafReaderContext context) throws Exception {
            LeafOrdinalsFieldData fieldData = delegate.loadDirect(context);
            return new KeyedFlattenedLeafFieldData(key, fieldData);
        }

        @Override
        public IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader) {
            IndexOrdinalsFieldData fieldData = delegate.loadGlobal(indexReader);
            return new KeyedFlattenedFieldData(key, fieldData);
        }

        @Override
        public IndexOrdinalsFieldData loadGlobalDirect(DirectoryReader indexReader) throws Exception {
            IndexOrdinalsFieldData fieldData = delegate.loadGlobalDirect(indexReader);
            return new KeyedFlattenedFieldData(key, fieldData);
        }

        @Override
        public OrdinalMap getOrdinalMap() {
            throw new UnsupportedOperationException("The field data for the flattened field ["
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
                return new KeyedFlattenedFieldData(key, delegate);
            }
        }
    }

    /**
     * A field type that represents all 'root' values. This field type is used in
     * searches on the flattened field itself, e.g. 'my_flattened: some_value'.
     */
    public static final class RootFlattenedFieldType extends StringFieldType implements DynamicFieldType {
        private final boolean splitQueriesOnWhitespace;
        private final boolean eagerGlobalOrdinals;

        public RootFlattenedFieldType(String name, boolean indexed, boolean hasDocValues, Map<String, String> meta,
                                      boolean splitQueriesOnWhitespace, boolean eagerGlobalOrdinals) {
            super(name, indexed, false, hasDocValues,
                splitQueriesOnWhitespace ? TextSearchInfo.WHITESPACE_MATCH_ONLY : TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            this.splitQueriesOnWhitespace = splitQueriesOnWhitespace;
            this.eagerGlobalOrdinals = eagerGlobalOrdinals;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean eagerGlobalOrdinals() {
            return eagerGlobalOrdinals;
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
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), CoreValuesSourceType.KEYWORD);
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.identity(name(), context, format);
        }

        @Override
        public MappedFieldType getChildFieldType(String childPath) {
            return new KeyedFlattenedFieldType(name(), childPath, this);
        }
    }

    private final FlattenedFieldParser fieldParser;
    private final Builder builder;

    private FlattenedFieldMapper(String simpleName,
                                 MappedFieldType mappedFieldType,
                                 Builder builder) {
        super(simpleName, mappedFieldType, Lucene.KEYWORD_ANALYZER, MultiFields.empty(), CopyTo.empty());
        this.builder = builder;
        this.fieldParser = new FlattenedFieldParser(mappedFieldType.name(), mappedFieldType.name() + KEYED_FIELD_SUFFIX,
            mappedFieldType, builder.depthLimit.get(), builder.ignoreAbove.get(), builder.nullValue.get());
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    int depthLimit() {
        return builder.depthLimit.get();
    }

    int ignoreAbove() {
        return builder.ignoreAbove.get();
    }

    @Override
    public RootFlattenedFieldType fieldType() {
        return (RootFlattenedFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
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
            context.addToFieldNames(fieldType().name());
        }
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }
}
