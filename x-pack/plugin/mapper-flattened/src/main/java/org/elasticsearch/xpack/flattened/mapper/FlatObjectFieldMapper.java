/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.flattened.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.DynamicKeyFieldMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.mapper.TypeParsers.parseField;

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
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }

        public static final int DEPTH_LIMIT = 20;
        public static final int IGNORE_ABOVE = Integer.MAX_VALUE;
    }

    public static class Builder extends FieldMapper.Builder<Builder> {
        private int depthLimit = Defaults.DEPTH_LIMIT;
        private int ignoreAbove = Defaults.IGNORE_ABOVE;
        private String nullValue = null;
        private boolean eagerGlobalOrdinals = false;
        private boolean splitQueriesOnWhitespace = false;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public Builder indexOptions(IndexOptions indexOptions) {
            if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) > 0) {
                throw new IllegalArgumentException("The [" + CONTENT_TYPE
                    + "] field does not support positions, got [index_options]="
                    + indexOptionToString(indexOptions));
            }
            return super.indexOptions(indexOptions);
        }

        public Builder depthLimit(int depthLimit) {
            if (depthLimit < 0) {
                throw new IllegalArgumentException("[depth_limit] must be positive, got " + depthLimit);
            }
            this.depthLimit = depthLimit;
            return this;
        }

        public Builder eagerGlobalOrdinals(boolean eagerGlobalOrdinals) {
            this.eagerGlobalOrdinals = eagerGlobalOrdinals;
            return builder;
        }

        public Builder ignoreAbove(int ignoreAbove) {
            if (ignoreAbove < 0) {
                throw new IllegalArgumentException("[ignore_above] must be positive, got " + ignoreAbove);
            }
            this.ignoreAbove = ignoreAbove;
            return this;
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        public Builder splitQueriesOnWhitespace(boolean splitQueriesOnWhitespace) {
            this.splitQueriesOnWhitespace = splitQueriesOnWhitespace;
            return builder;
        }

        @Override
        public Builder addMultiField(Mapper.Builder<?> mapperBuilder) {
            throw new UnsupportedOperationException("[fields] is not supported for [" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Builder copyTo(CopyTo copyTo) {
            throw new UnsupportedOperationException("[copy_to] is not supported for [" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Builder store(boolean store) {
            throw new UnsupportedOperationException("[store] is not supported for [" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public FlatObjectFieldMapper build(BuilderContext context) {
            MappedFieldType ft = new RootFlatObjectFieldType(buildFullName(context), indexed, hasDocValues, meta, splitQueriesOnWhitespace);
            if (eagerGlobalOrdinals) {
                ft.setEagerGlobalOrdinals(true);
            }
            return new FlatObjectFieldMapper(name, fieldType, ft, ignoreAbove, depthLimit, nullValue);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);
            parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (propName.equals("depth_limit")) {
                    builder.depthLimit(XContentMapValues.nodeIntegerValue(propNode, -1));
                    iterator.remove();
                } else if (propName.equals("eager_global_ordinals")) {
                    builder.eagerGlobalOrdinals(XContentMapValues.nodeBooleanValue(propNode, "eager_global_ordinals"));
                    iterator.remove();
                } else if (propName.equals("ignore_above")) {
                    builder.ignoreAbove(XContentMapValues.nodeIntegerValue(propNode, -1));
                    iterator.remove();
                } else if (propName.equals("null_value")) {
                    if (propNode == null) {
                        throw new MapperParsingException("Property [null_value] cannot be null.");
                    }
                    builder.nullValue(propNode.toString());
                    iterator.remove();
                } else if (propName.equals("split_queries_on_whitespace")) {
                    builder.splitQueriesOnWhitespace
                        (XContentMapValues.nodeBooleanValue(propNode, "split_queries_on_whitespace"));
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    /**
     * A field type that represents the values under a particular JSON key, used
     * when searching under a specific key as in 'my_flat_object.key: some_value'.
     */
    public static final class KeyedFlatObjectFieldType extends StringFieldType {
        private final String key;
        private boolean splitQueriesOnWhitespace;

        public KeyedFlatObjectFieldType(String name, boolean indexed, boolean hasDocValues, String key,
                                        boolean splitQueriesOnWhitespace, Map<String, String> meta) {
            super(name, indexed, hasDocValues,
                splitQueriesOnWhitespace ? TextSearchInfo.WHITESPACE_MATCH_ONLY : TextSearchInfo.SIMPLE_MATCH_ONLY,
                meta);
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            this.key = key;
            this.splitQueriesOnWhitespace = splitQueriesOnWhitespace;
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

        public boolean splitQueriesOnWhitespace() {
            return splitQueriesOnWhitespace;
        }

        public void setSplitQueriesOnWhitespace(boolean splitQueriesOnWhitespace) {
            this.splitQueriesOnWhitespace = splitQueriesOnWhitespace;
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
        public Query regexpQuery(String value, int flags, int maxDeterminizedStates,
                                 MultiTermQuery.RewriteMethod method, QueryShardContext context) {
            throw new UnsupportedOperationException("[regexp] queries are not currently supported on keyed " +
                "[" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query wildcardQuery(String value,
                                   MultiTermQuery.RewriteMethod method,
                                   QueryShardContext context) {
            throw new UnsupportedOperationException("[wildcard] queries are not currently supported on keyed " +
                "[" + CONTENT_TYPE + "] fields.");
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
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new KeyedFlatObjectFieldData.Builder(name(), key, CoreValuesSourceType.BYTES);
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
        public void clear() {
            delegate.clear();
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
        public IndexOrdinalsFieldData localGlobalDirect(DirectoryReader indexReader) throws Exception {
            IndexOrdinalsFieldData fieldData = delegate.localGlobalDirect(indexReader);
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
            public IndexFieldData<?> build(IndexFieldDataCache cache, CircuitBreakerService breakerService, MapperService mapperService) {
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
        private boolean splitQueriesOnWhitespace;

        public RootFlatObjectFieldType(String name, boolean indexed, boolean hasDocValues, Map<String, String> meta,
                                       boolean splitQueriesOnWhitespace) {
            super(name, indexed, hasDocValues,
                splitQueriesOnWhitespace ? TextSearchInfo.WHITESPACE_MATCH_ONLY : TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            this.splitQueriesOnWhitespace = splitQueriesOnWhitespace;
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public boolean splitQueriesOnWhitespace() {
            return splitQueriesOnWhitespace;
        }

        public void setSplitQueriesOnWhitespace(boolean splitQueriesOnWhitespace) {
            this.splitQueriesOnWhitespace = splitQueriesOnWhitespace;
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
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            }
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), CoreValuesSourceType.BYTES);
        }

    }

    private FlatObjectFieldParser fieldParser;
    private final String nullValue;
    private int depthLimit;
    private int ignoreAbove;

    private FlatObjectFieldMapper(String simpleName,
                                  FieldType fieldType,
                                  MappedFieldType mappedFieldType,
                                  int ignoreAbove,
                                  int depthLimit,
                                  String nullValue) {
        super(simpleName, fieldType, mappedFieldType, CopyTo.empty());
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;

        this.depthLimit = depthLimit;
        this.ignoreAbove = ignoreAbove;
        this.nullValue = nullValue;
        this.fieldParser = new FlatObjectFieldParser(mappedFieldType.name(), keyedFieldName(),
            mappedFieldType, depthLimit, ignoreAbove, nullValue);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected String nullValue() {
        return nullValue;
    }

    @Override
    protected void mergeOptions(FieldMapper mergeWith, List<String> conflicts) {
        FlatObjectFieldMapper other = ((FlatObjectFieldMapper) mergeWith);
        if (Objects.equals(this.nullValue, other.nullValue) == false) {
            conflicts.add("mapper [" + name() + "] has different [null_value] settings");
        }
        this.depthLimit = other.depthLimit;
        this.ignoreAbove = other.ignoreAbove;
        this.fieldParser = new FlatObjectFieldParser(mappedFieldType.name(), keyedFieldName(),
            mappedFieldType, depthLimit, ignoreAbove, nullValue);
    }

    @Override
    protected FlatObjectFieldMapper clone() {
        return (FlatObjectFieldMapper) super.clone();
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

        if (fieldType.indexOptions() == IndexOptions.NONE && mappedFieldType.hasDocValues() == false) {
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
    protected Object parseSourceValue(Object value, String format) {
        if (format != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
        }
        return value;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (fieldType.indexOptions() != IndexOptions.NONE
            && (includeDefaults || fieldType.indexOptions() != Defaults.FIELD_TYPE.indexOptions())) {
            builder.field("index_options", indexOptionToString(fieldType.indexOptions()));
        }
        if (includeDefaults || depthLimit != Defaults.DEPTH_LIMIT) {
            builder.field("depth_limit", depthLimit);
        }
        if (includeDefaults || ignoreAbove != Defaults.IGNORE_ABOVE) {
            builder.field("ignore_above", ignoreAbove);
        }
        if (includeDefaults || fieldType().eagerGlobalOrdinals()) {
            builder.field("eager_global_ordinals", fieldType().eagerGlobalOrdinals());
        }
        if (nullValue != null) {
            builder.field("null_value", nullValue);
        }

        if (includeDefaults || fieldType().splitQueriesOnWhitespace()) {
            builder.field("split_queries_on_whitespace", fieldType().splitQueriesOnWhitespace());
        }
    }
}
