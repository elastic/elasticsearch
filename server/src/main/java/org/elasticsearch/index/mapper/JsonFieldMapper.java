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

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.AtomicOrdinalsFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.plain.AbstractAtomicOrdinalsFieldData;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetDVOrdinalsIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.MultiValueMode;

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
 * As an example, given a json field called 'json_field' and the following input
 *
 * {
 *   "json_field: {
 *     "key1": "some value",
 *     "key2": {
 *       "key3": true
 *     }
 *   }
 * }
 *
 * the mapper will produce untokenized string fields called "json_field" with values "some value" and "true",
 * as well as string fields called "json_field._keyed" with values "key\0some value" and "key2.key3\0true".
 *
 * Note that \0 is a reserved separator character, and cannot be used in the keys of the JSON object
 * (see {@link JsonFieldParser#SEPARATOR}).
 *
 * When 'store' is enabled, a single stored field is added containing the entire JSON object in
 * pretty-printed format.
 */
public final class JsonFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "json";
    public static final NamedAnalyzer WHITESPACE_ANALYZER = new NamedAnalyzer(
        "whitespace", AnalyzerScope.INDEX, new WhitespaceAnalyzer());
    private static final String KEYED_FIELD_SUFFIX = "._keyed";

    private static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new RootJsonFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }

        public static final int DEPTH_LIMIT = 20;
        public static final int IGNORE_ABOVE = Integer.MAX_VALUE;
    }

    public static class Builder extends FieldMapper.Builder<Builder, JsonFieldMapper> {
        private int depthLimit = Defaults.DEPTH_LIMIT;
        private int ignoreAbove = Defaults.IGNORE_ABOVE;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public RootJsonFieldType fieldType() {
            return (RootJsonFieldType) super.fieldType();
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
            fieldType().setEagerGlobalOrdinals(eagerGlobalOrdinals);
            return builder;
        }

        public Builder ignoreAbove(int ignoreAbove) {
            if (ignoreAbove < 0) {
                throw new IllegalArgumentException("[ignore_above] must be positive, got " + ignoreAbove);
            }
            this.ignoreAbove = ignoreAbove;
            return this;
        }

        public Builder splitQueriesOnWhitespace(boolean splitQueriesOnWhitespace) {
            fieldType().setSplitQueriesOnWhitespace(splitQueriesOnWhitespace);
            return builder;
        }

        @Override
        public Builder addMultiField(Mapper.Builder mapperBuilder) {
            throw new UnsupportedOperationException("[fields] is not supported for [" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Builder copyTo(CopyTo copyTo) {
            throw new UnsupportedOperationException("[copy_to] is not supported for [" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public JsonFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            if (fieldType().splitQueriesOnWhitespace()) {
                fieldType().setSearchAnalyzer(WHITESPACE_ANALYZER);
            }
            return new JsonFieldMapper(name, fieldType, defaultFieldType,
                ignoreAbove, depthLimit, context.indexSettings());
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?,?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            JsonFieldMapper.Builder builder = new JsonFieldMapper.Builder(name);
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
     * when searching under a specific key as in 'my_json_field.key: some_value'.
     */
    public static final class KeyedJsonFieldType extends StringFieldType {
        private final String key;
        private boolean splitQueriesOnWhitespace;

        public KeyedJsonFieldType(String key) {
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            this.key = key;
        }

        public KeyedJsonFieldType clone() {
            return new KeyedJsonFieldType(this);
        }

        private KeyedJsonFieldType(KeyedJsonFieldType ref) {
            super(ref);
            this.key = ref.key;
            this.splitQueriesOnWhitespace = ref.splitQueriesOnWhitespace;
        }

        private KeyedJsonFieldType(String name, String key, RootJsonFieldType ref) {
            super(ref);
            setName(name);
            this.key = key;
            this.splitQueriesOnWhitespace = ref.splitQueriesOnWhitespace;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            KeyedJsonFieldType that = (KeyedJsonFieldType) o;
            return splitQueriesOnWhitespace == that.splitQueriesOnWhitespace;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), splitQueriesOnWhitespace);
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
            checkIfFrozen();
            this.splitQueriesOnWhitespace = splitQueriesOnWhitespace;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            Term term = new Term(name(), JsonFieldParser.createKeyedValue(key, ""));
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
                                boolean transpositions) {
            throw new UnsupportedOperationException("[fuzzy] queries are not currently supported on [" +
                CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query regexpQuery(String value, int flags, int maxDeterminizedStates,
                                 MultiTermQuery.RewriteMethod method, QueryShardContext context) {
            throw new UnsupportedOperationException("[regexp] queries are not currently supported on [" +
                CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query wildcardQuery(String value,
                                   MultiTermQuery.RewriteMethod method,
                                   QueryShardContext context) {
            throw new UnsupportedOperationException("[wildcard] queries are not currently supported on [" +
                CONTENT_TYPE + "] fields.");
        }

        @Override
        public BytesRef indexedValueForSearch(Object value) {
            if (value == null) {
                return null;
            }

            String stringValue = value instanceof BytesRef
                ? ((BytesRef) value).utf8ToString()
                : value.toString();
            String keyedValue = JsonFieldParser.createKeyedValue(key, stringValue);
            return new BytesRef(keyedValue);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new KeyedJsonIndexFieldData.Builder(key);
        }
    }

    /**
     * A field data implementation that gives access to the values associated with
     * a particular JSON key.
     *
     * This class wraps the field data that is built directly on the keyed JSON field, and
     * filters out values whose prefix doesn't match the requested key. Loading and caching
     * is fully delegated to the wrapped field data, so that different {@link KeyedJsonIndexFieldData}
     * for the same JSON field share the same global ordinals.
     */
    public static class KeyedJsonIndexFieldData implements IndexOrdinalsFieldData {
        private final String key;
        private final IndexOrdinalsFieldData delegate;

        private KeyedJsonIndexFieldData(String key, IndexOrdinalsFieldData delegate) {
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
        public SortField sortField(Object missingValue,
                                   MultiValueMode sortMode,
                                   XFieldComparatorSource.Nested nested,
                                   boolean reverse) {
            XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
            return new SortField(getFieldName(), source, reverse);
        }

        @Override
        public void clear() {
            delegate.clear();
        }

        @Override
        public AtomicOrdinalsFieldData load(LeafReaderContext context) {
            AtomicOrdinalsFieldData fieldData = delegate.load(context);
            return new KeyedJsonAtomicFieldData(key, fieldData);
        }

        @Override
        public AtomicOrdinalsFieldData loadDirect(LeafReaderContext context) throws Exception {
            AtomicOrdinalsFieldData fieldData = delegate.loadDirect(context);
            return new KeyedJsonAtomicFieldData(key, fieldData);
        }

        @Override
        public IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader) {
            IndexOrdinalsFieldData fieldData = delegate.loadGlobal(indexReader);
            return new KeyedJsonIndexFieldData(key, fieldData);
        }

        @Override
        public IndexOrdinalsFieldData localGlobalDirect(DirectoryReader indexReader) throws Exception {
            IndexOrdinalsFieldData fieldData = delegate.localGlobalDirect(indexReader);
            return new KeyedJsonIndexFieldData(key, fieldData);
        }

        @Override
        public OrdinalMap getOrdinalMap() {
            return delegate.getOrdinalMap();
        }

        @Override
        public Index index() {
            return delegate.index();
        }

        public static class Builder implements IndexFieldData.Builder {
            private final String key;

            Builder(String key) {
                this.key = key;
            }

            @Override
            public IndexFieldData<?> build(IndexSettings indexSettings,
                                           MappedFieldType fieldType,
                                           IndexFieldDataCache cache,
                                           CircuitBreakerService breakerService,
                                           MapperService mapperService) {
                String fieldName = fieldType.name();
                IndexOrdinalsFieldData delegate = new SortedSetDVOrdinalsIndexFieldData(indexSettings,
                    cache, fieldName, breakerService, AbstractAtomicOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION);
                return new KeyedJsonIndexFieldData(key, delegate);
            }
        }
    }

    /**
     * A field type that represents all 'root' values. This field type is used in
     * searches on the JSON field itself, e.g. 'my_json_field: some_value'.
     */
    public static final class RootJsonFieldType extends StringFieldType {
        private boolean splitQueriesOnWhitespace;

        public RootJsonFieldType() {
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
        }

        private RootJsonFieldType(RootJsonFieldType ref) {
            super(ref);
            this.splitQueriesOnWhitespace = ref.splitQueriesOnWhitespace;
        }

        public RootJsonFieldType clone() {
            return new RootJsonFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            RootJsonFieldType that = (RootJsonFieldType) o;
            return splitQueriesOnWhitespace == that.splitQueriesOnWhitespace;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), splitQueriesOnWhitespace);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public boolean splitQueriesOnWhitespace() {
            return splitQueriesOnWhitespace;
        }

        public void setSplitQueriesOnWhitespace(boolean splitQueriesOnWhitespace) {
            checkIfFrozen();
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
        public Query fuzzyQuery(Object value, Fuzziness fuzziness, int prefixLength, int maxExpansions,
                                boolean transpositions) {
            throw new UnsupportedOperationException("[fuzzy] queries are not currently supported on [" +
                CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query regexpQuery(String value, int flags, int maxDeterminizedStates,
                                 MultiTermQuery.RewriteMethod method, QueryShardContext context) {
            throw new UnsupportedOperationException("[regexp] queries are not currently supported on [" +
                CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query wildcardQuery(String value,
                                   MultiTermQuery.RewriteMethod method,
                                   QueryShardContext context) {
            throw new UnsupportedOperationException("[wildcard] queries are not currently supported on [" +
                CONTENT_TYPE + "] fields.");
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new DocValuesIndexFieldData.Builder();
        }
    }

    private final JsonFieldParser fieldParser;
    private int depthLimit;
    private int ignoreAbove;

    private JsonFieldMapper(String simpleName,
                            MappedFieldType fieldType,
                            MappedFieldType defaultFieldType,
                            int ignoreAbove,
                            int depthLimit,
                            Settings indexSettings) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, MultiFields.empty(), CopyTo.empty());
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;

        this.depthLimit = depthLimit;
        this.ignoreAbove = ignoreAbove;
        this.fieldParser = new JsonFieldParser(fieldType.name(), keyedFieldName(),
            fieldType, depthLimit, ignoreAbove);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doMerge(Mapper mergeWith) {
        super.doMerge(mergeWith);
        this.ignoreAbove = ((JsonFieldMapper) mergeWith).ignoreAbove;
    }

    @Override
    protected JsonFieldMapper clone() {
        return (JsonFieldMapper) super.clone();
    }

    @Override
    public RootJsonFieldType fieldType() {
        return (RootJsonFieldType) super.fieldType();
    }

    public KeyedJsonFieldType keyedFieldType(String key) {
        return new KeyedJsonFieldType(keyedFieldName(), key, fieldType());
    }

    public String keyedFieldName() {
        return fieldType.name() + KEYED_FIELD_SUFFIX;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        if (context.parser().currentToken() == XContentParser.Token.VALUE_NULL) {
            return;
        }

        if (fieldType.indexOptions() == IndexOptions.NONE
                && !fieldType.hasDocValues()
                && !fieldType.stored()) {
            context.parser().skipChildren();
            return;
        }

        BytesRef storedValue = null;
        if (fieldType.stored()) {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                .prettyPrint()
                .copyCurrentStructure(context.parser());
            storedValue = BytesReference.bytes(builder).toBytesRef();
            fields.add(new StoredField(fieldType.name(), storedValue));
        }

        XContentParser indexedFieldsParser = context.parser();

        // If store is enabled, we've already consumed the content to produce the stored field. Here we
        // 'reset' the parser, so that we can traverse the content again.
        if (storedValue != null) {
            indexedFieldsParser = JsonXContent.jsonXContent.createParser(context.parser().getXContentRegistry(),
                context.parser().getDeprecationHandler(),
                storedValue.bytes);
            indexedFieldsParser.nextToken();
        }

        fields.addAll(fieldParser.parse(indexedFieldsParser));

        if (!fieldType.hasDocValues()) {
            createFieldNamesField(context, fields);
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || depthLimit != Defaults.DEPTH_LIMIT) {
            builder.field("depth_limit", depthLimit);
        }

        if (includeDefaults || ignoreAbove != Defaults.IGNORE_ABOVE) {
            builder.field("ignore_above", ignoreAbove);
        }

        if (includeDefaults || fieldType().nullValue() != null) {
            builder.field("null_value", fieldType().nullValue());
        }

        if (includeDefaults || fieldType().splitQueriesOnWhitespace()) {
            builder.field("split_queries_on_whitespace", fieldType().splitQueriesOnWhitespace());
        }
    }
}
