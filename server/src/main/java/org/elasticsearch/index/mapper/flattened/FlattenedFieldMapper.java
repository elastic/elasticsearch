/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOBooleanSupplier;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.plain.BytesBinaryIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockSourceReader;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.DynamicFieldType;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperMergeContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MappingParser;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.PassThroughFieldSource;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.TextParams;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.field.FlattenedDocValuesField;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

import static org.elasticsearch.index.IndexSettings.IGNORE_ABOVE_SETTING;
import static org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper.RootFlattenedFieldType.toSubFieldLoaders;

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
 * "key1\0some value" and "key2.key3\0true". Note that \0 is used as a reserved separator
 *  character (see {@link FlattenedFieldParser#SEPARATOR}).
 */
public final class FlattenedFieldMapper extends FieldMapper implements PassThroughFieldSource {

    public static final String CONTENT_TYPE = "flattened";
    public static final String KEYED_FIELD_SUFFIX = "._keyed";
    public static final String KEYED_IGNORED_VALUES_FIELD_SUFFIX = "._keyed._ignored";
    public static final String TIME_SERIES_DIMENSIONS_ARRAY_PARAM = "time_series_dimensions";

    public static final NodeFeature FLATTENED_MAPPED_SUBFIELDS_FEATURE = new NodeFeature("mapper.flattened.mapped_subfields");
    public static final NodeFeature FLATTENED_PASSTHROUGH_FEATURE = new NodeFeature("mapper.flattened.passthrough");

    private static class Defaults {
        public static final int DEPTH_LIMIT = 20;
    }

    public enum PreserveLeafArrays {
        LOSSY,
        EXACT;

        @Override
        public final String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private static Builder builder(Mapper in) {
        return ((FlattenedFieldMapper) in).builder;
    }

    public static class Builder extends FieldMapper.Builder {

        final Parameter<Integer> depthLimit = Parameter.intParam(
            "depth_limit",
            true,
            m -> builder(m).depthLimit.get(),
            Defaults.DEPTH_LIMIT
        ).addValidator(v -> {
            if (v < 0) {
                throw new IllegalArgumentException("[depth_limit] must be positive, got [" + v + "]");
            }
        });

        private final Parameter<Boolean> indexed;
        private final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> builder(m).hasDocValues.get(), true);

        private final Parameter<String> nullValue = Parameter.stringParam("null_value", false, m -> builder(m).nullValue.get(), null)
            .acceptsNull();

        private final Parameter<Boolean> eagerGlobalOrdinals = Parameter.boolParam(
            "eager_global_ordinals",
            true,
            m -> builder(m).eagerGlobalOrdinals.get(),
            false
        );
        private final int ignoreAboveDefault;
        private final Parameter<Integer> ignoreAbove;
        private final boolean indexDisabledByDefault;

        private final Parameter<String> indexOptions = TextParams.keywordIndexOptions(m -> builder(m).indexOptions.get());
        private final Parameter<SimilarityProvider> similarity = TextParams.similarity(m -> builder(m).similarity.get());

        private final Parameter<Boolean> splitQueriesOnWhitespace = Parameter.boolParam(
            "split_queries_on_whitespace",
            true,
            m -> builder(m).splitQueriesOnWhitespace.get(),
            false
        );

        private final Parameter<List<String>> dimensions;

        private final Parameter<PreserveLeafArrays> preserveLeafArrays;

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final Parameter<Map<String, FieldMapper.Builder>> properties;

        private final Parameter<Map<String, Object>> passthrough = new Parameter<>("passthrough", true, () -> null, (n, c, o) -> {
            if (o == null) return null;
            if ((o instanceof Map<?, ?>) == false) {
                throw new MapperParsingException("[passthrough] must be an object with a [priority] field, got [" + o + "]");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) o;
            if (map.containsKey("priority") == false) {
                throw new MapperParsingException("[passthrough] requires a [priority] field");
            }
            return map;
        }, m -> builder(m).passthrough.get(), (b, n, v) -> { if (v != null) b.field(n, v); }, Objects::toString).acceptsNull()
            .addValidator(v -> {
                if (v != null) {
                    int priority = XContentMapValues.nodeIntegerValue(v.get("priority"));
                    if (priority < 0) {
                        throw new IllegalArgumentException("[passthrough.priority] must be non-negative, got [" + priority + "]");
                    }
                }
            })
            .setSerializerCheck((includeDefaults, isConfigured, v) -> v != null);

        private final IndexSettings indexSettings;
        private final boolean usesBinaryDocValues;
        private final boolean isLegacyIndexWithRootValues;
        private final boolean forceStoreRootDocValues;
        private final boolean storeIgnoredFieldsInBinaryDocValues;

        public static FieldMapper.Parameter<List<String>> dimensionsParam(Function<FieldMapper, List<String>> initializer) {
            return FieldMapper.Parameter.stringArrayParam(TIME_SERIES_DIMENSIONS_ARRAY_PARAM, false, initializer);
        }

        private static Parameter<Map<String, FieldMapper.Builder>> propertiesParam(
            Function<FieldMapper, Map<String, FieldMapper.Builder>> initializer
        ) {
            var parameter = new Parameter<>(
                "properties",
                true,
                TreeMap::new,
                FlattenedFieldMapper.Builder::parseProperties,
                initializer,
                (b, n, v) -> {},
                Objects::toString
            );

            // Serialization of properties is handled by doXContentBody using the built FieldMapper objects.
            parameter.neverSerialize();
            return parameter;
        }

        private static boolean usesBinaryDocValues(IndexSettings indexSettings) {
            return indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.FLATTENED_FIELD_TSDB_CODEC_USE_BINARY_DOC_VALUES)
                && indexSettings.useTimeSeriesDocValuesFormat();
        }

        private static boolean usesBinaryDocValuesForIgnoredFields(IndexSettings indexSettings) {
            return indexSettings.getIndexVersionCreated().onOrAfter(IndexVersions.STORE_IGNORED_FLATTENED_FIELDS_IN_BINARY_DOC_VALUES)
                && indexSettings.useTimeSeriesDocValuesFormat();
        }

        /**
         * Root doc values are written when the index predates the removal version, when the field is indexed
         * (since the inverted index dominates storage so the saving is marginal), or when the escape-hatch
         * setting {@link IndexSettings#STORE_FLATTENED_ROOT_DOC_VALUES} is enabled.
         */
        private boolean hasRootDocValues() {
            return isLegacyIndexWithRootValues || indexed.get() || forceStoreRootDocValues;
        }

        public Builder(final String name, IndexSettings indexSettings) {
            this(
                name,
                IgnoreAbove.getIgnoreAboveDefaultValue(indexSettings.getMode(), indexSettings.getIndexVersionCreated()),
                indexSettings,
                false,
                false,
                false,
                false,
                false
            );
        }

        private Builder(String name, MappingParserContext mappingParserContext) {
            this(
                name,
                IGNORE_ABOVE_SETTING.get(mappingParserContext.getSettings()),
                mappingParserContext.getIndexSettings(),
                usesBinaryDocValues(mappingParserContext.getIndexSettings()),
                mappingParserContext.indexVersionCreated().before(IndexVersions.FLATTENED_FIELD_NO_ROOT_DOC_VALUES),
                IndexSettings.STORE_FLATTENED_ROOT_DOC_VALUES.get(mappingParserContext.getSettings()),
                usesBinaryDocValuesForIgnoredFields(mappingParserContext.getIndexSettings()),
                mappingParserContext.getIndexSettings().isIndexDisabledByDefault()
            );
        }

        private Builder(
            String name,
            int ignoreAboveDefault,
            IndexSettings indexSettings,
            boolean usesBinaryDocValues,
            boolean isLegacyIndexWithRootValues,
            boolean forceStoreRootDocValues,
            boolean storeIgnoredFieldsInBinaryDocValues,
            boolean indexDisabledByDefault
        ) {
            super(name);
            this.indexed = Parameter.indexParam(m -> builder(m).indexed.get(), indexDisabledByDefault == false);
            this.dimensions = dimensionsParam(m -> builder(m).dimensions.get()).addValidator(v -> {
                if (v.isEmpty() == false && (indexed.getValue() == false || hasDocValues.getValue() == false)) {
                    throw new IllegalArgumentException(
                        "Field ["
                            + TIME_SERIES_DIMENSIONS_ARRAY_PARAM
                            + "] requires that ["
                            + indexed.name
                            + "] and ["
                            + hasDocValues.name
                            + "] are true"
                    );
                }
            });
            this.ignoreAboveDefault = ignoreAboveDefault;
            this.indexSettings = indexSettings;
            this.ignoreAbove = Parameter.ignoreAboveParam(m -> builder(m).ignoreAbove.get(), ignoreAboveDefault);
            this.dimensions.precludesParameters(ignoreAbove);
            this.usesBinaryDocValues = usesBinaryDocValues;
            this.isLegacyIndexWithRootValues = isLegacyIndexWithRootValues;
            this.forceStoreRootDocValues = forceStoreRootDocValues;
            this.properties = propertiesParam(m -> builder(m).properties.getValue());
            this.storeIgnoredFieldsInBinaryDocValues = storeIgnoredFieldsInBinaryDocValues;

            preserveLeafArrays = Parameter.enumParam(
                "preserve_leaf_arrays",
                false,
                m -> builder(m).preserveLeafArrays.get(),
                indexSettings.getValue(Mapper.SYNTHETIC_SOURCE_KEEP_INDEX_SETTING) == SourceKeepMode.NONE
                    ? PreserveLeafArrays.LOSSY
                    : PreserveLeafArrays.EXACT,
                PreserveLeafArrays.class
            );
            this.indexDisabledByDefault = indexDisabledByDefault;
        }

        public Builder passthrough(int priority) {
            this.passthrough.setValue(Map.of("priority", priority));
            return this;
        }

        public Builder property(String name, FieldMapper.Builder propertyBuilder) {
            Map<String, FieldMapper.Builder> current = new TreeMap<>(this.properties.getValue());
            current.put(name, propertyBuilder);
            this.properties.setValue(current);
            return this;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] {
                indexed,
                hasDocValues,
                depthLimit,
                nullValue,
                eagerGlobalOrdinals,
                ignoreAbove,
                indexOptions,
                similarity,
                splitQueriesOnWhitespace,
                meta,
                dimensions,
                properties,
                passthrough,
                preserveLeafArrays };
        }

        @Override
        public String contentType() {
            return CONTENT_TYPE;
        }

        @Override
        public FlattenedFieldMapper build(MapperBuilderContext context) {
            MultiFields multiFields = multiFieldsBuilder.build(this, context);
            if (multiFields.iterator().hasNext()) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + leafName() + "] does not support [fields]");
            }
            if (copyTo.copyToFields().isEmpty() == false) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + leafName() + "] does not support [copy_to]");
            }
            Map<String, FieldMapper.Builder> propertyBuilders = properties.getValue();
            if (passthrough.getValue() != null && propertyBuilders.isEmpty()) {
                throw new MapperParsingException("Flattened field [" + leafName() + "] has [passthrough] set but no [properties] defined");
            }
            Map<String, FieldMapper> mappedSubFields = new TreeMap<>();
            if (propertyBuilders.isEmpty() == false) {
                MapperBuilderContext childContext = context.createChildContext(leafName(), null);
                for (Map.Entry<String, FieldMapper.Builder> entry : propertyBuilders.entrySet()) {
                    mappedSubFields.put(entry.getKey(), entry.getValue().build(childContext));
                }
            }

            boolean hasRootDocValues = hasRootDocValues();
            MappedFieldType ft = new RootFlattenedFieldType(
                context.buildFullName(leafName()),
                IndexType.terms(indexed.get(), hasDocValues.get()),
                meta.get(),
                splitQueriesOnWhitespace.get(),
                eagerGlobalOrdinals.get(),
                dimensions.get(),
                new IgnoreAbove(ignoreAbove.getValue(), indexSettings.getMode(), indexSettings.getIndexVersionCreated()),
                usesBinaryDocValues,
                hasRootDocValues,
                nullValue.get(),
                context.isSourceSynthetic(),
                mappedSubFields,
                storeIgnoredFieldsInBinaryDocValues,
                preserveLeafArrays.get(),
                indexSettings.getIndexVersionCreated()
            );
            return new FlattenedFieldMapper(leafName(), ft, builderParams(this, context), this, mappedSubFields);
        }

        @Override
        protected void mergeFromBuilder(FieldMapper.Builder incoming, Conflicts conflicts, MapperMergeContext mergeContext) {
            Map<String, FieldMapper.Builder> currentProperties = new TreeMap<>(properties.getValue());
            super.mergeFromBuilder(incoming, conflicts, mergeContext);
            MapperMergeContext childContext = MapperMergeContext.from(mergeContext.getMapperBuilderContext(), Long.MAX_VALUE);
            for (Map.Entry<String, FieldMapper.Builder> entry : ((Builder) incoming).properties.getValue().entrySet()) {
                currentProperties.merge(
                    entry.getKey(),
                    entry.getValue(),
                    (existing, inc) -> (FieldMapper.Builder) existing.mergeWith(inc, childContext)
                );
            }
            properties.setValue(currentProperties);
        }

        /**
         * Parses sub-field properties for flattened fields. Unlike {@code ObjectMapper.Builder.parseProperties},
         * flattened sub-fields are restricted to an explicit allow list of types, disallow copy_to/multi-fields,
         * and are built into a flat map keyed by dotted path rather than a recursive object tree.
         */
        @SuppressWarnings("unchecked")
        private static Map<String, FieldMapper.Builder> parseProperties(
            String flattenedName,
            MappingParserContext parserContext,
            Object propertiesNode
        ) {
            Map<String, FieldMapper.Builder> propertyBuilders = new TreeMap<>();
            if (propertiesNode instanceof Collection<?> c && c.isEmpty()) {
                return propertyBuilders;
            }
            if (propertiesNode instanceof Map == false) {
                throw new MapperParsingException("[properties] on flattened field [" + flattenedName + "] must be a map");
            }
            Map<String, Object> propsMap = (Map<String, Object>) propertiesNode;
            for (Map.Entry<String, Object> entry : propsMap.entrySet()) {
                String propertyName = entry.getKey();
                if (entry.getValue() instanceof Map == false) {
                    throw new MapperParsingException(
                        "Expected map for property [" + propertyName + "] in flattened field [" + flattenedName + "]"
                    );
                }
                Map<String, Object> propNode = (Map<String, Object>) entry.getValue();
                Object typeNode = propNode.get("type");
                if (typeNode == null) {
                    throw new MapperParsingException(
                        "No type specified for property [" + propertyName + "] in flattened field [" + flattenedName + "]"
                    );
                }
                String type = typeNode.toString();
                if (ALLOWED_SUB_FIELD_TYPES.contains(type) == false) {
                    throw new MapperParsingException(
                        "Type ["
                            + type
                            + "] is not supported as a mapped sub-field of flattened field ["
                            + flattenedName
                            + "]. Supported types: "
                            + ALLOWED_SUB_FIELD_TYPES
                    );
                }
                if (propNode.containsKey("copy_to")) {
                    throw new MapperParsingException("[copy_to] is not supported on properties of flattened field [" + flattenedName + "]");
                }
                if (propNode.containsKey("fields")) {
                    throw new MapperParsingException(
                        "[fields] (multi-fields) is not supported on properties of flattened field [" + flattenedName + "]"
                    );
                }
                Mapper.TypeParser typeParser = parserContext.typeParser(type);
                if (typeParser == null) {
                    throw new MapperParsingException("No handler for type [" + type + "] declared on property [" + propertyName + "]");
                }
                Mapper.Builder fieldBuilder = typeParser.parse(propertyName, propNode, parserContext);
                assert fieldBuilder instanceof FieldMapper.Builder
                    : "allowed sub-field type [" + type + "] produced a non-FieldMapper builder";
                propertyBuilders.put(propertyName, (FieldMapper.Builder) fieldBuilder);
                propNode.remove("type");
                MappingParser.checkNoRemainingFields(propertyName, propNode);
            }
            return propertyBuilders;
        }
    }

    private static final Set<String> ALLOWED_SUB_FIELD_TYPES = Set.of(
        "keyword",
        "constant_keyword",
        "wildcard",
        "text",
        "long",
        "integer",
        "short",
        "byte",
        "double",
        "float",
        "half_float",
        "scaled_float",
        "unsigned_long",
        "date",
        "date_nanos",
        "boolean",
        "ip"
    );

    public static final FieldMapper.TypeParser PARSER = FieldMapper.createTypeParserWithLegacySupport(Builder::new);

    abstract static class BaseFlattenedFieldType extends StringFieldType {
        protected final IgnoreAbove ignoreAbove;
        protected final boolean usesBinaryDocValues;
        protected final boolean hasRootDocValues;
        protected final String nullValue;
        protected final IndexVersion indexVersion;

        BaseFlattenedFieldType(
            String name,
            IndexType indexType,
            boolean isStored,
            TextSearchInfo textSearchInfo,
            Map<String, String> meta,
            IgnoreAbove ignoreAbove,
            boolean usesBinaryDocValues,
            boolean hasRootDocValues,
            String nullValue,
            IndexVersion indexVersion
        ) {
            super(name, indexType, isStored, textSearchInfo, meta);
            this.ignoreAbove = ignoreAbove;
            this.usesBinaryDocValues = usesBinaryDocValues;
            this.hasRootDocValues = hasRootDocValues;
            this.nullValue = nullValue;
            this.indexVersion = indexVersion;
        }

        protected Mapper.IgnoreAbove ignoreAbove() {
            return ignoreAbove;
        }

        protected BlockSourceReader.LeafIteratorLookup sourceBlockLoaderLookup(
            MappedFieldType.BlockLoaderContext blContext,
            String fieldName
        ) {
            if (hasDocValues() && ignoreAbove().valuesPotentiallyIgnored() == false) {
                String keyedFieldName = fieldName + KEYED_FIELD_SUFFIX;
                if (usesBinaryDocValues) {
                    return ctx -> Objects.requireNonNullElseGet(ctx.reader().getBinaryDocValues(keyedFieldName), DocIdSetIterator::empty);
                } else {
                    return ctx -> Objects.requireNonNullElseGet(
                        ctx.reader().getSortedSetDocValues(keyedFieldName),
                        DocIdSetIterator::empty
                    );
                }
            }
            if (hasDocValues() == false && indexType.hasTerms()) {
                // We only write the field names field if there aren't doc values or norms
                return BlockSourceReader.lookupFromFieldNames(blContext.fieldNames(), fieldName);
            }
            return BlockSourceReader.lookupMatchingAll();
        }
    }

    /**
     * A field type that represents the values under a particular JSON key, used
     * when searching under a specific key as in 'my_flattened.key: some_value'.
     */
    public static final class KeyedFlattenedFieldType extends BaseFlattenedFieldType {
        private final String key;
        private final String rootName;
        private final boolean isDimension;
        private final boolean isSyntheticSourceEnabled;

        @Override
        public boolean isDimension() {
            return isDimension;
        }

        KeyedFlattenedFieldType(
            String rootName,
            IndexType indexType,
            String key,
            boolean splitQueriesOnWhitespace,
            Map<String, String> meta,
            boolean isDimension,
            IgnoreAbove ignoreAbove,
            boolean usesBinaryDocValues,
            boolean hasRootDocValues,
            String nullValue,
            IndexVersion indexVersion,
            boolean isSyntheticSourceEnabled
        ) {
            super(
                rootName + KEYED_FIELD_SUFFIX,
                indexType,
                false,
                splitQueriesOnWhitespace ? TextSearchInfo.WHITESPACE_MATCH_ONLY : TextSearchInfo.SIMPLE_MATCH_ONLY,
                meta,
                ignoreAbove,
                usesBinaryDocValues,
                hasRootDocValues,
                nullValue,
                indexVersion
            );
            this.key = key;
            this.rootName = rootName;
            this.isDimension = isDimension;
            this.isSyntheticSourceEnabled = isSyntheticSourceEnabled;
        }

        private KeyedFlattenedFieldType(
            String rootName,
            String key,
            RootFlattenedFieldType ref,
            IgnoreAbove ignoreAbove,
            boolean usesBinaryDocValues,
            String nullValue,
            boolean isSyntheticSourceEnabled
        ) {
            this(
                rootName,
                ref.indexType(),
                key,
                ref.splitQueriesOnWhitespace,
                ref.meta(),
                ref.dimensions.contains(key),
                ignoreAbove,
                usesBinaryDocValues,
                ref.hasRootDocValues,
                nullValue,
                ref.indexVersion,
                isSyntheticSourceEnabled
            );
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public String rootName() {
            return this.rootName;
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
        public Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            SearchExecutionContext context
        ) {

            // We require range queries to specify both bounds because an unbounded query could incorrectly match
            // values from other keys. For example, a query on the 'first' key with only a lower bound would become
            // ("first\0value", null), which would also match the value "second\0value" belonging to the key 'second'.
            if (lowerTerm == null || upperTerm == null) {
                throw new IllegalArgumentException(
                    "[range] queries on keyed [" + CONTENT_TYPE + "] fields must include both an upper and a lower bound."
                );
            }

            return super.rangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, context);
        }

        @Override
        public Query fuzzyQuery(
            Object value,
            Fuzziness fuzziness,
            int prefixLength,
            int maxExpansions,
            boolean transpositions,
            SearchExecutionContext context,
            @Nullable MultiTermQuery.RewriteMethod rewriteMethod
        ) {
            throw new UnsupportedOperationException(
                "[fuzzy] queries are not currently supported on keyed " + "[" + CONTENT_TYPE + "] fields."
            );
        }

        @Override
        public Query regexpQuery(
            String value,
            int syntaxFlags,
            int matchFlags,
            int maxDeterminizedStates,
            MultiTermQuery.RewriteMethod method,
            SearchExecutionContext context
        ) {
            throw new UnsupportedOperationException(
                "[regexp] queries are not currently supported on keyed " + "[" + CONTENT_TYPE + "] fields."
            );
        }

        @Override
        public Query wildcardQuery(
            String value,
            MultiTermQuery.RewriteMethod method,
            boolean caseInsensitive,
            SearchExecutionContext context
        ) {
            throw new UnsupportedOperationException(
                "[wildcard] queries are not currently supported on keyed " + "[" + CONTENT_TYPE + "] fields."
            );
        }

        @Override
        public Query termQueryCaseInsensitive(Object value, SearchExecutionContext context) {
            return AutomatonQueries.caseInsensitiveTermQuery(new Term(name(), indexedValueForSearch(value)));
        }

        @Override
        public TermsEnum getTerms(IndexReader reader, String prefix, boolean caseInsensitive, String searchAfter) throws IOException {
            Terms terms = MultiTerms.getTerms(reader, name());
            if (terms == null) {
                // Field does not exist on this shard.
                return null;
            }

            Automaton a = Automata.makeString(key + FlattenedFieldParser.SEPARATOR);
            if (caseInsensitive) {
                a = Operations.concatenate(a, AutomatonQueries.caseInsensitivePrefix(prefix));
            } else {
                a = Operations.concatenate(a, Automata.makeString(prefix));
                a = Operations.concatenate(a, Automata.makeAnyString());
            }
            assert a.isDeterministic();

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

            String stringValue = value instanceof BytesRef ? ((BytesRef) value).utf8ToString() : value.toString();
            String keyedValue = FlattenedFieldParser.createKeyedValue(key, stringValue);
            return new BytesRef(keyedValue);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();

            if (usesBinaryDocValues) {
                return new BinaryKeyedFlattenedFieldData.Builder(name(), key, FlattenedDocValuesField::new, indexVersion);
            } else {
                return new KeyedFlattenedFieldData.Builder(name(), key, (dv, n) -> new FlattenedDocValuesField(FieldData.toString(dv), n));
            }
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException(
                    "Field [" + rootName + "." + key + "] of type [" + typeName() + "] doesn't support formats."
                );
            }
            return SourceValueFetcher.identity(rootName + "." + key, context, null);
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
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            boolean preferLoadFromSource = blContext.fieldExtractPreference() == FieldExtractPreference.STORED
                && isSyntheticSourceEnabled == false;
            if (hasDocValues() && preferLoadFromSource == false) {
                return new KeyedFlattenedDocValuesBlockLoader(name(), key, usesBinaryDocValues);
            }

            var fetcher = new SourceValueFetcher(
                blContext.sourcePaths(rootName + "." + key),
                nullValue,
                blContext.indexSettings().getIgnoredSourceFormat()
            ) {
                @Override
                protected Object parseSourceValue(Object value) {
                    return value.toString();
                }
            };

            return new BlockSourceReader.BytesRefsBlockLoader(fetcher, sourceBlockLoaderLookup(blContext, rootName()));
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

        // =============== All other TermsEnum methods not supported =================

        @Override
        public AttributeSource attributes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public IOBooleanSupplier prepareSeekExact(BytesRef bytesRef) throws IOException {
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
        private final ToScriptFieldFactory<SortedSetDocValues> toScriptFieldFactory;

        private KeyedFlattenedFieldData(
            String key,
            IndexOrdinalsFieldData delegate,
            ToScriptFieldFactory<SortedSetDocValues> toScriptFieldFactory
        ) {
            this.delegate = delegate;
            this.key = key;
            this.toScriptFieldFactory = toScriptFieldFactory;
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
        public SortField sortField(Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, boolean reverse) {
            XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
            return new SortField(getFieldName(), source, reverse);
        }

        @Override
        public BucketedSort newBucketedSort(
            BigArrays bigArrays,
            Object missingValue,
            MultiValueMode sortMode,
            Nested nested,
            SortOrder sortOrder,
            DocValueFormat format,
            int bucketSize,
            BucketedSort.ExtraData extra
        ) {
            throw new IllegalArgumentException("only supported on numeric fields");
        }

        @Override
        public LeafOrdinalsFieldData load(LeafReaderContext context) {
            LeafOrdinalsFieldData fieldData = delegate.load(context);
            return new KeyedFlattenedLeafFieldData(key, fieldData, toScriptFieldFactory);
        }

        @Override
        public LeafOrdinalsFieldData loadDirect(LeafReaderContext context) throws Exception {
            LeafOrdinalsFieldData fieldData = delegate.loadDirect(context);
            return new KeyedFlattenedLeafFieldData(key, fieldData, toScriptFieldFactory);
        }

        @Override
        public IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader) {
            IndexOrdinalsFieldData fieldData = delegate.loadGlobal(indexReader);
            return new KeyedFlattenedFieldData(key, fieldData, toScriptFieldFactory);
        }

        @Override
        public IndexOrdinalsFieldData loadGlobalDirect(DirectoryReader indexReader) throws Exception {
            IndexOrdinalsFieldData fieldData = delegate.loadGlobalDirect(indexReader);
            return new KeyedFlattenedFieldData(key, fieldData, toScriptFieldFactory);
        }

        @Override
        public OrdinalMap getOrdinalMap() {
            throw new UnsupportedOperationException(
                "The field data for the flattened field ["
                    + delegate.getFieldName()
                    + "] does not allow access to the underlying ordinal map."
            );
        }

        @Override
        public boolean supportsGlobalOrdinalsMapping() {
            return false;
        }

        public static class Builder implements IndexFieldData.Builder {
            private final String fieldName;
            private final String key;
            private final ToScriptFieldFactory<SortedSetDocValues> toScriptFieldFactory;

            Builder(String fieldName, String key, ToScriptFieldFactory<SortedSetDocValues> toScriptFieldFactory) {
                this.fieldName = fieldName;
                this.key = key;
                this.toScriptFieldFactory = toScriptFieldFactory;
            }

            @Override
            public IndexFieldData<?> build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
                IndexOrdinalsFieldData delegate = new SortedSetOrdinalsIndexFieldData(
                    cache,
                    fieldName,
                    CoreValuesSourceType.KEYWORD,
                    breakerService,
                    // The delegate should never be accessed
                    (dv, n) -> { throw new UnsupportedOperationException(); }
                );
                return new KeyedFlattenedFieldData(key, delegate, toScriptFieldFactory);
            }
        }
    }

    public static final class BinaryKeyedFlattenedFieldData implements IndexFieldData<LeafFieldData> {
        private final String key;
        private final BytesBinaryIndexFieldData delegate;
        private final ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory;

        private BinaryKeyedFlattenedFieldData(
            String key,
            BytesBinaryIndexFieldData delegate,
            ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory
        ) {
            this.delegate = delegate;
            this.key = key;
            this.toScriptFieldFactory = toScriptFieldFactory;
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
        public SortField sortField(Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, boolean reverse) {
            XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
            return new SortField(getFieldName(), source, reverse);
        }

        @Override
        public BucketedSort newBucketedSort(
            BigArrays bigArrays,
            Object missingValue,
            MultiValueMode sortMode,
            Nested nested,
            SortOrder sortOrder,
            DocValueFormat format,
            int bucketSize,
            BucketedSort.ExtraData extra
        ) {
            throw new IllegalArgumentException("only supported on numeric fields");
        }

        @Override
        public LeafFieldData load(LeafReaderContext context) {
            LeafFieldData fieldData = delegate.load(context);
            return new BinaryKeyedFlattenedLeafFieldData(key, fieldData, toScriptFieldFactory);
        }

        @Override
        public LeafFieldData loadDirect(LeafReaderContext context) throws Exception {
            LeafFieldData fieldData = delegate.loadDirect(context);
            return new BinaryKeyedFlattenedLeafFieldData(key, fieldData, toScriptFieldFactory);
        }

        public static class Builder implements IndexFieldData.Builder {
            private final String fieldName;
            private final String key;
            private final ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory;
            private final IndexVersion indexVersion;

            Builder(
                String fieldName,
                String key,
                ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory,
                IndexVersion indexVersion
            ) {
                this.fieldName = fieldName;
                this.key = key;
                this.toScriptFieldFactory = toScriptFieldFactory;
                this.indexVersion = indexVersion;
            }

            @Override
            public IndexFieldData<?> build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
                var delegate = new BytesBinaryIndexFieldData(fieldName, CoreValuesSourceType.KEYWORD, toScriptFieldFactory, indexVersion);
                return new BinaryKeyedFlattenedFieldData(key, delegate, toScriptFieldFactory);
            }
        }
    }

    /**
     * A field type that represents all 'root' values. This field type is used in
     * searches on the flattened field itself, e.g. 'my_flattened: some_value'.
     */
    public static final class RootFlattenedFieldType extends BaseFlattenedFieldType implements DynamicFieldType {
        private final boolean splitQueriesOnWhitespace;
        private final boolean eagerGlobalOrdinals;
        private final List<String> dimensions;
        private final boolean isDimension;
        private final boolean isSyntheticSourceEnabled;
        private final Map<String, FieldMapper> mappedSubFields;
        private final boolean storeIgnoredFieldsInBinaryDocValues;
        private final PreserveLeafArrays preserveLeafArrays;

        RootFlattenedFieldType(
            String name,
            IndexType indexType,
            Map<String, String> meta,
            boolean splitQueriesOnWhitespace,
            boolean eagerGlobalOrdinals,
            IgnoreAbove ignoreAbove,
            boolean usesBinaryDocValues,
            boolean hasRootDocValues,
            String nullValue,
            boolean isSyntheticSourceEnabled,
            PreserveLeafArrays preserveLeafArrays
        ) {
            this(
                name,
                indexType,
                meta,
                splitQueriesOnWhitespace,
                eagerGlobalOrdinals,
                Collections.emptyList(),
                ignoreAbove,
                usesBinaryDocValues,
                hasRootDocValues,
                nullValue,
                isSyntheticSourceEnabled,
                Collections.emptyMap(),
                false,
                preserveLeafArrays,
                IndexVersion.current()
            );
        }

        RootFlattenedFieldType(
            String name,
            IndexType indexType,
            Map<String, String> meta,
            boolean splitQueriesOnWhitespace,
            boolean eagerGlobalOrdinals,
            List<String> dimensions,
            IgnoreAbove ignoreAbove,
            boolean usesBinaryDocValues,
            boolean hasRootDocValues,
            String nullValue,
            boolean isSyntheticSourceEnabled,
            Map<String, FieldMapper> mappedSubFields,
            boolean storeIgnoredFieldsInBinaryDocValues,
            PreserveLeafArrays preserveLeafArrays,
            IndexVersion indexVersion
        ) {
            super(
                name,
                indexType,
                false,
                splitQueriesOnWhitespace ? TextSearchInfo.WHITESPACE_MATCH_ONLY : TextSearchInfo.SIMPLE_MATCH_ONLY,
                meta,
                ignoreAbove,
                usesBinaryDocValues,
                hasRootDocValues,
                nullValue,
                indexVersion
            );
            this.splitQueriesOnWhitespace = splitQueriesOnWhitespace;
            this.eagerGlobalOrdinals = eagerGlobalOrdinals;
            this.dimensions = dimensions;
            this.isDimension = dimensions.isEmpty() == false;
            this.isSyntheticSourceEnabled = isSyntheticSourceEnabled;
            this.mappedSubFields = mappedSubFields;
            this.storeIgnoredFieldsInBinaryDocValues = storeIgnoredFieldsInBinaryDocValues;
            this.preserveLeafArrays = preserveLeafArrays;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            if (hasDocValues()) {
                return new FieldExistsQuery(name() + KEYED_FIELD_SUFFIX);
            }
            return super.existsQuery(context);
        }

        static List<SourceLoader.SyntheticFieldLoader> toSubFieldLoaders(Map<String, FieldMapper> mappedSubFields) {
            return mappedSubFields.values()
                .stream()
                .map(FieldMapper::syntheticFieldLoader)
                .filter(l -> l != SourceLoader.SyntheticFieldLoader.NOTHING)
                .toList();
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            // If a value trips ignore_above, it is stored in the fallback binary doc values field only if synthetic source is enabled.
            // Otherwise, that value only exists in stored _source.
            final boolean docValuesContainAllValues = ignoreAbove.valuesPotentiallyIgnored() == false || isSyntheticSourceEnabled;
            final boolean preferLoadFromSource = blContext.fieldExtractPreference() == FieldExtractPreference.STORED
                && isSyntheticSourceEnabled == false;
            if (hasDocValues() && docValuesContainAllValues && preferLoadFromSource == false) {
                return new RootFlattenedDocValuesBlockLoader(
                    name(),
                    ignoreAbove,
                    usesBinaryDocValues,
                    toSubFieldLoaders(mappedSubFields),
                    storeIgnoredFieldsInBinaryDocValues,
                    preserveLeafArrays
                );
            }

            SourceValueFetcher fetcher = new SourceValueFetcher(
                blContext.sourcePaths(name()),
                nullValue,
                blContext.indexSettings().getIgnoredSourceFormat()
            ) {
                @Override
                @SuppressWarnings("unchecked")
                protected Object parseSourceValue(Object value) {
                    try {
                        return Strings.toString(XContentFactory.jsonBuilder().map((Map<String, Object>) value));
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            };

            return new BlockSourceReader.BytesRefsBlockLoader(fetcher, sourceBlockLoaderLookup(blContext, name()));
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
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();

            if (hasRootDocValues) {
                if (usesBinaryDocValues) {
                    return new BytesBinaryIndexFieldData.Builder(
                        name(),
                        CoreValuesSourceType.KEYWORD,
                        FlattenedDocValuesField::new,
                        indexVersion
                    );
                } else {
                    return new SortedSetOrdinalsIndexFieldData.Builder(
                        name(),
                        CoreValuesSourceType.KEYWORD,
                        (dv, n) -> new FlattenedDocValuesField(FieldData.toString(dv), n)
                    );
                }
            }
            return new RootFlattenedFromKeyedFieldData.Builder(name(), name() + KEYED_FIELD_SUFFIX, usesBinaryDocValues, indexVersion);
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            return sourceValueFetcher(
                context.isSourceEnabled() ? context.sourcePath(name()) : Collections.emptySet(),
                context.getIndexSettings()
            );
        }

        public IgnoreAbove ignoreAbove() {
            return ignoreAbove;
        }

        private SourceValueFetcher sourceValueFetcher(Set<String> sourcePaths, IndexSettings indexSettings) {
            return new SourceValueFetcher(sourcePaths, null, indexSettings.getIgnoredSourceFormat()) {
                @Override
                @SuppressWarnings("unchecked")
                protected Object parseSourceValue(Object value) {
                    if (value instanceof Map<?, ?> valueAsMap && valueAsMap.isEmpty() == false) {
                        final Map<String, Object> result = filterIgnoredValues((Map<String, Object>) valueAsMap);
                        return result.isEmpty() ? null : result;
                    }
                    if (value instanceof String valueAsString && ignoreAbove.isIgnored(valueAsString) == false) {
                        return valueAsString;
                    }
                    return null;
                }

                private Map<String, Object> filterIgnoredValues(final Map<String, Object> values) {
                    final Map<String, Object> result = new HashMap<>();
                    for (final Map.Entry<String, Object> entry : values.entrySet()) {
                        Object value = filterIgnoredValues(entry.getValue());
                        if (value != null) {
                            result.put(entry.getKey(), value);
                        }
                    }
                    return result;
                }

                private Object filterIgnoredValues(final Object entryValue) {
                    if (entryValue instanceof List<?> valueAsList) {
                        final List<Object> validValues = new ArrayList<>();
                        for (Object value : valueAsList) {
                            if (value instanceof String valueAsString) {
                                if (ignoreAbove.isIgnored(valueAsString) == false) {
                                    validValues.add(valueAsString);
                                }
                            } else {
                                validValues.add(value);
                            }
                        }
                        if (validValues.isEmpty()) {
                            return null;
                        }
                        if (validValues.size() == 1) {
                            // NOTE: for single-value flattened fields do not return an array
                            return validValues.getFirst();
                        }
                        return validValues;
                    } else if (entryValue instanceof String valueAsString) {
                        if (ignoreAbove.isIgnored(valueAsString) == false) {
                            return valueAsString;
                        }
                        return null;
                    }
                    return entryValue;
                }
            };
        }

        @Override
        public MappedFieldType getChildFieldType(String childPath) {
            FieldMapper mappedSubField = mappedSubFields.get(childPath);
            if (mappedSubField != null) {
                return mappedSubField.fieldType();
            }
            return new KeyedFlattenedFieldType(
                name(),
                childPath,
                this,
                ignoreAbove,
                usesBinaryDocValues,
                nullValue,
                isSyntheticSourceEnabled
            );
        }

        public MappedFieldType getKeyedFieldType() {
            return new KeywordFieldMapper.KeywordFieldType(
                name() + KEYED_FIELD_SUFFIX,
                true,
                true,
                usesBinaryDocValues,
                Collections.emptyMap()
            );
        }

        @Override
        public boolean isDimension() {
            return isDimension;
        }

        @Override
        public List<String> dimensions() {
            return this.dimensions;
        }

        @Override
        public void validateMatchedRoutingPath(final String routingPath) {
            if (this.dimensions.contains(routingPath) == false) {
                super.validateMatchedRoutingPath(routingPath);
            }
        }
    }

    private final FlattenedFieldParser fieldParser;
    private final Builder builder;
    private final Map<String, FieldMapper> mappedSubFields;
    private final int passthroughPriority; // -1 means passthrough disabled
    private final boolean passthrough;
    private final PreserveLeafArrays preserveLeafArrays;

    private FlattenedFieldMapper(
        String leafName,
        MappedFieldType mappedFieldType,
        BuilderParams builderParams,
        Builder builder,
        Map<String, FieldMapper> mappedSubFields
    ) {
        super(leafName, mappedFieldType, builderParams);
        this.builder = builder;
        this.mappedSubFields = Collections.unmodifiableSortedMap(new TreeMap<>(mappedSubFields));
        Map<String, Object> passthroughConfig = builder.passthrough.getValue();
        this.passthroughPriority = passthroughConfig != null ? XContentMapValues.nodeIntegerValue(passthroughConfig.get("priority")) : -1;
        this.passthrough = this.passthroughPriority >= 0;
        this.fieldParser = new FlattenedFieldParser(
            mappedFieldType.name(),
            mappedFieldType.name() + KEYED_FIELD_SUFFIX,
            mappedFieldType.name() + KEYED_IGNORED_VALUES_FIELD_SUFFIX,
            mappedFieldType,
            builder.depthLimit.get(),
            builder.ignoreAbove.get(),
            builder.nullValue.get(),
            builder.usesBinaryDocValues,
            builder.hasRootDocValues(),
            mappedSubFields,
            builder.storeIgnoredFieldsInBinaryDocValues,
            builder.preserveLeafArrays.get(),
            builder.indexSettings.getIndexVersionCreated(),
            builder.dimensions.getValue().isEmpty() == false
                && builder.indexSettings.getIndexRouting() instanceof IndexRouting.ExtractFromSource efs
                && efs.extractDimensionsWhileMapping()
        );
        this.preserveLeafArrays = builder.preserveLeafArrays.get();
    }

    public PreserveLeafArrays preserveLeafArrays() {
        return preserveLeafArrays;
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), Lucene.KEYWORD_ANALYZER);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    int depthLimit() {
        return builder.depthLimit.get();
    }

    public boolean isPassthrough() {
        return passthrough;
    }

    @Override
    public int priority() {
        return passthroughPriority;
    }

    @Override
    public Collection<FieldMapper> passThroughSubFields() {
        if (passthrough == false || mappedSubFields.isEmpty()) {
            return Collections.emptyList();
        }
        return mappedSubFields.values();
    }

    @Override
    public RootFlattenedFieldType fieldType() {
        return (RootFlattenedFieldType) super.fieldType();
    }

    @Override
    protected boolean supportsParsingObject() {
        return true;
    }

    @Override
    public Iterator<Mapper> iterator() {
        if (mappedSubFields.isEmpty()) {
            return super.iterator();
        }
        return Iterators.concat(super.iterator(), mappedSubFields.values().stream().map(m -> (Mapper) m).iterator());
    }

    @Override
    public int getTotalFieldsCount() {
        int count = super.getTotalFieldsCount();
        for (FieldMapper mapper : mappedSubFields.values()) {
            count += mapper.getTotalFieldsCount();
        }
        return count;
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        if (context.parser().currentToken() == XContentParser.Token.VALUE_NULL) {
            return;
        }

        if (mappedFieldType.indexType() == IndexType.NONE) {
            context.parser().skipChildren();
            return;
        }

        FlattenedFieldArrayContext arrayContext;
        if (preserveLeafArrays == PreserveLeafArrays.LOSSY) {
            arrayContext = null;
        } else {
            arrayContext = new FlattenedFieldArrayContext(mappedFieldType.name());
        }

        try {
            // make sure that we don't expand dots in field names while parsing
            context.path().setWithinLeafObject(true);
            fieldParser.parse(context, arrayContext);
        } finally {
            context.path().setWithinLeafObject(false);
        }

        if (arrayContext != null) {
            arrayContext.addToLuceneDocument(context);
        }

        if (mappedFieldType.hasDocValues() == false) {
            context.addToFieldNames(fieldType().name());
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder xContentBuilder, Params params) throws IOException {
        super.doXContentBody(xContentBuilder, params);
        if (mappedSubFields.isEmpty() == false) {
            xContentBuilder.startObject("properties");
            for (Map.Entry<String, FieldMapper> entry : mappedSubFields.entrySet()) {
                entry.getValue().toXContent(xContentBuilder, params);
            }
            xContentBuilder.endObject();
        }
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        Builder b = new Builder(
            leafName(),
            builder.ignoreAboveDefault,
            builder.indexSettings,
            builder.usesBinaryDocValues,
            builder.isLegacyIndexWithRootValues,
            builder.forceStoreRootDocValues,
            builder.storeIgnoredFieldsInBinaryDocValues,
            builder.indexDisabledByDefault
        );
        b.init(this);
        Map<String, FieldMapper.Builder> propBuilders = new TreeMap<>();
        for (Map.Entry<String, FieldMapper> entry : mappedSubFields.entrySet()) {
            FieldMapper.Builder propMergeBuilder = entry.getValue().getMergeBuilder();
            if (propMergeBuilder != null) {
                propBuilders.put(entry.getKey(), propMergeBuilder);
            }
        }
        b.properties.setValue(propBuilders);
        return b;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        if (fieldType().hasDocValues()) {
            return new SyntheticSourceSupport.Native(
                () -> new FlattenedDocValuesSyntheticFieldLoader(
                    fullPath(),
                    fullPath() + KEYED_FIELD_SUFFIX,
                    fieldType().ignoreAbove.valuesPotentiallyIgnored() ? fullPath() + KEYED_IGNORED_VALUES_FIELD_SUFFIX : null,
                    leafName(),
                    builder.usesBinaryDocValues,
                    toSubFieldLoaders(mappedSubFields),
                    builder.storeIgnoredFieldsInBinaryDocValues,
                    preserveLeafArrays
                )
            );
        }

        return super.syntheticSourceSupport();
    }
}
