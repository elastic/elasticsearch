/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.record;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockSourceReader;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.DynamicFieldType;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.TextParams;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldArrayContext;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldParser;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.index.IndexSettings.IGNORE_ABOVE_SETTING;

/**
 * A mapper for the {@code record} field type: schema-free recursive JSON with array-of-object
 * isolation. Phase 0 — identical indexing behaviour to {@code flattened} (leaf values encoded as
 * {@code key\0value} keyword terms). Phase 1 will add child Lucene documents for arrays of objects,
 * enabling correlated queries via {@code RecordQueryBuilder}.
 *
 * <p>Like {@code flattened}, every leaf value (regardless of its JSON type) is indexed as a keyword
 * string. Numeric ranges and date handling are intentionally unsupported; this keeps the type
 * schema-free without per-key type metadata.
 */
public final class RecordFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "record";

    /**
     * The internal suffix for the keyed inverted-index field that stores {@code key\0value} terms.
     * Identical to {@code FlattenedFieldMapper.KEYED_FIELD_SUFFIX} so that tooling that already
     * understands flattened's encoding can read record fields without modification.
     */
    public static final String KEYED_FIELD_SUFFIX = "._keyed";

    private static final int DEFAULT_DEPTH_LIMIT = 20;

    private static Builder builder(Mapper in) {
        return ((RecordFieldMapper) in).builder;
    }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<Integer> depthLimit = Parameter.intParam(
            "depth_limit",
            true,
            m -> builder(m).depthLimit.get(),
            DEFAULT_DEPTH_LIMIT
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

        private final Parameter<Integer> ignoreAbove;

        private final Parameter<String> indexOptions = TextParams.keywordIndexOptions(m -> builder(m).indexOptions.get());
        private final Parameter<SimilarityProvider> similarity = TextParams.similarity(m -> builder(m).similarity.get());

        private final Parameter<Boolean> splitQueriesOnWhitespace = Parameter.boolParam(
            "split_queries_on_whitespace",
            true,
            m -> builder(m).splitQueriesOnWhitespace.get(),
            false
        );

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final int ignoreAboveDefault;

        public Builder(final String name, IndexSettings indexSettings) {
            this(name, Mapper.IgnoreAbove.getIgnoreAboveDefaultValue(indexSettings.getMode(), indexSettings.getIndexVersionCreated()));
        }

        Builder(String name, MappingParserContext ctx) {
            this(name, IGNORE_ABOVE_SETTING.get(ctx.getSettings()));
            this.indexed.setValue(ctx.getIndexSettings().isIndexDisabledByDefault() == false);
        }

        private Builder(String name, int ignoreAboveDefault) {
            super(name);
            this.indexed = Parameter.indexParam(m -> builder(m).indexed.get(), true);
            this.ignoreAboveDefault = ignoreAboveDefault;
            this.ignoreAbove = Parameter.ignoreAboveParam(m -> builder(m).ignoreAbove.get(), ignoreAboveDefault);
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
                meta };
        }

        @Override
        public String contentType() {
            return CONTENT_TYPE;
        }

        @Override
        public RecordFieldMapper build(MapperBuilderContext context) {
            if (multiFieldsBuilder.build(this, context).iterator().hasNext()) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + leafName() + "] does not support [fields]");
            }
            if (copyTo.copyToFields().isEmpty() == false) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + leafName() + "] does not support [copy_to]");
            }
            String fullName = context.buildFullName(leafName());
            Mapper.IgnoreAbove ignoreAboveObj = new Mapper.IgnoreAbove(ignoreAbove.getValue());
            MappedFieldType ft = new RootRecordFieldType(
                fullName,
                IndexType.terms(indexed.get(), hasDocValues.get()),
                meta.get(),
                splitQueriesOnWhitespace.get(),
                eagerGlobalOrdinals.get(),
                ignoreAboveObj,
                nullValue.get(),
                context.isSourceSynthetic()
            );
            return new RecordFieldMapper(leafName(), ft, builderParams(this, context), this);
        }
    }

    public static final FieldMapper.TypeParser PARSER = new FieldMapper.TypeParser((name, ctx) -> new Builder(name, ctx));

    // -------------------------------------------------------------------------
    // Root field type (the "record" field itself)
    // -------------------------------------------------------------------------

    /**
     * The field type for the root {@code record} field. Queries on the root field match any leaf
     * value in the JSON object. Sub-key access ({@code record.foo.bar}) is handled by
     * {@link #getChildFieldType(String)}, which returns a {@link KeyedRecordFieldType}.
     */
    public static final class RootRecordFieldType extends StringFieldType implements DynamicFieldType {

        private final boolean splitQueriesOnWhitespace;
        private final boolean eagerGlobalOrdinals;
        private final Mapper.IgnoreAbove ignoreAbove;
        private final String nullValue;
        private final boolean isSyntheticSourceEnabled;

        RootRecordFieldType(
            String name,
            IndexType indexType,
            Map<String, String> meta,
            boolean splitQueriesOnWhitespace,
            boolean eagerGlobalOrdinals,
            Mapper.IgnoreAbove ignoreAbove,
            String nullValue,
            boolean isSyntheticSourceEnabled
        ) {
            super(
                name,
                indexType,
                false,
                splitQueriesOnWhitespace ? TextSearchInfo.WHITESPACE_MATCH_ONLY : TextSearchInfo.SIMPLE_MATCH_ONLY,
                meta
            );
            this.splitQueriesOnWhitespace = splitQueriesOnWhitespace;
            this.eagerGlobalOrdinals = eagerGlobalOrdinals;
            this.ignoreAbove = ignoreAbove;
            this.nullValue = nullValue;
            this.isSyntheticSourceEnabled = isSyntheticSourceEnabled;
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

        @Override
        public boolean eagerGlobalOrdinals() {
            return eagerGlobalOrdinals;
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            return ((BytesRef) value).utf8ToString();
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            // Use the keyed field for fielddata so that sorting/aggs see key\0value terms,
            // matching what flattened does when hasRootDocValues==false.
            return new SortedSetOrdinalsIndexFieldData.Builder(
                name() + KEYED_FIELD_SUFFIX,
                CoreValuesSourceType.KEYWORD,
                (dv, n) -> null   // fielddata on record root is not supported for now
            );
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            return new SourceValueFetcher(
                context.isSourceEnabled() ? context.sourcePath(name()) : Collections.emptySet(),
                null,
                context.getIndexSettings().getIgnoredSourceFormat()
            ) {
                @Override
                protected Object parseSourceValue(Object value) {
                    return value;
                }
            };
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            // Phase 0: always load from _source. The keyed doc-values block loader
            // (RootFlattenedDocValuesBlockLoader) can be wired up in a follow-up.
            ValueFetcher fetcher = new SourceValueFetcher(
                blContext.sourcePaths(name()),
                null,
                blContext.indexSettings().getIgnoredSourceFormat()
            ) {
                @Override
                protected Object parseSourceValue(Object value) {
                    return value;
                }
            };
            return new BlockSourceReader.BytesRefsBlockLoader(fetcher, BlockSourceReader.lookupMatchingAll());
        }

        @Override
        public MappedFieldType getChildFieldType(String childPath) {
            return new KeyedRecordFieldType(name(), childPath, this);
        }
    }

    // -------------------------------------------------------------------------
    // Keyed field type (record.some.key)
    // -------------------------------------------------------------------------

    /**
     * Returned by {@link RootRecordFieldType#getChildFieldType(String)} when a user queries a
     * specific sub-key, e.g. {@code record.user.name}. Queries are routed to the keyed inverted
     * index ({@code record._keyed}) using the {@code key\0value} encoding.
     */
    public static final class KeyedRecordFieldType extends StringFieldType {

        private final String key;
        private final String rootName;

        KeyedRecordFieldType(String rootName, String key, RootRecordFieldType root) {
            super(
                rootName + KEYED_FIELD_SUFFIX,
                root.indexType(),
                false,
                root.splitQueriesOnWhitespace ? TextSearchInfo.WHITESPACE_MATCH_ONLY : TextSearchInfo.SIMPLE_MATCH_ONLY,
                root.meta()
            );
            this.key = key;
            this.rootName = rootName;
        }

        public String key() {
            return key;
        }

        public String rootName() {
            return rootName;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        protected BytesRef indexedValueForSearch(Object value) {
            String keyedValue = FlattenedFieldParser.createKeyedValue(key, value.toString());
            return new BytesRef(keyedValue);
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            // Match any term with the prefix "key\0" — i.e. any value under this key.
            String keyPrefix = FlattenedFieldParser.createKeyedValue(key, ""); // "key\0"
            BytesRef lower = new BytesRef(keyPrefix);
            // Exclusive upper: bump the last byte (\0) to \1 so the range covers key\0anything.
            BytesRef upper = new BytesRef(keyPrefix);
            upper.bytes[upper.offset + upper.length - 1] = (byte) 0x01;
            return new TermRangeQuery(name(), lower, upper, true, false);
        }

        @Override
        public Query prefixQuery(
            String value,
            MultiTermQuery.RewriteMethod method,
            boolean caseInsensitive,
            SearchExecutionContext context
        ) {
            // Prefix on a keyed field: "key\0value_prefix"
            String keyedPrefix = FlattenedFieldParser.createKeyedValue(key, value);
            PrefixQuery query = method == null
                ? new PrefixQuery(new Term(name(), keyedPrefix))
                : new PrefixQuery(new Term(name(), keyedPrefix), method);
            return query;
        }

        @Override
        public Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            SearchExecutionContext context
        ) {
            // Values are stored as keyword strings, so string range.
            String lower = lowerTerm == null
                ? FlattenedFieldParser.createKeyedValue(key, "")
                : FlattenedFieldParser.createKeyedValue(key, lowerTerm.toString());
            String upper = upperTerm == null
                ? FlattenedFieldParser.createKeyedValue(key, "￿￿")
                : FlattenedFieldParser.createKeyedValue(key, upperTerm.toString());
            // When the lower is the key prefix (open range), include it.
            boolean actualIncludeLower = lowerTerm == null ? true : includeLower;
            boolean actualIncludeUpper = upperTerm == null ? false : includeUpper;
            return new TermRangeQuery(name(), new BytesRef(lower), new BytesRef(upper), actualIncludeLower, actualIncludeUpper);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), CoreValuesSourceType.KEYWORD, (dv, n) -> null);
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            // Values for a specific key are fetched from _source.
            throw new IllegalArgumentException(
                "Field ["
                    + rootName
                    + "."
                    + key
                    + "] of type ["
                    + typeName()
                    + "] does not support [fields] retrieval directly; "
                    + "retrieve the parent field ["
                    + rootName
                    + "] instead."
            );
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            // Phase 0: not yet wired to keyed doc-values block loader.
            throw new UnsupportedOperationException("blockLoader not yet supported for keyed record fields");
        }
    }

    // -------------------------------------------------------------------------
    // Mapper itself
    // -------------------------------------------------------------------------

    private final Builder builder;
    private final FlattenedFieldParser fieldParser;

    private RecordFieldMapper(String leafName, MappedFieldType mappedFieldType, BuilderParams builderParams, Builder builder) {
        super(leafName, mappedFieldType, builderParams);
        this.builder = builder;
        this.fieldParser = new FlattenedFieldParser(
            mappedFieldType.name(),
            mappedFieldType.name() + KEYED_FIELD_SUFFIX,
            // No ignored-values field for Phase 0 (no synthetic source support yet).
            mappedFieldType.name() + KEYED_FIELD_SUFFIX + "._ignored",
            mappedFieldType,
            builder.depthLimit.get(),
            builder.ignoreAbove.get(),
            builder.nullValue.get(),
            /* usesBinaryDocValues= */ false,
            /* hasRootDocValues= */ false,
            /* mappedSubFields= */ Collections.emptyMap(),
            /* storeIgnoredFieldsInBinaryDocValues= */ false,
            /* preserveLeafArrays= */ null,     // LOSSY — no array-order tracking in Phase 0
            /* indexVersion= */ IndexVersion.current(),
            /* writeDimensionRouting= */ false,
            /* usesArrayOrderBinaryDocValues= */ false
        );
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), Lucene.KEYWORD_ANALYZER);
    }

    @Override
    public RootRecordFieldType fieldType() {
        return (RootRecordFieldType) super.fieldType();
    }

    @Override
    protected boolean supportsParsingObject() {
        return true;
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

        try {
            context.path().setWithinLeafObject(true);
            // FlattenedFieldParser traverses the JSON object recursively, encoding every leaf as
            // a key\0value term. For Phase 0 we pass null for the array-offset context (LOSSY
            // array handling) — arrays of scalars are fine; arrays of objects lose object identity.
            fieldParser.parse(context, (FlattenedFieldArrayContext) null);
        } finally {
            context.path().setWithinLeafObject(false);
        }

        if (mappedFieldType.hasDocValues() == false) {
            context.addToFieldNames(fieldType().name());
        }
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        Builder b = new Builder(leafName(), builder.ignoreAboveDefault);
        b.init(this);
        return b;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        // Phase 0: no native synthetic source loader; fall back to storing _source.
        return super.syntheticSourceSupport();
    }
}
