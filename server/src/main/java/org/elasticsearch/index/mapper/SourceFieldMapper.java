/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.engine.SearchBasedChangesSnapshot;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.subphase.FetchSourcePhase;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class SourceFieldMapper extends MetadataFieldMapper {
    public static final NodeFeature REMOVE_SYNTHETIC_SOURCE_ONLY_VALIDATION = new NodeFeature(
        "mapper.source.remove_synthetic_source_only_validation"
    );
    public static final NodeFeature SOURCE_MODE_FROM_INDEX_SETTING = new NodeFeature("mapper.source.mode_from_index_setting");
    public static final NodeFeature SYNTHETIC_RECOVERY_SOURCE = new NodeFeature("mapper.synthetic_recovery_source");

    public static final String NAME = "_source";
    public static final String RECOVERY_SOURCE_NAME = "_recovery_source";

    public static final String RECOVERY_SOURCE_SIZE_NAME = "_recovery_source_size";

    public static final String CONTENT_TYPE = "_source";

    public static final String LOSSY_PARAMETERS_ALLOWED_SETTING_NAME = "index.lossy.source-mapping-parameters";

    public static final String DEPRECATION_WARNING_TITLE = "Configuring source mode in mappings is deprecated.";

    public static final String DEPRECATION_WARNING = "Configuring source mode in mappings is deprecated and will be removed "
        + "in future versions. Use [index.mapping.source.mode] index setting instead.";

    /** The source mode */
    public enum Mode {
        DISABLED,
        STORED,
        SYNTHETIC,
        COLUMNAR_STORED
    }

    private static final SourceFieldMapper DEFAULT = new SourceFieldMapper(
        null,
        Explicit.IMPLICIT_TRUE,
        Strings.EMPTY_ARRAY,
        Strings.EMPTY_ARRAY,
        false,
        false
    );

    private static final SourceFieldMapper STORED = new SourceFieldMapper(
        Mode.STORED,
        Explicit.IMPLICIT_TRUE,
        Strings.EMPTY_ARRAY,
        Strings.EMPTY_ARRAY,
        false,
        false
    );

    private static final SourceFieldMapper SYNTHETIC = new SourceFieldMapper(
        Mode.SYNTHETIC,
        Explicit.IMPLICIT_TRUE,
        Strings.EMPTY_ARRAY,
        Strings.EMPTY_ARRAY,
        false,
        false
    );

    private static final SourceFieldMapper DISABLED = new SourceFieldMapper(
        Mode.DISABLED,
        Explicit.IMPLICIT_TRUE,
        Strings.EMPTY_ARRAY,
        Strings.EMPTY_ARRAY,
        false,
        false
    );

    public static class Defaults {
        public static final String NAME = SourceFieldMapper.NAME;

        public static final FieldType FIELD_TYPE;

        static {
            FieldType ft = new FieldType();
            ft.setIndexOptions(IndexOptions.NONE); // not indexed
            ft.setStored(true);
            ft.setOmitNorms(true);
            FIELD_TYPE = freezeAndDeduplicateFieldType(ft);
        }
    }

    private static SourceFieldMapper toType(FieldMapper in) {
        return (SourceFieldMapper) in;
    }

    public static class Builder extends MetadataFieldMapper.Builder {

        private final Parameter<Explicit<Boolean>> enabled = Parameter.explicitBoolParam("enabled", false, m -> toType(m).enabled, true)
            .setSerializerCheck((includeDefaults, isConfigured, value) -> value.explicit())
            // this field mapper may be enabled but once enabled, may not be disabled
            .setMergeValidator(
                (previous, current, conflicts) -> (previous.value() == current.value()) || (previous.value() && current.value() == false)
            );

        /*
         * The default mode for TimeSeries is left empty on purpose, so that mapping printings include the synthetic
         * source mode.
         */
        private final Parameter<Mode> mode;
        private final Parameter<List<String>> includes = Parameter.stringArrayParam(
            "includes",
            false,
            m -> Arrays.asList(toType(m).includes)
        );
        private final Parameter<List<String>> excludes = Parameter.stringArrayParam(
            "excludes",
            false,
            m -> Arrays.asList(toType(m).excludes)
        );

        private final Settings settings;

        private final IndexMode indexMode;
        private boolean serializeMode;
        private Mode fallbackMode;

        private final boolean supportsNonDefaultParameterValues;
        private final boolean sourceModeIsNoop;

        public Builder(
            IndexMode indexMode,
            final Settings settings,
            boolean sourceModeIsNoop,
            boolean supportsCheckForNonDefaultParams,
            boolean serializeMode
        ) {
            super(Defaults.NAME);
            this.settings = settings;
            this.indexMode = indexMode;
            this.supportsNonDefaultParameterValues = supportsCheckForNonDefaultParams == false
                || settings.getAsBoolean(LOSSY_PARAMETERS_ALLOWED_SETTING_NAME, true);
            this.sourceModeIsNoop = sourceModeIsNoop;
            this.serializeMode = serializeMode;
            this.mode = new Parameter<>("mode", true, () -> null, (n, c, o) -> Mode.valueOf(o.toString().toUpperCase(Locale.ROOT)), m -> {
                var sfm = toType(m);
                if (sfm.enabled.explicit()) {
                    return null;
                } else if (sfm.serializeMode) {
                    return sfm.mode;
                } else {
                    return null;
                }
            }, (b, n, v) -> b.field(n, v.toString().toLowerCase(Locale.ROOT)), v -> v.toString().toLowerCase(Locale.ROOT))
                .setMergeValidator((previous, current, conflicts) -> (previous == current) || current != Mode.STORED)
                // don't emit if `enabled` is configured
                .setSerializerCheck((includeDefaults, isConfigured, value) -> serializeMode && value != null);
        }

        public Builder setSynthetic() {
            this.mode.setValue(Mode.SYNTHETIC);
            return this;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { enabled, mode, includes, excludes };
        }

        private boolean isDefault() {
            return enabled.get().value() && includes.getValue().isEmpty() && excludes.getValue().isEmpty();
        }

        @Override
        public String contentType() {
            return CONTENT_TYPE;
        }

        @Override
        public SourceFieldMapper build() {
            if (enabled.getValue().explicit()) {
                if (mode.get() != null) {
                    throw new MapperParsingException("Cannot set both [mode] and [enabled] parameters");
                }
            }

            final Mode sourceMode = resolveSourceMode();

            if (supportsNonDefaultParameterValues == false) {
                List<String> disallowed = new ArrayList<>();
                if (enabled.get().value() == false) {
                    disallowed.add("enabled");
                }
                if (includes.get().isEmpty() == false) {
                    disallowed.add("includes");
                }
                if (excludes.get().isEmpty() == false) {
                    disallowed.add("excludes");
                }
                if (mode.get() == Mode.DISABLED) {
                    disallowed.add("mode=disabled");
                }
                if (disallowed.isEmpty() == false) {
                    throw new MapperParsingException(
                        disallowed.size() == 1
                            ? "Parameter [" + disallowed.get(0) + "] is not allowed in source"
                            : "Parameters [" + String.join(",", disallowed) + "] are not allowed in source"
                    );
                }
            }

            if (sourceMode == Mode.SYNTHETIC && (includes.getValue().isEmpty() == false || excludes.getValue().isEmpty() == false)) {
                throw new IllegalArgumentException("filtering the stored _source is incompatible with synthetic source");
            }
            if (mode.isConfigured() && sourceModeIsNoop == false) {
                serializeMode = true;
            }
            final SourceFieldMapper sourceFieldMapper;
            if (isDefault() && sourceMode == null) {
                // Needed for bwc so that "mode" is not serialized in case of a standard index with stored source.
                sourceFieldMapper = DEFAULT;
            } else if (isDefault() && serializeMode == false && sourceMode != null) {
                sourceFieldMapper = resolveStaticInstance(sourceMode);
            } else {
                sourceFieldMapper = new SourceFieldMapper(
                    sourceMode,
                    enabled.get(),
                    includes.getValue().toArray(Strings.EMPTY_ARRAY),
                    excludes.getValue().toArray(Strings.EMPTY_ARRAY),
                    serializeMode,
                    sourceModeIsNoop
                );
            }
            if (indexMode != null) {
                indexMode.validateSourceFieldMapper(sourceFieldMapper);
            }
            return sourceFieldMapper;
        }

        public boolean isSynthetic() {
            return resolveSourceMode() == Mode.SYNTHETIC;
        }

        public boolean isColumnarStored() {
            return resolveSourceMode() == Mode.COLUMNAR_STORED;
        }

        private Mode resolveSourceMode() {
            // If the `index.mapping.source.mode` exists it takes precedence to determine the source mode for `_source`
            // otherwise the mode is determined according to `_source.mode`.
            if (IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.exists(settings)) {
                return IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.get(settings);
            }

            // If `_source.mode` is not set we need to apply a default according to index mode.
            if (mode.get() == null || sourceModeIsNoop) {
                if (indexMode == null || indexMode == IndexMode.STANDARD) {
                    return fallbackMode;
                }

                return indexMode.defaultSourceMode();
            }

            return mode.get();
        }
    }

    private static SourceFieldMapper resolveStaticInstance(final Mode sourceMode) {
        return switch (sourceMode) {
            case SYNTHETIC -> SYNTHETIC;
            case STORED -> STORED;
            case DISABLED -> DISABLED;
            // COLUMNAR_STORED gets its own instance per mapping version so ColumnarSourceWriter can cache
            // instances of SourceLoader.Synthetic and related classes without conflating state across indices.
            case COLUMNAR_STORED -> new SourceFieldMapper(
                Mode.COLUMNAR_STORED,
                Explicit.IMPLICIT_TRUE,
                Strings.EMPTY_ARRAY,
                Strings.EMPTY_ARRAY,
                false,
                false
            );
        };
    }

    public static final TypeParser PARSER = new ConfigurableTypeParser(
        c -> new Builder(
            c.getIndexSettings().getMode(),
            c.getSettings(),
            c.indexVersionCreated().onOrAfter(IndexVersions.SOURCE_MAPPER_MODE_ATTRIBUTE_NOOP),
            c.indexVersionCreated().onOrAfter(IndexVersions.SOURCE_MAPPER_LOSSY_PARAMS_CHECK),
            onOrAfterDeprecateModeVersion(c.indexVersionCreated()) == false
        )
    );

    static final class SourceFieldType extends MappedFieldType {
        private final boolean enabled;

        private SourceFieldType(boolean enabled) {
            super(NAME, IndexType.NONE, enabled, Collections.emptyMap());
            this.enabled = enabled;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            throw new IllegalArgumentException("Cannot fetch values for internal field [" + name() + "].");
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            throw new QueryShardException(context, "The _source field is not searchable");
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new QueryShardException(context, "The _source field is not searchable");
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            if (enabled) {
                var config = blContext.blockLoaderFunctionConfig();
                if (config instanceof BlockLoaderFunctionConfig.TimeSeriesMetadata tsm) {
                    return new TimeSeriesMetadataFieldBlockLoader(blContext, tsm.loadMetricFields());
                }
                return new SourceFieldBlockLoader();
            }
            return ConstantNull.INSTANCE;
        }
    }

    // nullable for bwc reasons - TODO: fold this into serializeMode
    private final @Nullable Mode mode;
    private final boolean serializeMode;
    private final boolean sourceModeIsNoop;
    private final Explicit<Boolean> enabled;

    /** indicates whether the source will always exist and be complete, for use by features like the update API */
    private final boolean complete;

    private final String[] includes;
    private final String[] excludes;
    private final SourceFilter sourceFilter;
    private final ColumnarSourceWriter columnarSourceWriter;

    private SourceFieldMapper(
        Mode mode,
        Explicit<Boolean> enabled,
        String[] includes,
        String[] excludes,
        boolean serializeMode,
        boolean sourceModeIsNoop
    ) {
        super(new SourceFieldType((enabled.explicit() && enabled.value()) || (enabled.explicit() == false && mode != Mode.DISABLED)));
        this.mode = mode;
        this.enabled = enabled;
        this.sourceFilter = buildSourceFilter(includes, excludes);
        this.includes = includes;
        this.excludes = excludes;
        this.complete = stored() && sourceFilter == null;
        this.serializeMode = serializeMode;
        this.sourceModeIsNoop = sourceModeIsNoop;
        this.columnarSourceWriter = mode == Mode.COLUMNAR_STORED ? new ColumnarSourceWriter(sourceFilter) : null;
    }

    private static SourceFilter buildSourceFilter(String[] includes, String[] excludes) {
        if (CollectionUtils.isEmpty(includes) && CollectionUtils.isEmpty(excludes)) {
            return null;
        }
        return new SourceFilter(includes, excludes);
    }

    private boolean stored() {
        if (enabled.explicit() || mode == null) {
            return enabled.value();
        }
        return mode == Mode.STORED || mode == Mode.COLUMNAR_STORED;
    }

    public boolean enabled() {
        if (enabled.explicit()) {
            return enabled.value();
        }
        if (mode != null) {
            return mode != Mode.DISABLED;
        }
        return enabled.value();
    }

    public boolean isComplete() {
        return complete;
    }

    @Override
    public void preParse(DocumentParserContext context) throws IOException {
        SourceToParse.Source sourceObject = context.sourceToParse().source();
        XContentType contentType = sourceObject.xContentType();
        final boolean recoverySourceEnabled = context.indexSettings().isRecoverySourceEnabled();
        final boolean syntheticRecovery = recoverySourceEnabled && context.indexSettings().isRecoverySourceSyntheticEnabled();

        // Materializing the source bytes is only required when something downstream needs them:
        // - storing the regular _source field (stored() == true), or
        // - storing the reduced _recovery_source field (recovery enabled, non-synthetic).
        // The recovery-disabled case needs nothing at all, and the synthetic-recovery case needs
        // only a byte-size estimate, which the EIRF row can supply without re-serializing.
        if (stored() == false && (recoverySourceEnabled == false || syntheticRecovery)) {
            if (syntheticRecovery) {
                assert isSynthetic() : "Recovery source should not be disabled for non-synthetic sources";
                context.doc().add(new NumericDocValuesField(RECOVERY_SOURCE_SIZE_NAME, sourceObject.estimatedSizeInBytes()));
            }
            return;
        }

        final var originalSource = sourceObject.originalBytes();
        final var storedSource = stored() ? removeSyntheticVectorFields(context.mappingLookup(), originalSource, contentType) : null;
        final var adaptedStoredSource = applyFilters(context.mappingLookup(), storedSource, contentType, false);
        final boolean useColumnarSource = mode == Mode.COLUMNAR_STORED;

        if (adaptedStoredSource != null && useColumnarSource == false) {
            final BytesRef ref = adaptedStoredSource.toBytesRef();
            context.doc().add(new StoredField(fieldType().name(), ref.bytes, ref.offset, ref.length));
        }

        if (recoverySourceEnabled == false) {
            return;
        }

        assert useColumnarSource == false || syntheticRecovery : "columnar_stored source requires synthetic recovery source";

        if (syntheticRecovery) {
            assert isSynthetic() || isColumnarStored() : "Recovery source should not be disabled for non-synthetic sources";
            // Synthetic source recovery is enabled; omit the full recovery source.
            // Instead, store only the size of the uncompressed original source.
            // This size is used by LuceneSyntheticSourceChangesSnapshot to manage memory usage
            // when loading batches of synthetic sources during recovery.
            context.doc().add(new NumericDocValuesField(RECOVERY_SOURCE_SIZE_NAME, originalSource.length()));
        } else if (stored() == false || adaptedStoredSource != storedSource) {
            // If the source is missing (due to synthetic source, columnar_stored, or disabled mode)
            // or has been altered (via source filtering), store a reduced recovery source.
            // This includes the original source with synthetic vector fields removed for operation-based recovery.
            var recoverySource = removeSyntheticVectorFields(context.mappingLookup(), originalSource, contentType).toBytesRef();
            context.doc().add(new StoredField(RECOVERY_SOURCE_NAME, recoverySource.bytes, recoverySource.offset, recoverySource.length));
            context.doc().add(new NumericDocValuesField(RECOVERY_SOURCE_NAME, 1));
        }
    }

    @Override
    public void postParse(DocumentParserContext context) throws IOException {
        if (mode != Mode.COLUMNAR_STORED) {
            return;
        }
        try (var builder = XContentFactory.jsonBuilder()) {
            columnarSourceWriter.write(context, builder);
            BytesRef encodedValue = XContentDataHelper.encodeXContentBuilder(builder);
            // Remove per-field fallback entries collected during parsing — their contents are
            // subsumed by the whole-document entry written below, and binary doc values only allow
            // one field instance per document. Entries kept here (e.g. .offsets, _ignored) are
            // still used after indexing by block loaders or queries.
            context.doc().getFields().removeIf(f -> isRedundantInColumnarStoredSource(f.name()));
            IgnoredSourceFieldMapper.ignoredSourceFormat(context.indexSettings())
                .writeIgnoredFields(
                    List.of(new IgnoredSourceFieldMapper.NameValue(NAME, 0, encodedValue, context.doc())),
                    context.indexSettings().getIndexVersionCreated(),
                    false
                );
        }
    }

    /**
     * Returns {@code true} for Lucene fields that exist only to support per-field synthetic-source reconstruction and are therefore
     * redundant once {@link #postParse} has materialized the whole-document source blob into {@code _ignored_source}.
     *
     * <p>The following are removed:</p>
     * <ul>
     *   <li>{@code _ignored_source} per-field entries
     *   <li>{@code <field>._ignore_malformed} (and {@code .counts})</li>
     *   <li>{@code <field>._original} (and {@code .counts}) — the text / keyword fallback field for ignored-above and
     *       normalized values</li>
     *   <li>{@code <field>._on_failure} (and {@code .counts}) — the {@code doc_values.on_failure=ignore} failure column</li>
     * </ul>
     *
     */
    private static boolean isRedundantInColumnarStoredSource(String fieldName) {
        // A less expense check to avoid more string comparison:
        if (fieldName.indexOf('_') == -1) {
            return false;
        }

        String counts = MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;
        return IgnoredSourceFieldMapper.NAME.equals(fieldName)
            || (IgnoredSourceFieldMapper.NAME + counts).equals(fieldName)
            || fieldName.endsWith(IgnoreMalformedStoredValues.IGNORE_MALFORMED_FIELD_NAME_SUFFIX)
            || fieldName.endsWith(IgnoreMalformedStoredValues.IGNORE_MALFORMED_FIELD_NAME_SUFFIX + counts)
            || fieldName.endsWith(TextFamilyFieldType.FALLBACK_FIELD_NAME_SUFFIX)
            || fieldName.endsWith(TextFamilyFieldType.FALLBACK_FIELD_NAME_SUFFIX + counts)
            || fieldName.endsWith(OnFailureStoredValues.ON_FAILURE_FIELD_NAME_SUFFIX)
            || fieldName.endsWith(OnFailureStoredValues.ON_FAILURE_FIELD_NAME_SUFFIX + counts);
    }

    /**
     * Removes the synthetic vector fields (_inference and synthetic vector fields) from the {@code _source} if it is present.
     * These fields are regenerated at query or snapshot recovery time using stored fields and doc values.
     *
     * <p>For details on how the metadata is re-added, see:</p>
     * <ul>
     *   <li>{@link SearchBasedChangesSnapshot#addSyntheticFields(Source, int)}</li>
     *   <li>{@link FetchSourcePhase#getProcessor(FetchContext)}</li>
     * </ul>
     */
    private BytesReference removeSyntheticVectorFields(
        MappingLookup mappingLookup,
        @Nullable BytesReference originalSource,
        @Nullable XContentType contentType
    ) throws IOException {
        if (originalSource == null) {
            return null;
        }
        Set<String> excludes = new HashSet<>();
        if (InferenceMetadataFieldsMapper.isEnabled(mappingLookup) && mappingLookup.inferenceFields().isEmpty() == false) {
            excludes.add(InferenceMetadataFieldsMapper.NAME);
        }
        if (excludes.isEmpty() && mappingLookup.syntheticVectorFields().isEmpty()) {
            return originalSource;
        }
        BytesStreamOutput streamOutput = new BytesStreamOutput();
        XContentBuilder builder = new XContentBuilder(contentType.xContent(), streamOutput);
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                XContentParserConfiguration.EMPTY.withFiltering(Set.of(), excludes, true),
                originalSource,
                contentType
            )
        ) {
            if ((parser.currentToken() == null) && (parser.nextToken() == null)) {
                return originalSource;
            }
            // Removes synthetic vector fields from the source while preserving empty parent objects,
            // ensuring that the fields can later be rehydrated in their original locations.
            removeSyntheticVectorFields(builder.generator(), parser, "", mappingLookup.syntheticVectorFields());
            return BytesReference.bytes(builder);
        }
    }

    @Nullable
    public BytesReference applyFilters(
        MappingLookup mappingLookup,
        @Nullable BytesReference originalSource,
        @Nullable XContentType contentType,
        boolean removeMetadataFields
    ) throws IOException {
        if (stored() == false || originalSource == null) {
            return null;
        }
        var modSourceFilter = sourceFilter;
        if (removeMetadataFields
            && InferenceMetadataFieldsMapper.isEnabled(mappingLookup)
            && mappingLookup.inferenceFields().isEmpty() == false) {
            /*
             * Removes the {@link InferenceMetadataFieldsMapper} content from the {@code _source}.
             */
            String[] modExcludes = new String[excludes != null ? excludes.length + 1 : 1];
            if (excludes != null) {
                System.arraycopy(excludes, 0, modExcludes, 0, excludes.length);
            }
            modExcludes[modExcludes.length - 1] = InferenceMetadataFieldsMapper.NAME;
            modSourceFilter = new SourceFilter(includes, modExcludes);
        }
        if (modSourceFilter != null) {
            // Percolate and tv APIs may not set the source and that is ok, because these APIs will not index any data
            return Source.fromBytes(originalSource, contentType).filter(modSourceFilter).internalSourceRef();
        } else {
            return originalSource;
        }
    }

    @Override
    public boolean supportsColumnarParse(IndexSettings indexSettings) {
        // TODO: Need to implement support for additional scenarios
        // Columnar batch mapping only ports the cheap branch of preParse: no stored _source to
        // materialize, and either recovery source is disabled or only a size estimate is needed
        // (synthetic recovery). Stored source, COLUMNAR_STORED (stored() == true for that mode
        // too), and non-synthetic recovery source all require the full row path.
        final boolean recoverySourceEnabled = indexSettings.isRecoverySourceEnabled();
        final boolean syntheticRecovery = recoverySourceEnabled && indexSettings.isRecoverySourceSyntheticEnabled();
        return stored() == false && (recoverySourceEnabled == false || syntheticRecovery);
    }

    @Override
    public void preColumnarParse(BatchMappingContext context) throws IOException {
        final boolean syntheticRecovery = context.indexSettings().isRecoverySourceEnabled()
            && context.indexSettings().isRecoverySourceSyntheticEnabled();
        if (syntheticRecovery == false) {
            return;
        }

        final int docCount = context.docCount();
        final byte[] sizes = new byte[docCount * 8];
        for (int d = 0; d < docCount; d++) {
            final IndexRequest request = context.request(d);
            final XContentType contentType = request.getContentType() != null ? request.getContentType() : XContentType.JSON;
            ByteUtils.writeLongLE(SourceToParse.Source.fromBytes(request.source(), contentType).estimatedSizeInBytes(), sizes, d * 8);
        }
        context.addColumn(
            MappedColumns.longColumn(sizes, RECOVERY_SOURCE_SIZE_NAME, NumericDocValuesField.TYPE, LongColumn.NumericKind.LONG)
        );
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        Builder b = new Builder(null, Settings.EMPTY, sourceModeIsNoop, false, serializeMode);
        b.fallbackMode = this.mode;
        b.init(this);
        return b;
    }

    public boolean isSynthetic() {
        return mode == Mode.SYNTHETIC;
    }

    public boolean isColumnarStored() {
        return mode == Mode.COLUMNAR_STORED;
    }

    /**
     * Caution: this function is not aware of the legacy "mappings._source.mode" parameter that some legacy indices might use. You should
     * prefer to get information about synthetic source from {@link MapperBuilderContext}.
     */
    public static boolean isSynthetic(IndexSettings indexSettings) {
        return IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.get(indexSettings.getSettings()) == SourceFieldMapper.Mode.SYNTHETIC;
    }

    public static boolean isStored(IndexSettings indexSettings) {
        return IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.get(indexSettings.getSettings()) == Mode.STORED;
    }

    public boolean isDisabled() {
        return mode == Mode.DISABLED;
    }

    public boolean isStored() {
        return mode == null || mode == Mode.STORED;
    }

    public static boolean onOrAfterDeprecateModeVersion(IndexVersion version) {
        return version.onOrAfter(IndexVersions.DEPRECATE_SOURCE_MODE_MAPPER)
            || version.between(IndexVersions.V8_DEPRECATE_SOURCE_MODE_MAPPER, IndexVersions.UPGRADE_TO_LUCENE_10_0_0);
    }

    private static void removeSyntheticVectorFields(
        XContentGenerator destination,
        XContentParser parser,
        String fullPath,
        Set<String> patchFullPaths
    ) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.FIELD_NAME) {
            String fieldName = parser.currentName();
            token = parser.nextToken();
            fullPath = fullPath + (fullPath.isEmpty() ? "" : ".") + fieldName;
            if (patchFullPaths.contains(fullPath)) {
                parser.skipChildren();
                return;
            }
            destination.writeFieldName(fieldName);
        }

        switch (token) {
            case START_ARRAY -> {
                destination.writeStartArray();
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    removeSyntheticVectorFields(destination, parser, fullPath, patchFullPaths);
                }
                destination.writeEndArray();
            }
            case START_OBJECT -> {
                destination.writeStartObject();
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    removeSyntheticVectorFields(destination, parser, fullPath, patchFullPaths);
                }
                destination.writeEndObject();
            }
            default -> // others are simple:
                destination.copyCurrentEvent(parser);
        }
    }
}
