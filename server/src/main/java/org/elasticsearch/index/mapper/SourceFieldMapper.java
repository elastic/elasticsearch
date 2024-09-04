/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class SourceFieldMapper extends MetadataFieldMapper {
    public static final NodeFeature SYNTHETIC_SOURCE_FALLBACK = new NodeFeature("mapper.source.synthetic_source_fallback");
    public static final NodeFeature SYNTHETIC_SOURCE_STORED_FIELDS_ADVANCE_FIX = new NodeFeature(
        "mapper.source.synthetic_source_stored_fields_advance_fix"
    );

    public static final String NAME = "_source";
    public static final String RECOVERY_SOURCE_NAME = "_recovery_source";

    public static final String CONTENT_TYPE = "_source";

    public static final String LOSSY_PARAMETERS_ALLOWED_SETTING_NAME = "index.lossy.source-mapping-parameters";

    /** The source mode */
    private enum Mode {
        DISABLED,
        STORED,
        SYNTHETIC
    }

    private static final SourceFieldMapper DEFAULT = new SourceFieldMapper(
        null,
        Explicit.IMPLICIT_TRUE,
        Strings.EMPTY_ARRAY,
        Strings.EMPTY_ARRAY,
        null
    );

    private static final SourceFieldMapper TSDB_DEFAULT = new SourceFieldMapper(
        Mode.SYNTHETIC,
        Explicit.IMPLICIT_TRUE,
        Strings.EMPTY_ARRAY,
        Strings.EMPTY_ARRAY,
        IndexMode.TIME_SERIES
    );

    private static final SourceFieldMapper LOGSDB_DEFAULT = new SourceFieldMapper(
        Mode.SYNTHETIC,
        Explicit.IMPLICIT_TRUE,
        Strings.EMPTY_ARRAY,
        Strings.EMPTY_ARRAY,
        IndexMode.LOGSDB
    );

    /*
     * Synthetic source was added as the default for TSDB in v.8.7. The legacy field mapper below
     * is used in bwc tests and mixed clusters containing time series indexes created in an earlier version.
     */
    private static final SourceFieldMapper TSDB_LEGACY_DEFAULT = new SourceFieldMapper(
        null,
        Explicit.IMPLICIT_TRUE,
        Strings.EMPTY_ARRAY,
        Strings.EMPTY_ARRAY,
        IndexMode.TIME_SERIES
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
        private final Parameter<Mode> mode = new Parameter<>(
            "mode",
            true,
            () -> null,
            (n, c, o) -> Mode.valueOf(o.toString().toUpperCase(Locale.ROOT)),
            m -> toType(m).enabled.explicit() ? null : toType(m).mode,
            (b, n, v) -> b.field(n, v.toString().toLowerCase(Locale.ROOT)),
            v -> v.toString().toLowerCase(Locale.ROOT)
        ).setMergeValidator((previous, current, conflicts) -> (previous == current) || current != Mode.STORED)
            .setSerializerCheck((includeDefaults, isConfigured, value) -> value != null); // don't emit if `enabled` is configured
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

        private final IndexMode indexMode;

        private final boolean supportsNonDefaultParameterValues;

        public Builder(IndexMode indexMode, final Settings settings, boolean supportsCheckForNonDefaultParams) {
            super(Defaults.NAME);
            this.indexMode = indexMode;
            this.supportsNonDefaultParameterValues = supportsCheckForNonDefaultParams == false
                || settings.getAsBoolean(LOSSY_PARAMETERS_ALLOWED_SETTING_NAME, true);
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
            Mode m = mode.get();
            if (m != null
                && (((indexMode != null && indexMode.isSyntheticSourceEnabled() && m == Mode.SYNTHETIC) == false) || m == Mode.DISABLED)) {
                return false;
            }
            return enabled.get().value() && includes.getValue().isEmpty() && excludes.getValue().isEmpty();
        }

        @Override
        public SourceFieldMapper build() {
            if (enabled.getValue().explicit()) {
                if (indexMode != null && indexMode.isSyntheticSourceEnabled()) {
                    throw new MapperParsingException("Indices with with index mode [" + indexMode + "] only support synthetic source");
                }
                if (mode.get() != null) {
                    throw new MapperParsingException("Cannot set both [mode] and [enabled] parameters");
                }
            }
            if (isDefault()) {
                return switch (indexMode) {
                    case TIME_SERIES -> TSDB_DEFAULT;
                    case LOGSDB -> LOGSDB_DEFAULT;
                    default -> DEFAULT;
                };
            }
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
            SourceFieldMapper sourceFieldMapper = new SourceFieldMapper(
                mode.get(),
                enabled.get(),
                includes.getValue().toArray(Strings.EMPTY_ARRAY),
                excludes.getValue().toArray(Strings.EMPTY_ARRAY),
                indexMode
            );
            if (indexMode != null) {
                indexMode.validateSourceFieldMapper(sourceFieldMapper);
            }
            return sourceFieldMapper;
        }

    }

    public static final TypeParser PARSER = new ConfigurableTypeParser(c -> {
        var indexMode = c.getIndexSettings().getMode();
        if (indexMode.isSyntheticSourceEnabled()) {
            if (indexMode == IndexMode.TIME_SERIES) {
                if (c.getIndexSettings().getIndexVersionCreated().onOrAfter(IndexVersions.V_8_7_0)) {
                    return TSDB_DEFAULT;
                } else {
                    return TSDB_LEGACY_DEFAULT;
                }
            } else if (indexMode == IndexMode.LOGSDB) {
                return LOGSDB_DEFAULT;
            }
        }
        return DEFAULT;
    },
        c -> new Builder(
            c.getIndexSettings().getMode(),
            c.getSettings(),
            c.indexVersionCreated().onOrAfter(IndexVersions.SOURCE_MAPPER_LOSSY_PARAMS_CHECK)
        )
    );

    static final class SourceFieldType extends MappedFieldType {
        private final boolean enabled;

        private SourceFieldType(boolean enabled) {
            super(NAME, false, enabled, false, TextSearchInfo.NONE, Collections.emptyMap());
            this.enabled = enabled;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for internal field [" + name() + "].");
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
                return new SourceFieldBlockLoader();
            }
            return BlockLoader.CONSTANT_NULLS;
        }
    }

    // nullable for bwc reasons
    private final @Nullable Mode mode;
    private final Explicit<Boolean> enabled;

    /** indicates whether the source will always exist and be complete, for use by features like the update API */
    private final boolean complete;

    private final String[] includes;
    private final String[] excludes;
    private final SourceFilter sourceFilter;

    private final IndexMode indexMode;

    private SourceFieldMapper(Mode mode, Explicit<Boolean> enabled, String[] includes, String[] excludes, IndexMode indexMode) {
        super(new SourceFieldType((enabled.explicit() && enabled.value()) || (enabled.explicit() == false && mode != Mode.DISABLED)));
        assert enabled.explicit() == false || mode == null;
        this.mode = mode;
        this.enabled = enabled;
        this.sourceFilter = buildSourceFilter(includes, excludes);
        this.includes = includes;
        this.excludes = excludes;
        if (this.sourceFilter != null && (mode == Mode.SYNTHETIC || indexMode == IndexMode.TIME_SERIES)) {
            throw new IllegalArgumentException("filtering the stored _source is incompatible with synthetic source");
        }
        this.complete = stored() && sourceFilter == null;
        this.indexMode = indexMode;
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
        return mode == Mode.STORED;
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
        BytesReference originalSource = context.sourceToParse().source();
        XContentType contentType = context.sourceToParse().getXContentType();
        final BytesReference adaptedSource = applyFilters(originalSource, contentType);

        if (adaptedSource != null) {
            assert context.indexSettings().getIndexVersionCreated().before(IndexVersions.V_8_7_0)
                || indexMode == null
                || indexMode.isSyntheticSourceEnabled() == false;
            final BytesRef ref = adaptedSource.toBytesRef();
            context.doc().add(new StoredField(fieldType().name(), ref.bytes, ref.offset, ref.length));
        }

        if (originalSource != null && adaptedSource != originalSource) {
            // if we omitted source or modified it we add the _recovery_source to ensure we have it for ops based recovery
            BytesRef ref = originalSource.toBytesRef();
            context.doc().add(new StoredField(RECOVERY_SOURCE_NAME, ref.bytes, ref.offset, ref.length));
            context.doc().add(new NumericDocValuesField(RECOVERY_SOURCE_NAME, 1));
        }
    }

    @Nullable
    public BytesReference applyFilters(@Nullable BytesReference originalSource, @Nullable XContentType contentType) throws IOException {
        if (stored() == false) {
            return null;
        }
        if (originalSource != null && sourceFilter != null) {
            // Percolate and tv APIs may not set the source and that is ok, because these APIs will not index any data
            return Source.fromBytes(originalSource, contentType).filter(sourceFilter).internalSourceRef();
        } else {
            return originalSource;
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(indexMode, Settings.EMPTY, false).init(this);
    }

    /**
     * Build something to load source {@code _source}.
     */
    public SourceLoader newSourceLoader(Mapping mapping, SourceFieldMetrics metrics) {
        if (mode == Mode.SYNTHETIC) {
            return new SourceLoader.Synthetic(mapping::syntheticFieldLoader, metrics);
        }
        return SourceLoader.FROM_STORED_SOURCE;
    }

    public boolean isSynthetic() {
        return mode == Mode.SYNTHETIC;
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        return SourceLoader.SyntheticFieldLoader.NOTHING;
    }
}
