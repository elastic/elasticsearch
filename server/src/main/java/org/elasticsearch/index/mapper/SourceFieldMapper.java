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
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class SourceFieldMapper extends MetadataFieldMapper {
    public static final String NAME = "_source";
    public static final String RECOVERY_SOURCE_NAME = "_recovery_source";

    public static final String CONTENT_TYPE = "_source";

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

    public static class Defaults {
        public static final String NAME = SourceFieldMapper.NAME;

        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE); // not indexed
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
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
        private final Parameter<Mode> mode = new Parameter<>(
            "mode",
            true,
            () -> getIndexMode() == IndexMode.TIME_SERIES ? Mode.SYNTHETIC : null,
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

        public Builder(IndexMode indexMode) {
            super(Defaults.NAME);
            this.indexMode = indexMode;
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
            if (mode.get() != null) {
                return false;
            }
            if (enabled.get().value() == false) {
                return false;
            }
            return includes.getValue().isEmpty() && excludes.getValue().isEmpty();
        }

        @Override
        public SourceFieldMapper build() {
            if (enabled.getValue().explicit() && mode.get() != null) {
                if (indexMode == IndexMode.TIME_SERIES) {
                    throw new MapperParsingException("Time series indices only support synthetic source");
                } else {
                    throw new MapperParsingException("Cannot set both [mode] and [enabled] parameters");
                }
            }
            if (isDefault()) {
                return indexMode == IndexMode.TIME_SERIES ? TSDB_DEFAULT : DEFAULT;
            }
            SourceFieldMapper sourceFieldMapper = new SourceFieldMapper(
                mode.get(),
                enabled.get(),
                includes.getValue().toArray(String[]::new),
                excludes.getValue().toArray(String[]::new),
                indexMode
            );
            if (indexMode != null) {
                indexMode.validateSourceFieldMapper(sourceFieldMapper);
            }
            return sourceFieldMapper;
        }

        private IndexMode getIndexMode() {
            return indexMode;
        }
    }

    public static final TypeParser PARSER = new ConfigurableTypeParser(
        c -> c.getIndexSettings().getMode() == IndexMode.TIME_SERIES ? TSDB_DEFAULT : DEFAULT,
        c -> new Builder(c.getIndexSettings().getMode())
    );

    static final class SourceFieldType extends MappedFieldType {

        private SourceFieldType(boolean enabled) {
            super(NAME, false, enabled, false, TextSearchInfo.NONE, Collections.emptyMap());
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
        if (this.sourceFilter != null && mode == Mode.SYNTHETIC) {
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
        return new Builder(indexMode).init(this);
    }

    /**
     * Build something to load source {@code _source}.
     */
    public SourceLoader newSourceLoader(Mapping mapping) {
        if (mode == Mode.SYNTHETIC) {
            return new SourceLoader.Synthetic(mapping);
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
