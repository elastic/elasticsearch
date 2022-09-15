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
import org.elasticsearch.common.xcontent.XContentFieldFilter;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
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
    private final XContentFieldFilter filter;

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
        Strings.EMPTY_ARRAY
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

        public Builder() {
            super(Defaults.NAME);
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
                throw new MapperParsingException("Cannot set both [mode] and [enabled] parameters");
            }
            if (isDefault()) {
                return DEFAULT;
            }
            return new SourceFieldMapper(
                mode.get(),
                enabled.get(),
                includes.getValue().toArray(String[]::new),
                excludes.getValue().toArray(String[]::new)
            );
        }
    }

    public static final TypeParser PARSER = new ConfigurableTypeParser(c -> DEFAULT, c -> new Builder());

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

    private SourceFieldMapper(Mode mode, Explicit<Boolean> enabled, String[] includes, String[] excludes) {
        super(new SourceFieldType((enabled.explicit() && enabled.value()) || (enabled.explicit() == false && mode != Mode.DISABLED)));
        assert enabled.explicit() == false || mode == null;
        this.mode = mode;
        this.enabled = enabled;
        this.includes = includes;
        this.excludes = excludes;
        final boolean filtered = CollectionUtils.isEmpty(includes) == false || CollectionUtils.isEmpty(excludes) == false;
        if (filtered && mode == Mode.SYNTHETIC) {
            throw new IllegalArgumentException("filtering the stored _source is incompatible with synthetic source");
        }
        this.filter = stored() && filtered
            ? XContentFieldFilter.newFieldFilter(includes, excludes)
            : (sourceBytes, contentType) -> sourceBytes;
        this.complete = stored() && CollectionUtils.isEmpty(includes) && CollectionUtils.isEmpty(excludes);
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
        if (stored() && originalSource != null) {
            // Percolate and tv APIs may not set the source and that is ok, because these APIs will not index any data
            return filter.apply(originalSource, contentType);
        } else {
            return null;
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder().init(this);
    }

    /**
     * Build something to load source {@code _source}.
     */
    public <T> SourceLoader newSourceLoader(Mapping mapping) {
        if (mode == Mode.SYNTHETIC) {
            return new SourceLoader.Synthetic(mapping);
        }
        return SourceLoader.FROM_STORED_SOURCE;
    }

    public boolean isSynthetic() {
        return mode == Mode.SYNTHETIC;
    }
}
