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

public class SourceFieldMapper extends MetadataFieldMapper {
    public static final String NAME = "_source";
    public static final String RECOVERY_SOURCE_NAME = "_recovery_source";

    public static final String CONTENT_TYPE = "_source";
    private final XContentFieldFilter filter;

    private static final SourceFieldMapper DEFAULT = new SourceFieldMapper(Defaults.ENABLED, Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY);

    public static class Defaults {
        public static final String NAME = SourceFieldMapper.NAME;
        public static final boolean ENABLED = true;

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

        private final Parameter<Boolean> enabled = Parameter.boolParam("enabled", false, m -> toType(m).enabled, Defaults.ENABLED)
            // this field mapper may be enabled but once enabled, may not be disabled
            .setMergeValidator((previous, current, conflicts) -> (previous == current) || (previous && current == false));
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
        protected List<Parameter<?>> getParameters() {
            return List.of(enabled, includes, excludes);
        }

        @Override
        public SourceFieldMapper build() {
            if (enabled.getValue() == Defaults.ENABLED && includes.getValue().isEmpty() && excludes.getValue().isEmpty()) {
                return DEFAULT;
            }
            return new SourceFieldMapper(
                enabled.getValue(),
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

    private final boolean enabled;
    /** indicates whether the source will always exist and be complete, for use by features like the update API */
    private final boolean complete;

    private final String[] includes;
    private final String[] excludes;

    private SourceFieldMapper(boolean enabled, String[] includes, String[] excludes) {
        super(new SourceFieldType(enabled));
        this.enabled = enabled;
        this.includes = includes;
        this.excludes = excludes;
        final boolean filtered = CollectionUtils.isEmpty(includes) == false || CollectionUtils.isEmpty(excludes) == false;
        this.filter = enabled && filtered
            ? XContentFieldFilter.newFieldFilter(includes, excludes)
            : (sourceBytes, contentType) -> sourceBytes;
        this.complete = enabled && CollectionUtils.isEmpty(includes) && CollectionUtils.isEmpty(excludes);
    }

    public boolean enabled() {
        return enabled;
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
        if (enabled && originalSource != null) {
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
}
