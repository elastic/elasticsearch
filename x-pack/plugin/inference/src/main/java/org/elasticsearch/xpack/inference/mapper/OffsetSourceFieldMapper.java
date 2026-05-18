/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.IndexVersions.SEMANTIC_FIELD_TYPE;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A {@link FieldMapper} that maps a field name to its start and end offsets.
 * The {@link CharsetFormat} used to compute the offsets is specified via the charset parameter.
 * Currently, only {@link CharsetFormat#UTF_16} is supported, aligning with Java's {@code String} charset
 * for simpler internal usage and integration.
 *
 * Each document can store at most one value in this field.
 *
 * Note: This mapper is not yet documented and is intended exclusively for internal use by
 * {@link SemanticTextFieldMapper}. If exposing this mapper directly to users becomes necessary,
 * extending charset compatibility should be considered, as the current default (and sole supported charset)
 * was chosen for ease of Java integration.
 */
public class OffsetSourceFieldMapper extends FieldMapper {
    public static final String CONTENT_TYPE = "offset_source";

    private static final String SOURCE_NAME_FIELD = "field";
    private static final String START_OFFSET_FIELD = "start";
    private static final String END_OFFSET_FIELD = "end";
    private static final String INPUT_INDEX_FIELD = "input_index";

    public static final class OffsetSource implements ToXContentObject {
        private static final int NO_OFFSET = -1;

        private final String field;
        private final int start;
        private final int end;
        @Nullable
        private final Integer inputIndex;

        public OffsetSource(String field, int start, int end) {
            if (start < 0 || end < 0) {
                throw new IllegalArgumentException("Illegal offsets, expected positive numbers, got: " + start + ":" + end);
            }
            if (start > end) {
                throw new IllegalArgumentException("Illegal offsets, expected start < end, got: " + start + " > " + end);
            }
            this.field = Objects.requireNonNull(field);
            this.start = start;
            this.end = end;
            this.inputIndex = null;
        }

        public OffsetSource(String field, int inputIndex) {
            if (inputIndex < 0) {
                throw new IllegalArgumentException("Illegal input index, expected non-negative, got: " + inputIndex);
            }
            this.field = Objects.requireNonNull(field);
            this.start = NO_OFFSET;
            this.end = NO_OFFSET;
            this.inputIndex = inputIndex;
        }

        public String field() {
            return field;
        }

        public int start() {
            return start;
        }

        public int end() {
            return end;
        }

        @Nullable
        public Integer inputIndex() {
            return inputIndex;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(SOURCE_NAME_FIELD, field);
            if (inputIndex != null) {
                builder.field(INPUT_INDEX_FIELD, inputIndex);
            } else {
                builder.field(START_OFFSET_FIELD, start);
                builder.field(END_OFFSET_FIELD, end);
            }
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OffsetSource that = (OffsetSource) o;
            return start == that.start
                && end == that.end
                && Objects.equals(field, that.field)
                && Objects.equals(inputIndex, that.inputIndex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, start, end, inputIndex);
        }

        @Override
        public String toString() {
            return inputIndex != null
                ? "OffsetSource[field=" + field + ", inputIndex=" + inputIndex + "]"
                : "OffsetSource[field=" + field + ", start=" + start + ", end=" + end + "]";
        }

        private boolean hasOffsets() {
            return start != NO_OFFSET && end != NO_OFFSET;
        }
    }

    private static final ConstructingObjectParser<OffsetSource, Void> OFFSET_SOURCE_PARSER = new ConstructingObjectParser<>(
        CONTENT_TYPE,
        true,
        args -> {
            String field = (String) args[0];
            Integer start = (Integer) args[1];
            Integer end = (Integer) args[2];
            Integer inputIndex = (Integer) args[3];
            if (inputIndex != null) {
                if (start != null || end != null) {
                    throw new IllegalArgumentException(
                        "["
                            + CONTENT_TYPE
                            + "] field must not specify both ["
                            + INPUT_INDEX_FIELD
                            + "] and ["
                            + START_OFFSET_FIELD
                            + "]/["
                            + END_OFFSET_FIELD
                            + "]"
                    );
                }
                return new OffsetSource(field, inputIndex);
            }

            if (start == null || end == null) {
                throw new IllegalArgumentException(
                    "["
                        + CONTENT_TYPE
                        + "] field requires either ["
                        + INPUT_INDEX_FIELD
                        + "] or both ["
                        + START_OFFSET_FIELD
                        + "] and ["
                        + END_OFFSET_FIELD
                        + "]"
                );
            }
            return new OffsetSource(field, start, end);
        }
    );

    static {
        OFFSET_SOURCE_PARSER.declareString(constructorArg(), new ParseField(SOURCE_NAME_FIELD));
        OFFSET_SOURCE_PARSER.declareInt(optionalConstructorArg(), new ParseField(START_OFFSET_FIELD));
        OFFSET_SOURCE_PARSER.declareInt(optionalConstructorArg(), new ParseField(END_OFFSET_FIELD));
        OFFSET_SOURCE_PARSER.declareInt(optionalConstructorArg(), new ParseField(INPUT_INDEX_FIELD));
    }

    public enum CharsetFormat {
        UTF_16(StandardCharsets.UTF_16);

        private Charset charSet;

        CharsetFormat(Charset charSet) {
            this.charSet = charSet;
        }
    }

    public static class Builder extends FieldMapper.Builder {
        private final Parameter<CharsetFormat> charset = Parameter.enumParam(
            "charset",
            false,
            i -> CharsetFormat.UTF_16,
            CharsetFormat.UTF_16,
            CharsetFormat.class
        );
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            super(name);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { meta, charset };
        }

        @Override
        public String contentType() {
            return CONTENT_TYPE;
        }

        @Override
        public OffsetSourceFieldMapper build(MapperBuilderContext context) {
            return new OffsetSourceFieldMapper(
                leafName(),
                new OffsetSourceFieldType(context.buildFullName(leafName()), charset.get(), meta.getValue()),
                builderParams(this, context)
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    public static final class OffsetSourceFieldType extends MappedFieldType {
        private final CharsetFormat charset;

        public OffsetSourceFieldType(String name, CharsetFormat charset, Map<String, String> meta) {
            super(name, IndexType.terms(true, false), false, meta);
            this.charset = charset;
        }

        public Charset getCharset() {
            return charset.charSet;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            throw new IllegalArgumentException("[offset_source] fields do not support sorting, scripting or aggregating");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new ValueFetcher() {
                OffsetSourceField.OffsetSourceLoader offsetLoader;

                @Override
                public void setNextReader(LeafReaderContext context) {
                    try {
                        var terms = context.reader().terms(name());
                        offsetLoader = terms != null ? OffsetSourceField.loader(terms) : null;
                    } catch (IOException exc) {
                        throw new UncheckedIOException(exc);
                    }
                }

                @Override
                public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) throws IOException {
                    var offsetSource = offsetLoader != null ? offsetLoader.advanceTo(doc, context.indexVersionCreated()) : null;
                    return offsetSource != null ? List.of(offsetSource) : null;
                }

                @Override
                public StoredFieldsSpec storedFieldsSpec() {
                    return StoredFieldsSpec.NO_REQUIREMENTS;
                }
            };
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("Queries on [offset_source] fields are not supported");
        }

        @Override
        public boolean isSearchable() {
            return false;
        }
    }

    /**
     * @param simpleName      the leaf name of the mapper
     * @param mappedFieldType
     * @param params          initialization params for this field mapper
     */
    protected OffsetSourceFieldMapper(String simpleName, MappedFieldType mappedFieldType, BuilderParams params) {
        super(simpleName, mappedFieldType, params);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected boolean supportsParsingObject() {
        return true;
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        var parser = context.parser();
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            // skip
            return;
        }

        if (context.doc().getByKey(fullPath()) != null) {
            throw new IllegalArgumentException(
                "[offset_source] fields do not support indexing multiple values for the same field ["
                    + fullPath()
                    + "] in the same document"
            );
        }

        // make sure that we don't expand dots in field names while parsing
        boolean isWithinLeafObject = context.path().isWithinLeafObject();
        context.path().setWithinLeafObject(true);
        try {
            var offsetSource = OFFSET_SOURCE_PARSER.parse(parser, null);

            if (offsetSource.inputIndex() != null) {
                // Temporary logic to ensure input index is not used in release builds
                if (SemanticFieldMapper.SEMANTIC_FIELD_FEATURE_FLAG.isEnabled() == false) {
                    throw new UnsupportedOperationException("Input index is not supported yet");
                }

                if (context.indexSettings().getIndexVersionCreated().before(SEMANTIC_FIELD_TYPE)) {
                    throw new IllegalStateException(
                        "Input index cannot be set on indices with index version < "
                            + SEMANTIC_FIELD_TYPE
                            + "(detected index version: "
                            + context.indexSettings().getIndexVersionCreated()
                            + ")"
                    );
                }
            }

            assert offsetSource.hasOffsets() == (offsetSource.inputIndex() == null);
            OffsetSourceField luceneField = offsetSource.inputIndex() != null
                ? new OffsetSourceField(fullPath(), offsetSource.field(), offsetSource.inputIndex())
                : new OffsetSourceField(fullPath(), offsetSource.field(), offsetSource.start(), offsetSource.end());
            context.doc().addWithKey(fieldType().name(), luceneField);
            context.addToFieldNames(fieldType().name());
        } finally {
            context.path().setWithinLeafObject(isWithinLeafObject);
        }
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(leafName()).init(this);
    }
}
