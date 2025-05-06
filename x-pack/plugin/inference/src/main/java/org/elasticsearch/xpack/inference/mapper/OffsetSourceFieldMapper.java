/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.TextSearchInfo;
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

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

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

    public record OffsetSource(String field, int start, int end) implements ToXContentObject {
        public OffsetSource {
            if (start < 0 || end < 0) {
                throw new IllegalArgumentException("Illegal offsets, expected positive numbers, got: " + start + ":" + end);
            }
            if (start > end) {
                throw new IllegalArgumentException("Illegal offsets, expected start < end, got: " + start + " > " + end);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(SOURCE_NAME_FIELD, field);
            builder.field(START_OFFSET_FIELD, start);
            builder.field(END_OFFSET_FIELD, end);
            return builder.endObject();
        }
    }

    private static final ConstructingObjectParser<OffsetSource, Void> OFFSET_SOURCE_PARSER = new ConstructingObjectParser<>(
        CONTENT_TYPE,
        true,
        args -> new OffsetSource((String) args[0], (int) args[1], (int) args[2])
    );

    static {
        OFFSET_SOURCE_PARSER.declareString(constructorArg(), new ParseField(SOURCE_NAME_FIELD));
        OFFSET_SOURCE_PARSER.declareInt(constructorArg(), new ParseField(START_OFFSET_FIELD));
        OFFSET_SOURCE_PARSER.declareInt(constructorArg(), new ParseField(END_OFFSET_FIELD));
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
        public OffsetSourceFieldMapper build(MapperBuilderContext context) {
            OffsetSourceFieldType fieldType = new OffsetSourceFieldType(context.buildFullName(leafName()), charset.get(), meta.getValue());
            // explicitly setting the field type to be not available for field caps
            fieldType.setExcludeFromFieldCaps(true);

            return new OffsetSourceFieldMapper(leafName(), fieldType, builderParams(this, context));
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    public static final class OffsetSourceFieldType extends MappedFieldType {
        private final CharsetFormat charset;

        public OffsetSourceFieldType(String name, CharsetFormat charset, Map<String, String> meta) {
            super(name, true, false, false, TextSearchInfo.NONE, meta);
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
        public boolean fieldHasValue(FieldInfos fieldInfos) {
            return fieldInfos.fieldInfo(name()) != null;
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
                    var offsetSource = offsetLoader != null ? offsetLoader.advanceTo(doc) : null;
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
            context.doc()
                .addWithKey(
                    fieldType().name(),
                    new OffsetSourceField(fullPath(), offsetSource.field, offsetSource.start, offsetSource.end)
                );
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
