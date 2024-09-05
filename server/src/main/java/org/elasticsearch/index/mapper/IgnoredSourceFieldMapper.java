/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**

 * Mapper for the {@code _ignored_source} field.
 *
 * A field mapper that records fields that have been ignored or otherwise need storing their source, along with their values.
 * It's intended for use in indexes with synthetic source to reconstruct the latter, taking into account fields that got ignored or
 * transformed during indexing. Entries get stored in lexicographical order by field name.
 *
 * This overlaps with {@link IgnoredFieldMapper} that tracks just the ignored field names. It's worth evaluating
 * if we can replace it for all use cases to avoid duplication, assuming that the storage tradeoff is favorable.
 */
public class IgnoredSourceFieldMapper extends MetadataFieldMapper {

    // This factor is used to combine two offsets within the same integer:
    // - the offset of the end of the parent field within the field name (N / PARENT_OFFSET_IN_NAME_OFFSET)
    // - the offset of the field value within the encoding string containing the offset (first 4 bytes), the field name and value
    // (N % PARENT_OFFSET_IN_NAME_OFFSET)
    private static final int PARENT_OFFSET_IN_NAME_OFFSET = 1 << 16;

    public static final String NAME = "_ignored_source";

    public static final IgnoredSourceFieldMapper INSTANCE = new IgnoredSourceFieldMapper();

    public static final TypeParser PARSER = new FixedTypeParser(context -> INSTANCE);

    static final NodeFeature TRACK_IGNORED_SOURCE = new NodeFeature("mapper.track_ignored_source");

    /*
     * Container for the ignored field data:
     *  - the full name
     *  - the offset in the full name indicating the end of the substring matching
     *    the full name of the parent field
     *  - the value, encoded as a byte array
     */
    public record NameValue(String name, int parentOffset, BytesRef value, LuceneDocument doc) {
        /**
         * Factory method, for use with fields under the parent object. It doesn't apply to objects at root level.
         * @param context the parser context, containing a non-null parent
         * @param name the fully-qualified field name, including the path from root
         * @param value the value to store
         */
        public static NameValue fromContext(DocumentParserContext context, String name, BytesRef value) {
            int parentOffset = context.parent() instanceof RootObjectMapper ? 0 : context.parent().fullPath().length() + 1;
            return new NameValue(name, parentOffset, value, context.doc());
        }

        String getParentFieldName() {
            // _doc corresponds to the root object
            return (parentOffset == 0) ? MapperService.SINGLE_MAPPING_NAME : name.substring(0, parentOffset - 1);
        }

        void write(XContentBuilder builder) throws IOException {
            builder.field(getFieldName());
            XContentDataHelper.decodeAndWrite(builder, value());
        }

        String getFieldName() {
            return parentOffset() == 0 ? name() : name().substring(parentOffset());
        }
    }

    static final class IgnoredValuesFieldMapperType extends StringFieldType {

        private static final IgnoredValuesFieldMapperType INSTANCE = new IgnoredValuesFieldMapperType();

        private IgnoredValuesFieldMapperType() {
            super(NAME, false, true, false, TextSearchInfo.NONE, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return NAME;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new StoredValueFetcher(context.lookup(), NAME);
        }
    }

    private IgnoredSourceFieldMapper() {
        super(IgnoredValuesFieldMapperType.INSTANCE);
    }

    @Override
    protected String contentType() {
        return NAME;
    }

    @Override
    public void postParse(DocumentParserContext context) {
        // Ignored values are only expected in synthetic mode.
        assert context.getIgnoredFieldValues().isEmpty() || context.mappingLookup().isSourceSynthetic();
        List<NameValue> ignoredFieldValues = new ArrayList<>(context.getIgnoredFieldValues());
        // ensure consistent ordering when retrieving synthetic source
        Collections.sort(ignoredFieldValues, Comparator.comparing(NameValue::name));
        for (NameValue nameValue : ignoredFieldValues) {
            nameValue.doc().add(new StoredField(NAME, encode(nameValue)));
        }
    }

    static byte[] encode(NameValue values) {
        assert values.parentOffset < PARENT_OFFSET_IN_NAME_OFFSET;
        assert values.parentOffset * (long) PARENT_OFFSET_IN_NAME_OFFSET < Integer.MAX_VALUE;

        byte[] nameBytes = values.name.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[4 + nameBytes.length + values.value.length];
        ByteUtils.writeIntLE(values.name.length() + PARENT_OFFSET_IN_NAME_OFFSET * values.parentOffset, bytes, 0);
        System.arraycopy(nameBytes, 0, bytes, 4, nameBytes.length);
        System.arraycopy(values.value.bytes, values.value.offset, bytes, 4 + nameBytes.length, values.value.length);
        return bytes;
    }

    static NameValue decode(Object field) {
        byte[] bytes = ((BytesRef) field).bytes;
        int encodedSize = ByteUtils.readIntLE(bytes, 0);
        int nameSize = encodedSize % PARENT_OFFSET_IN_NAME_OFFSET;
        int parentOffset = encodedSize / PARENT_OFFSET_IN_NAME_OFFSET;
        String name = new String(bytes, 4, nameSize, StandardCharsets.UTF_8);
        BytesRef value = new BytesRef(bytes, 4 + nameSize, bytes.length - nameSize - 4);
        return new NameValue(name, parentOffset, value, null);
    }

    public record MappedNameValue(NameValue nameValue, XContentType type, Map<String, Object> map) {}

    /**
     * Parses the passed byte array as a NameValue and converts its decoded value to a map of maps that corresponds to the field-value
     * subtree. There is only a single pair at the top level, with the key corresponding to the field name. If the field contains a single
     * value, the map contains a single key-value pair. Otherwise, the value of the first pair will be another map etc.
     * @param value encoded NameValue
     * @return MappedNameValue with the parsed NameValue, the XContentType to use for serializing its contents and the field-value map.
     * @throws IOException
     */
    public static MappedNameValue decodeAsMap(byte[] value) throws IOException {
        BytesRef bytes = new BytesRef(value);
        IgnoredSourceFieldMapper.NameValue nameValue = IgnoredSourceFieldMapper.decode(bytes);
        XContentBuilder xContentBuilder = XContentBuilder.builder(XContentDataHelper.getXContentType(nameValue.value()).xContent());
        xContentBuilder.startObject().field(nameValue.name());
        XContentDataHelper.decodeAndWrite(xContentBuilder, nameValue.value());
        xContentBuilder.endObject();
        Tuple<XContentType, Map<String, Object>> result = XContentHelper.convertToMap(BytesReference.bytes(xContentBuilder), true);
        return new MappedNameValue(nameValue, result.v1(), result.v2());
    }

    /**
     * Clones the passed NameValue, using the passed map to produce its value.
     * @param mappedNameValue containing the NameValue to clone
     * @param map containing a simple field-value pair, or a deeper field-value subtree for objects and arrays with fields
     * @return a byte array containing the encoding form of the cloned NameValue
     * @throws IOException
     */
    public static byte[] encodeFromMap(MappedNameValue mappedNameValue, Map<String, Object> map) throws IOException {
        // The first entry is the field name, we skip to get to the value to encode.
        assert map.size() == 1;
        Object content = map.values().iterator().next();

        // Check if the field contains a single value or an object.
        @SuppressWarnings("unchecked")
        XContentBuilder xContentBuilder = (content instanceof Map<?, ?> objectMap)
            ? XContentBuilder.builder(mappedNameValue.type().xContent()).map((Map<String, ?>) objectMap)
            : XContentBuilder.builder(mappedNameValue.type().xContent()).value(content);

        // Clone the NameValue with the updated value.
        NameValue oldNameValue = mappedNameValue.nameValue();
        IgnoredSourceFieldMapper.NameValue filteredNameValue = new IgnoredSourceFieldMapper.NameValue(
            oldNameValue.name(),
            oldNameValue.parentOffset(),
            XContentDataHelper.encodeXContentBuilder(xContentBuilder),
            oldNameValue.doc()
        );
        return IgnoredSourceFieldMapper.encode(filteredNameValue);
    }

    // This mapper doesn't contribute to source directly as it has no access to the object structure. Instead, its contents
    // are loaded by SourceLoader and passed to object mappers that, in turn, write their ignore fields at the appropriate level.
    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        return SourceLoader.SyntheticFieldLoader.NOTHING;
    }

}
