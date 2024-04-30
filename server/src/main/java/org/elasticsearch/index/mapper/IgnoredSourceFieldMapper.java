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
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

/**

 * Mapper for the {@code _ignored_source} field.
 *
 * A field mapper that records fields that have been ignored, along with their values. It's intended for use
 * in indexes with synthetic source to reconstruct the latter, taking into account fields that got ignored during
 * indexing.
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
    public record NameValue(String name, int parentOffset, BytesRef value) {
        String getParentFieldName() {
            // _doc corresponds to the root object
            return (parentOffset == 0) ? MapperService.SINGLE_MAPPING_NAME : name.substring(0, parentOffset - 1);
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
        for (NameValue nameValue : context.getIgnoredFieldValues()) {
            context.doc().add(new StoredField(NAME, encode(nameValue)));
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
        return new NameValue(name, parentOffset, value);
    }

    // This mapper doesn't contribute to source directly as it has no access to the object structure. Instead, its contents
    // are loaded by SourceLoader and passed to object mappers that, in turn, write their ignore fields at the appropriate level.
    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        return SourceLoader.SyntheticFieldLoader.NOTHING;
    }

}
