/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Mapper for the {@code _ignored_values} field.
 *
 * A field mapper that records fields that have been ignored, along with their values. It's intended for use
 * in indexes with synthetic source to reconstruct the latter, taking into account fields that got ignored during
 * indexing.
 *
 * This overlaps with {@link IgnoredFieldMapper} that tracks just the ignored field names. It's worth evaluating
 * if we can replace it for all use cases to avoid duplication, assuming that the storage tradeoff is favorable.
 */
public class IgnoredValuesFieldMapper extends MetadataFieldMapper {

    // This factor is used to combine two offsets within the same integer:
    // - the offset of the end of the parent field within the field name (N / PARENT_OFFSET_IN_NAME_OFFSET)
    // - the offset of the field value within the encoding string containing the offset (first 4 bytes), the field name and value
    // (N % PARENT_OFFSET_IN_NAME_OFFSET)
    private static final int PARENT_OFFSET_IN_NAME_OFFSET = 1 << 16;

    public static final String NAME = "_ignored_values";

    public static final IgnoredValuesFieldMapper INSTANCE = new IgnoredValuesFieldMapper();

    public static final TypeParser PARSER = new FixedTypeParser(context -> INSTANCE);

    static final NodeFeature TRACK_IGNORED_VALUES = new NodeFeature("mapper.track_ignored_values");

    /*
     * Container for the ignored field data:
     *  - the full name
     *  - the offset in the full name indicating the end of the substring matching
     *    the full name of the parent field
     *  - the value, encoded as a byte array
     */
    public record Values(String name, int parentOffset, BytesRef value) {}

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

    private IgnoredValuesFieldMapper() {
        super(IgnoredValuesFieldMapperType.INSTANCE);
    }

    @Override
    protected String contentType() {
        return NAME;
    }

    @Override
    public void postParse(DocumentParserContext context) {
        // Ignored values are only expected in synthetic mode.
        assert context.getIgnoredFieldValues().isEmpty()
            || context.indexSettings().getMode().isSyntheticSourceEnabled()
            || context.mappingLookup().isSourceSynthetic();
        for (Values values : context.getIgnoredFieldValues()) {
            context.doc().add(new StoredField(NAME, encode(values)));
        }
    }

    static byte[] encode(Values values) {
        assert values.parentOffset < PARENT_OFFSET_IN_NAME_OFFSET;
        assert values.parentOffset * (long) PARENT_OFFSET_IN_NAME_OFFSET < Integer.MAX_VALUE;

        byte[] nameBytes = values.name.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[4 + nameBytes.length + values.value.length];
        ByteUtils.writeIntLE(values.name.length() + PARENT_OFFSET_IN_NAME_OFFSET * values.parentOffset, bytes, 0);
        System.arraycopy(nameBytes, 0, bytes, 4, nameBytes.length);
        System.arraycopy(values.value.bytes, values.value.offset, bytes, 4 + nameBytes.length, values.value.length);
        return bytes;
    }

    static Values decode(Object field) {
        byte[] bytes = ((BytesRef) field).bytes;
        int encodedSize = ByteUtils.readIntLE(bytes, 0);
        int nameSize = encodedSize % PARENT_OFFSET_IN_NAME_OFFSET;
        int parentOffset = encodedSize / PARENT_OFFSET_IN_NAME_OFFSET;
        String name = new String(bytes, 4, nameSize, StandardCharsets.UTF_8);
        BytesRef value = new BytesRef(bytes, 4 + nameSize, bytes.length - nameSize - 4);
        return new Values(name, parentOffset, value);
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        return new SyntheticLoader();
    }

    public static class SyntheticLoader implements SourceLoader.SyntheticFieldLoader {
        // Contains stored field values, i.e. encoded ignored field names and values.
        private List<Object> values = null;

        // Maps the names of existing objects to lists of ignored fields they contain.
        private Map<String, List<Values>> objectsWithIgnoredFields = Map.of();

        @Override
        public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
            return Stream.of(Map.entry(NAME, values -> this.values = values));
        }

        @Override
        public SourceLoader.SyntheticFieldLoader.DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf)
            throws IOException {
            return null;
        }

        @Override
        public boolean hasValue() {
            return false;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            // This mapper doesn't contribute to source directly as it has no access to the
            // object structure. Instead, it's accessed by object mappers to decode
            // and write its fields within the appropriate object.
            assert false : "IgnoredValuesFieldMapper::write should never be called";
        }

        public void trackObjectsWithIgnoredFields() {
            if (values == null) {
                return;
            }
            objectsWithIgnoredFields = new HashMap<>();
            for (Object value : values) {
                Values parsedValues = decode(value);
                objectsWithIgnoredFields.computeIfAbsent(
                    // _doc corresponds to the root object
                    (parsedValues.parentOffset == 0) ? "_doc" : parsedValues.name.substring(0, parsedValues.parentOffset - 1),
                    k -> new ArrayList<>()
                ).add(parsedValues);
            }
            values = null;
        }

        // This is expected to be called by object mappers, to add their ignored fields as part of the source.
        public void write(XContentBuilder b, String prefix) throws IOException {
            var matches = objectsWithIgnoredFields.get(prefix);
            if (matches != null) {
                for (var values : matches) {
                    b.field(values.parentOffset > 0 ? values.name.substring(values.parentOffset) : values.name);
                    FieldDataParseHelper.decodeAndWrite(b, values.value);
                }
            }
        }

        public boolean containsIgnoredFields(String prefix) {
            return objectsWithIgnoredFields.containsKey(prefix);
        }
    };
}
