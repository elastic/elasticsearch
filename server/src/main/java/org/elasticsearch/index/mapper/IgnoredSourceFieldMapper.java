/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fielddata.MultiValuedSortedBinaryDocValues;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

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
    private final IndexSettings indexSettings;

    // This factor is used to combine two offsets within the same integer:
    // - the offset of the end of the parent field within the field name (N / PARENT_OFFSET_IN_NAME_OFFSET)
    // - the offset of the field value within the encoding string containing the offset (first 4 bytes), the field name and value
    // (N % PARENT_OFFSET_IN_NAME_OFFSET)
    private static final int PARENT_OFFSET_IN_NAME_OFFSET = 1 << 16;

    public static final String NAME = "_ignored_source";

    public static final TypeParser PARSER = new FixedTypeParser(context -> new IgnoredSourceFieldMapper(context.getIndexSettings()));

    static final NodeFeature DONT_EXPAND_DOTS_IN_IGNORED_SOURCE = new NodeFeature("mapper.ignored_source.dont_expand_dots");
    static final NodeFeature IGNORED_SOURCE_AS_TOP_LEVEL_METADATA_ARRAY_FIELD = new NodeFeature(
        "mapper.ignored_source_as_top_level_metadata_array_field"
    );
    static final NodeFeature ALWAYS_STORE_OBJECT_ARRAYS_IN_NESTED_OBJECTS = new NodeFeature(
        "mapper.ignored_source.always_store_object_arrays_in_nested"
    );

    public static final FeatureFlag COALESCE_IGNORED_SOURCE_ENTRIES = new FeatureFlag("ignored_source_fields_per_entry");
    public static final FeatureFlag IGNORED_SOURCE_AS_DOC_VALUES_FF = new FeatureFlag("ignored_source_as_doc_values");

    /*
        Setting to disable encoding and writing values for this field.
        This is needed to unblock index functionality in case there is a bug on this code path.
     */
    public static final Setting<Boolean> SKIP_IGNORED_SOURCE_WRITE_SETTING = Setting.boolSetting(
        "index.mapping.synthetic_source.skip_ignored_source_write",
        false,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    /*
        Setting to disable reading and decoding values stored in this field.
        This is needed to unblock search functionality in case there is a bug on this code path.
     */
    public static final Setting<Boolean> SKIP_IGNORED_SOURCE_READ_SETTING = Setting.boolSetting(
        "index.mapping.synthetic_source.skip_ignored_source_read",
        false,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

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

        String getFieldName() {
            return parentOffset() == 0 ? name() : name().substring(parentOffset());
        }

        NameValue cloneWithValue(BytesRef value) {
            assert value() == null;
            return new NameValue(name, parentOffset, value, doc);
        }

        boolean hasValue() {
            return XContentDataHelper.isDataPresent(value);
        }
    }

    static final class IgnoredValuesFieldMapperType extends StringFieldType {

        private final IgnoredSourceFormat format;

        private IgnoredValuesFieldMapperType(IgnoredSourceFormat format) {
            super(NAME, IndexType.NONE, true, TextSearchInfo.NONE, Collections.emptyMap());
            this.format = format;
        }

        @Override
        public String typeName() {
            return NAME;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (this.format == IgnoredSourceFormat.DOC_VALUES_IGNORED_SOURCE) {
                return new DocValuesIgnoredSourceValueFetcher();
            }
            return new StoredValueFetcher(context.lookup(), NAME);
        }
    }

    /**
     * Reads individual ignored-source entries from binary doc values (IntegratedCount format).
     */
    private static final class DocValuesIgnoredSourceValueFetcher implements ValueFetcher {
        private MultiValuedSortedBinaryDocValues docValues;

        @Override
        public void setNextReader(LeafReaderContext context) {
            try {
                docValues = MultiValuedSortedBinaryDocValues.from(context.reader(), NAME);
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to load doc values for " + NAME, e);
            }
        }

        @Override
        public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) throws IOException {
            if (docValues == null || docValues.advanceExact(doc) == false) {
                return List.of();
            }
            int count = docValues.docValueCount();
            List<Object> result = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                result.add(BytesRef.deepCopyOf(docValues.nextValue()));
            }
            return result;
        }

        @Override
        public StoredFieldsSpec storedFieldsSpec() {
            return StoredFieldsSpec.NO_REQUIREMENTS;
        }
    }

    private IgnoredSourceFieldMapper(IndexSettings indexSettings) {
        super(new IgnoredValuesFieldMapperType(ignoredSourceFormat(indexSettings)));
        this.indexSettings = indexSettings;
    }

    @Override
    protected String contentType() {
        return NAME;
    }

    @Override
    public void postParse(DocumentParserContext context) {
        // Ignored values are only expected in synthetic mode.
        if (context.mappingLookup().isSourceSynthetic() == false) {
            assert context.getIgnoredFieldValues().isEmpty();
            return;
        }

        ignoredSourceFormat(context.indexSettings()).writeIgnoredFields(context.getIgnoredFieldValues());
    }

    // In rare cases decoding values stored in this field can fail leading to entire source
    // not being available.
    // We would like to have an option to lose some values in synthetic source
    // but have search not fail.
    public static Set<String> ensureLoaded(Set<String> fieldsToLoadForSyntheticSource, IndexSettings indexSettings) {
        if (indexSettings.getSkipIgnoredSourceRead() == false
            && ignoredSourceFormat(indexSettings) != IgnoredSourceFormat.DOC_VALUES_IGNORED_SOURCE) {
            fieldsToLoadForSyntheticSource.add(NAME);
        }

        return fieldsToLoadForSyntheticSource;
    }

    /**
     * Encodes/decodes a single ignored field/value pair to/from a {@link BytesRef} blob.
     * <p>
     * The blob itself is of the following format: {@code [header][field name][value]} where:
     * <ul>
     *     <li>{@code header} is a little-endian {@code int32} that packs {@code field-name.length} and the parent offset</li>
     *     <li>{@code field name} is the full field path, as UTF-8 bytes</li>
     *     <li>{@code value} is the ignored value encoded by {@link XContentDataHelper}</li>
     * </ul>
     */
    public static class SingularIgnoredSourceEncoding {

        public static BytesRef encode(NameValue values) {
            assert values.parentOffset < PARENT_OFFSET_IN_NAME_OFFSET;
            assert values.parentOffset * (long) PARENT_OFFSET_IN_NAME_OFFSET < Integer.MAX_VALUE;

            byte[] nameBytes = values.name.getBytes(StandardCharsets.UTF_8);
            byte[] bytes = new byte[4 + nameBytes.length + values.value.length];
            ByteUtils.writeIntLE(values.name.length() + PARENT_OFFSET_IN_NAME_OFFSET * values.parentOffset, bytes, 0);
            System.arraycopy(nameBytes, 0, bytes, 4, nameBytes.length);
            System.arraycopy(values.value.bytes, values.value.offset, bytes, 4 + nameBytes.length, values.value.length);
            return new BytesRef(bytes);
        }

        public static NameValue decode(Object field) {
            BytesRef ref = (BytesRef) field;
            byte[] bytes = ref.bytes;
            int off = ref.offset;
            int len = ref.length;

            int encodedSize = ByteUtils.readIntLE(bytes, off);
            int nameSize = encodedSize % PARENT_OFFSET_IN_NAME_OFFSET;
            int parentOffset = encodedSize / PARENT_OFFSET_IN_NAME_OFFSET;

            String decoded = new String(bytes, off + 4, len - 4, StandardCharsets.UTF_8);
            String name = decoded.substring(0, nameSize);
            int nameByteCount = name.getBytes(StandardCharsets.UTF_8).length;

            BytesRef value = new BytesRef(bytes, off + 4 + nameByteCount, len - nameByteCount - 4);
            return new NameValue(name, parentOffset, value, null);
        }

        public static BytesRef encodeFromMap(MappedNameValue mappedNameValue) throws IOException {
            return encode(mappedToNameValue(mappedNameValue));
        }

        public static MappedNameValue decodeAsMap(BytesRef value) throws IOException {
            return nameValueToMapped(decode(value));
        }
    }

    /**
     * Encodes/decodes multiple ignored values for the same field path to/from a {@link BytesRef} blob.
     * <p>
     * Instead of storing one {@link SingularIgnoredSourceEncoding} blob per ignored value, all ignored values for a single field path are
     * stored together and the field name is written only once.
     * <p>
     * The blob itself is of the following format: {@code [count][field name][(parent offset, value)*]} where:
     * <ul>
     *     <li>{@code count} is a {@link StreamInput#readVInt() vInt} number of values</li>
     *     <li>{@code field name} is the full field path, written once as a {@link StreamInput#readString() string}</li>
     *     <li>each {@code (parent offset, value)} pair contains:
     *     <ul>
     *         <li>{@code parent offset} as a {@link StreamInput#readVInt() vInt} offset into {@code field name} where the parent ends</li>
     *         <li>{@code value} as a {@link StreamInput#readBytesRef() bytesRef} encoded by {@link XContentDataHelper}</li>
     *     </ul>
     *     </li>
     * </ul>
     */
    public static class CoalescedIgnoredSourceEncoding {

        public static BytesRef encode(List<NameValue> values) {
            assert values.isEmpty() == false;
            try {
                BytesStreamOutput stream = new BytesStreamOutput();
                stream.writeVInt(values.size());
                String fieldName = values.getFirst().name;
                stream.writeString(fieldName);
                for (var value : values) {
                    assert fieldName.equals(value.name);
                    stream.writeVInt(value.parentOffset);
                    stream.writeBytesRef(value.value);
                }
                return stream.bytes().toBytesRef();
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to encode _ignored_source", e);
            }
        }

        public static List<NameValue> decode(BytesRef value) {
            try {
                StreamInput stream = new BytesArray(value).streamInput();
                var count = stream.readVInt();
                assert count >= 1;
                String fieldName = stream.readString();
                List<NameValue> values = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    int parentOffset = stream.readVInt();
                    BytesRef valueBytes = stream.readBytesRef();
                    values.add(new NameValue(fieldName, parentOffset, valueBytes, null));
                }
                return values;
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to decode _ignored_source", e);
            }
        }

        public static BytesRef encodeFromMap(List<MappedNameValue> filteredValues) throws IOException {
            List<NameValue> filteredNameValues = new ArrayList<>(filteredValues.size());
            for (var filteredValue : filteredValues) {
                filteredNameValues.add(mappedToNameValue(filteredValue));
            }
            return encode(filteredNameValues);
        }

        public static List<MappedNameValue> decodeAsMap(BytesRef value) throws IOException {
            List<NameValue> nameValues = decode(value);
            List<MappedNameValue> mappedValues = new ArrayList<>(nameValues.size());
            for (var nameValue : nameValues) {
                MappedNameValue mappedNameValue = nameValueToMapped(nameValue);
                if (mappedNameValue != null) {
                    mappedValues.add(mappedNameValue);
                }
            }
            return mappedValues;
        }
    }

    /**
     * Applies a field-level security filter to a single ignored source value.
     * Shared by {@link IgnoredSourceFormat#LEGACY_SINGLE_IGNORED_SOURCE} and {@link IgnoredSourceFormat#DOC_VALUES_IGNORED_SOURCE}.
     */
    static BytesRef filterLegacyValue(BytesRef value, Function<Map<String, Object>, Map<String, Object>> filter) throws IOException {
        // for _ignored_source, parse, filter out the field and its contents, and serialize back downstream
        MappedNameValue mappedNameValue = SingularIgnoredSourceEncoding.decodeAsMap(value);
        if (mappedNameValue == null) {
            return null;
        }
        Map<String, Object> transformedField = filter.apply(mappedNameValue.map());
        if (transformedField.isEmpty()) {
            // All values were filtered
            return null;
        }
        // The unfiltered map contains at least one element, the field name with its value. If the field contains
        // an object or an array, the value of the first element is a map or a list, respectively. Otherwise,
        // it's a single leaf value, e.g. a string or a number.
        var topValue = mappedNameValue.map().values().iterator().next();
        if (topValue instanceof Map<?, ?> || topValue instanceof List<?>) {
            // The field contains an object or an array, reconstruct it from the transformed map in case
            // any subfield has been filtered out.
            return SingularIgnoredSourceEncoding.encodeFromMap(mappedNameValue.withMap(transformedField));
        } else {
            // The field contains a leaf value, and it hasn't been filtered out. It is safe to propagate the original value.
            return value;
        }
    }

    public enum IgnoredSourceFormat {
        NO_IGNORED_SOURCE {
            @Override
            public Map<String, List<NameValue>> loadIgnoredFields(
                SourceFilter filter,
                Map<String, List<Object>> storedFields,
                int docId,
                MultiValuedSortedBinaryDocValues docValues
            ) {
                return Map.of();
            }

            @Override
            public void writeIgnoredFields(Collection<NameValue> ignoredFieldValues) {
                assert false : "cannot write " + ignoredFieldValues.size() + " values with format NO_IGNORED_SOURCE";
            }

            @Override
            public BytesRef filterValue(BytesRef value, Function<Map<String, Object>, Map<String, Object>> filter) {
                assert false : "cannot filter ignored source with format NO_IGNORED_SOURCE";
                return null;
            }
        },
        LEGACY_SINGLE_IGNORED_SOURCE {
            @Override
            public Map<String, List<NameValue>> loadIgnoredFields(
                SourceFilter filter,
                Map<String, List<Object>> storedFields,
                int docId,
                MultiValuedSortedBinaryDocValues docValues
            ) {
                var ignoredStoredValues = storedFields.get(NAME);
                if (ignoredStoredValues == null) {
                    return Map.of();
                }
                Map<String, List<NameValue>> objectsWithIgnoredFields = new HashMap<>();
                for (Object value : ignoredStoredValues) {
                    NameValue nv = SingularIgnoredSourceEncoding.decode(value);
                    if (filter != null && filter.isPathFiltered(nv.name(), XContentDataHelper.isEncodedObject(nv.value()))) {
                        continue;
                    }
                    objectsWithIgnoredFields.computeIfAbsent(nv.getParentFieldName(), k -> new ArrayList<>()).add(nv);
                }
                return objectsWithIgnoredFields;
            }

            @Override
            public void writeIgnoredFields(Collection<NameValue> ignoredFieldValues) {
                for (NameValue nameValue : ignoredFieldValues) {
                    nameValue.doc().add(new StoredField(NAME, SingularIgnoredSourceEncoding.encode(nameValue)));
                }
            }

            @Override
            public BytesRef filterValue(BytesRef value, Function<Map<String, Object>, Map<String, Object>> filter) throws IOException {
                return filterLegacyValue(value, filter);
            }
        },
        COALESCED_SINGLE_IGNORED_SOURCE {
            @Override
            public Map<String, List<NameValue>> loadIgnoredFields(
                SourceFilter filter,
                Map<String, List<Object>> storedFields,
                int docId,
                MultiValuedSortedBinaryDocValues docValues
            ) {
                var ignoredStoredValues = storedFields.get(NAME);
                if (ignoredStoredValues == null) {
                    return Map.of();
                }
                Map<String, List<NameValue>> objectsWithIgnoredFields = new HashMap<>();
                for (var ignoredSourceEntry : ignoredStoredValues) {
                    @SuppressWarnings("unchecked")
                    List<NameValue> nameValues = (ignoredSourceEntry instanceof List<?>)
                        ? (List<NameValue>) ignoredSourceEntry
                        : CoalescedIgnoredSourceEncoding.decode((BytesRef) ignoredSourceEntry);
                    assert nameValues.isEmpty() == false;
                    for (var nameValue : nameValues) {
                        if (filter != null
                            && filter.isPathFiltered(nameValue.name(), XContentDataHelper.isEncodedObject(nameValue.value()))) {
                            continue;
                        }
                        objectsWithIgnoredFields.computeIfAbsent(nameValue.getParentFieldName(), k -> new ArrayList<>()).add(nameValue);
                    }
                }
                return objectsWithIgnoredFields;
            }

            @Override
            public void writeIgnoredFields(Collection<NameValue> ignoredFieldValues) {
                Map<LuceneDocument, Map<String, List<NameValue>>> entriesMap = new HashMap<>();

                for (NameValue nameValue : ignoredFieldValues) {
                    entriesMap.computeIfAbsent(nameValue.doc(), d -> new HashMap<>())
                        .computeIfAbsent(nameValue.name(), n -> new ArrayList<>())
                        .add(nameValue);
                }

                for (var docEntry : entriesMap.entrySet()) {
                    for (var fieldEntry : docEntry.getValue().entrySet()) {
                        docEntry.getKey().add(new StoredField(NAME, CoalescedIgnoredSourceEncoding.encode(fieldEntry.getValue())));
                    }
                }
            }

            @Override
            public BytesRef filterValue(BytesRef value, Function<Map<String, Object>, Map<String, Object>> filter) throws IOException {
                List<MappedNameValue> mappedNameValues = CoalescedIgnoredSourceEncoding.decodeAsMap(value);
                List<MappedNameValue> filteredNameValues = new ArrayList<>(mappedNameValues.size());
                boolean maybeDidFilter = false;
                for (var mappedNameValue : mappedNameValues) {
                    Map<String, Object> transformedField = filter.apply(mappedNameValue.map());
                    if (transformedField.isEmpty()) {
                        maybeDidFilter = true;
                        continue;
                    }
                    var topValue = mappedNameValue.map().values().iterator().next();
                    if (topValue instanceof Map<?, ?> || topValue instanceof List<?>) {
                        // The field contains an object or an array in which some subfield may have been filtered out
                        maybeDidFilter = true;
                    }
                    filteredNameValues.add(mappedNameValue.withMap(transformedField));
                }
                if (maybeDidFilter) {
                    if (filteredNameValues.isEmpty()) {
                        // All values were filtered
                        return null;
                    } else {
                        return CoalescedIgnoredSourceEncoding.encodeFromMap(filteredNameValues);
                    }
                } else {
                    // The field contains a leaf value, and it hasn't been filtered out. It is safe to propagate the original value.
                    return value;
                }
            }
        },
        DOC_VALUES_IGNORED_SOURCE {
            @Override
            public Map<String, List<NameValue>> loadIgnoredFields(
                SourceFilter filter,
                Map<String, List<Object>> storedFields,
                int docId,
                MultiValuedSortedBinaryDocValues docValues
            ) throws IOException {
                if (docValues.advanceExact(docId) == false) {
                    return Map.of();
                }
                Map<String, List<NameValue>> objectsWithIgnoredFields = new HashMap<>();
                int count = docValues.docValueCount();
                for (int i = 0; i < count; i++) {
                    NameValue nv = SingularIgnoredSourceEncoding.decode(docValues.nextValue());
                    if (filter != null && filter.isPathFiltered(nv.name(), XContentDataHelper.isEncodedObject(nv.value()))) {
                        continue;
                    }
                    objectsWithIgnoredFields.computeIfAbsent(nv.getParentFieldName(), k -> new ArrayList<>()).add(nv);
                }
                return objectsWithIgnoredFields;
            }

            @Override
            public void writeIgnoredFields(Collection<NameValue> ignoredFieldValues) {
                for (NameValue nameValue : ignoredFieldValues) {
                    MultiValuedBinaryDocValuesField field = (MultiValuedBinaryDocValuesField) nameValue.doc().getByKey(NAME);
                    if (field == null) {
                        field = new MultiValuedBinaryDocValuesField.IntegratedCount(NAME, true, false);
                        nameValue.doc().addWithKey(NAME, field);
                    }
                    field.add(SingularIgnoredSourceEncoding.encode(nameValue));
                }
            }

            @Override
            public BytesRef filterValue(BytesRef value, Function<Map<String, Object>, Map<String, Object>> filter) throws IOException {
                return filterLegacyValue(value, filter);
            }
        };

        /**
         * Loads ignored-source entries for the given document, grouped by parent field name.
         *
         * @param filter       optional path filter; filtered paths are excluded from the result
         * @param storedFields pre-loaded stored fields for the document (used by stored-field-based formats)
         * @param docId        the Lucene doc ID within the current leaf
         * @param docValues    doc values instance for accessing ignored source (used by doc-values-based formats)
         * @return map from parent field name to list of {@link NameValue}, or an empty map when no entries exist
         */
        public abstract Map<String, List<NameValue>> loadIgnoredFields(
            SourceFilter filter,
            Map<String, List<Object>> storedFields,
            int docId,
            MultiValuedSortedBinaryDocValues docValues
        ) throws IOException;

        public abstract void writeIgnoredFields(Collection<NameValue> ignoredFieldValues);

        public abstract BytesRef filterValue(BytesRef value, Function<Map<String, Object>, Map<String, Object>> filter) throws IOException;
    }

    public IgnoredSourceFormat ignoredSourceFormat() {
        if (indexSettings.getSkipIgnoredSourceRead()) {
            return IgnoredSourceFormat.NO_IGNORED_SOURCE;
        }
        return ignoredSourceFormat(indexSettings);
    }

    public static IgnoredSourceFormat ignoredSourceFormat(IndexSettings indexSettings) {
        IndexVersion indexCreatedVersion = indexSettings.getIndexVersionCreated();
        // we need TSDB doc values format to use binary doc values for ignored source, otherwise the source will be uncompressed
        if (IGNORED_SOURCE_AS_DOC_VALUES_FF.isEnabled()
            && indexCreatedVersion.onOrAfter(IndexVersions.IGNORED_SOURCE_AS_DOC_VALUES)
            && indexSettings.useTimeSeriesDocValuesFormat()) {
            return IgnoredSourceFormat.DOC_VALUES_IGNORED_SOURCE;
        }

        IndexVersion switchToNewFormatVersion = COALESCE_IGNORED_SOURCE_ENTRIES.isEnabled()
            ? IndexVersions.IGNORED_SOURCE_COALESCED_ENTRIES_WITH_FF
            : IndexVersions.IGNORED_SOURCE_COALESCED_ENTRIES;

        return indexCreatedVersion.onOrAfter(switchToNewFormatVersion)
            ? IgnoredSourceFormat.COALESCED_SINGLE_IGNORED_SOURCE
            : IgnoredSourceFormat.LEGACY_SINGLE_IGNORED_SOURCE;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        // This loader controls if this field is loaded in scope of synthetic source constructions.
        // In rare cases decoding values stored in this field can fail leading to entire source
        // not being available.
        // We would like to have an option to lose some values in synthetic source
        // but have search not fail.
        return new SyntheticSourceSupport.Native(() -> new SourceLoader.SyntheticFieldLoader() {
            @Override
            public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
                var format = ignoredSourceFormat();
                if (format == IgnoredSourceFormat.NO_IGNORED_SOURCE || format == IgnoredSourceFormat.DOC_VALUES_IGNORED_SOURCE) {
                    // values are loaded via doc values and not stored fields
                    return Stream.empty();
                }

                // Values are handled in `SourceLoader`.
                return Stream.of(Map.entry(NAME, (v) -> {}));
            }

            @Override
            public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
                return null;
            }

            @Override
            public boolean hasValue() {
                return false;
            }

            @Override
            public void write(XContentBuilder b) throws IOException {

            }

            @Override
            public String fieldName() {
                // Does not really matter.
                return NAME;
            }

            @Override
            public void reset() {

            }
        });
    }

    /**
     * A parsed NameValue alongside its value decoded to a map of maps that corresponds to the field-value subtree. There is only a single
     * pair at the top level, with the key corresponding to the field name. If the field contains a single value, the map contains a single
     * key-value pair. Otherwise, the value of the first pair will be another map etc.
     */
    public record MappedNameValue(NameValue nameValue, XContentType type, Map<String, Object> map) {
        public MappedNameValue withMap(Map<String, Object> map) {
            return new MappedNameValue(new NameValue(nameValue.name, nameValue.parentOffset, null, nameValue.doc), type, map);
        }
    }

    private static MappedNameValue nameValueToMapped(NameValue nameValue) throws IOException {
        if (nameValue.hasValue() == false) {
            return null;
        }
        XContentBuilder xContentBuilder = XContentBuilder.builder(XContentDataHelper.getXContentType(nameValue.value()).xContent());
        xContentBuilder.startObject().field(nameValue.name());
        XContentDataHelper.decodeAndWrite(xContentBuilder, nameValue.value());
        xContentBuilder.endObject();
        Tuple<XContentType, Map<String, Object>> result = XContentHelper.convertToMap(BytesReference.bytes(xContentBuilder), true);
        return new MappedNameValue(nameValue, result.v1(), result.v2());
    }

    private static NameValue mappedToNameValue(MappedNameValue mappedNameValue) throws IOException {
        // The first entry is the field name, we skip to get to the value to encode.
        assert mappedNameValue.map.size() == 1;
        Object content = mappedNameValue.map.values().iterator().next();

        // Check if the field contains a single value or an object.
        @SuppressWarnings("unchecked")
        XContentBuilder xContentBuilder = (content instanceof Map<?, ?> objectMap)
            ? XContentBuilder.builder(mappedNameValue.type().xContent()).map((Map<String, ?>) objectMap)
            : XContentBuilder.builder(mappedNameValue.type().xContent()).value(content);

        // Clone the NameValue with the updated value.
        NameValue oldNameValue = mappedNameValue.nameValue();
        return new NameValue(
            oldNameValue.name(),
            oldNameValue.parentOffset(),
            XContentDataHelper.encodeXContentBuilder(xContentBuilder),
            oldNameValue.doc()
        );
    }

}
