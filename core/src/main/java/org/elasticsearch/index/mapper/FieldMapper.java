/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.Version;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper.FieldNamesFieldType;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.index.similarity.SimilarityService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.StreamSupport;

public abstract class FieldMapper extends Mapper implements Cloneable {
    public static final Setting<Boolean> IGNORE_MALFORMED_SETTING =
        Setting.boolSetting("index.mapping.ignore_malformed", false, Property.IndexScope);
    public static final Setting<Boolean> COERCE_SETTING =
        Setting.boolSetting("index.mapping.coerce", false, Property.IndexScope);
    public abstract static class Builder<T extends Builder, Y extends FieldMapper> extends Mapper.Builder<T, Y> {

        protected final MappedFieldType fieldType;
        protected final MappedFieldType defaultFieldType;
        private final IndexOptions defaultOptions;
        protected boolean omitNormsSet = false;
        protected Boolean includeInAll;
        protected boolean indexOptionsSet = false;
        protected boolean docValuesSet = false;
        protected final MultiFields.Builder multiFieldsBuilder;
        protected CopyTo copyTo = CopyTo.empty();

        protected Builder(String name, MappedFieldType fieldType, MappedFieldType defaultFieldType) {
            super(name);
            this.fieldType = fieldType.clone();
            this.defaultFieldType = defaultFieldType.clone();
            this.defaultOptions = fieldType.indexOptions(); // we have to store it the fieldType is mutable
            this.docValuesSet = fieldType.hasDocValues();
            multiFieldsBuilder = new MultiFields.Builder();
        }

        public MappedFieldType fieldType() {
            return fieldType;
        }

        public T index(boolean index) {
            if (index) {
                if (fieldType.indexOptions() == IndexOptions.NONE) {
                    /*
                     * the logic here is to reset to the default options only if we are not indexed ie. options are null
                     * if the fieldType has a non-null option we are all good it might have been set through a different
                     * call.
                     */
                    IndexOptions options = getDefaultIndexOption();
                    if (options == IndexOptions.NONE) {
                        // can happen when an existing type on the same index has disabled indexing
                        // since we inherit the default field type from the first mapper that is
                        // created on an index
                        throw new IllegalArgumentException("mapper [" + name + "] has different [index] values from other types of the same index");
                    }
                    fieldType.setIndexOptions(options);
                }
            } else {
                fieldType.setIndexOptions(IndexOptions.NONE);
            }
            return builder;
        }

        protected IndexOptions getDefaultIndexOption() {
            return defaultOptions;
        }

        /**
         * @return if this {@link Builder} allows setting of `index_options`
         */
        protected boolean allowsIndexOptions() {
            return true;
        }

        public T store(boolean store) {
            this.fieldType.setStored(store);
            return builder;
        }

        public T docValues(boolean docValues) {
            this.fieldType.setHasDocValues(docValues);
            this.docValuesSet = true;
            return builder;
        }

        public T storeTermVectors(boolean termVectors) {
            if (termVectors != this.fieldType.storeTermVectors()) {
                this.fieldType.setStoreTermVectors(termVectors);
            } // don't set it to false, it is default and might be flipped by a more specific option
            return builder;
        }

        public T storeTermVectorOffsets(boolean termVectorOffsets) {
            if (termVectorOffsets) {
                this.fieldType.setStoreTermVectors(termVectorOffsets);
            }
            this.fieldType.setStoreTermVectorOffsets(termVectorOffsets);
            return builder;
        }

        public T storeTermVectorPositions(boolean termVectorPositions) {
            if (termVectorPositions) {
                this.fieldType.setStoreTermVectors(termVectorPositions);
            }
            this.fieldType.setStoreTermVectorPositions(termVectorPositions);
            return builder;
        }

        public T storeTermVectorPayloads(boolean termVectorPayloads) {
            if (termVectorPayloads) {
                this.fieldType.setStoreTermVectors(termVectorPayloads);
            }
            this.fieldType.setStoreTermVectorPayloads(termVectorPayloads);
            return builder;
        }

        public T tokenized(boolean tokenized) {
            this.fieldType.setTokenized(tokenized);
            return builder;
        }

        public T boost(float boost) {
            this.fieldType.setBoost(boost);
            return builder;
        }

        public T omitNorms(boolean omitNorms) {
            this.fieldType.setOmitNorms(omitNorms);
            this.omitNormsSet = true;
            return builder;
        }

        public T indexOptions(IndexOptions indexOptions) {
            this.fieldType.setIndexOptions(indexOptions);
            this.indexOptionsSet = true;
            return builder;
        }

        public T indexAnalyzer(NamedAnalyzer indexAnalyzer) {
            this.fieldType.setIndexAnalyzer(indexAnalyzer);
            return builder;
        }

        public T searchAnalyzer(NamedAnalyzer searchAnalyzer) {
            this.fieldType.setSearchAnalyzer(searchAnalyzer);
            return builder;
        }

        public T searchQuoteAnalyzer(NamedAnalyzer searchQuoteAnalyzer) {
            this.fieldType.setSearchQuoteAnalyzer(searchQuoteAnalyzer);
            return builder;
        }

        public T includeInAll(Boolean includeInAll) {
            this.includeInAll = includeInAll;
            return builder;
        }

        public T similarity(SimilarityProvider similarity) {
            this.fieldType.setSimilarity(similarity);
            return builder;
        }

        public Builder nullValue(Object nullValue) {
            this.fieldType.setNullValue(nullValue);
            return this;
        }

        public T addMultiField(Mapper.Builder mapperBuilder) {
            multiFieldsBuilder.add(mapperBuilder);
            return builder;
        }

        public T copyTo(CopyTo copyTo) {
            this.copyTo = copyTo;
            return builder;
        }

        protected String buildFullName(BuilderContext context) {
            return context.path().pathAsText(name);
        }

        protected boolean defaultDocValues(Version indexCreated) {
            if (indexCreated.onOrAfter(Version.V_5_0_0_alpha1)) {
                // add doc values by default to keyword (boolean, numerics, etc.) fields
                return fieldType.tokenized() == false;
            } else {
                return fieldType.tokenized() == false && fieldType.indexOptions() != IndexOptions.NONE;
            }
        }

        protected void setupFieldType(BuilderContext context) {
            fieldType.setName(buildFullName(context));
            if (context.indexCreatedVersion().before(Version.V_5_0_0_alpha1)) {
                fieldType.setOmitNorms(fieldType.omitNorms() && fieldType.boost() == 1.0f);
            }
            if (fieldType.indexAnalyzer() == null && fieldType.tokenized() == false && fieldType.indexOptions() != IndexOptions.NONE) {
                fieldType.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
                fieldType.setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
            }
            boolean defaultDocValues = defaultDocValues(context.indexCreatedVersion());
            defaultFieldType.setHasDocValues(defaultDocValues);
            if (docValuesSet == false) {
                fieldType.setHasDocValues(defaultDocValues);
            }
        }
    }

    protected final Version indexCreatedVersion;
    protected MappedFieldType fieldType;
    protected final MappedFieldType defaultFieldType;
    protected MultiFields multiFields;
    protected CopyTo copyTo;

    protected FieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName);
        assert indexSettings != null;
        this.indexCreatedVersion = Version.indexCreated(indexSettings);
        if (indexCreatedVersion.onOrAfter(Version.V_5_0_0_beta1)) {
            if (simpleName.isEmpty()) {
                throw new IllegalArgumentException("name cannot be empty string");
            }
        }
        fieldType.freeze();
        this.fieldType = fieldType;
        defaultFieldType.freeze();
        this.defaultFieldType = defaultFieldType;
        this.multiFields = multiFields;
        this.copyTo = Objects.requireNonNull(copyTo);
    }

    @Override
    public String name() {
        return fieldType().name();
    }

    public MappedFieldType fieldType() {
        return fieldType;
    }

    /**
     * List of fields where this field should be copied to
     */
    public CopyTo copyTo() {
        return copyTo;
    }

    /**
     * Parse using the provided {@link ParseContext} and return a mapping
     * update if dynamic mappings modified the mappings, or {@code null} if
     * mappings were not modified.
     */
    public Mapper parse(ParseContext context) throws IOException {
        final List<IndexableField> fields = new ArrayList<>(2);
        try {
            parseCreateField(context, fields);
            for (IndexableField field : fields) {
                context.doc().add(field);
            }
        } catch (Exception e) {
            throw new MapperParsingException("failed to parse [" + fieldType().name() + "]", e);
        }
        multiFields.parse(this, context);
        return null;
    }

    /**
     * Parse the field value and populate <code>fields</code>.
     */
    protected abstract void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException;

    protected void createFieldNamesField(ParseContext context, List<IndexableField> fields) {
        FieldNamesFieldType fieldNamesFieldType = (FieldNamesFieldMapper.FieldNamesFieldType) context.docMapper()
                .metadataMapper(FieldNamesFieldMapper.class).fieldType();
        if (fieldNamesFieldType != null && fieldNamesFieldType.isEnabled()) {
            for (String fieldName : FieldNamesFieldMapper.extractFieldNames(fieldType().name())) {
                fields.add(new Field(FieldNamesFieldMapper.NAME, fieldName, fieldNamesFieldType));
            }
        }
    }

    @Override
    public Iterator<Mapper> iterator() {
        return multiFields.iterator();
    }

    @Override
    protected FieldMapper clone() {
        try {
            return (FieldMapper) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public FieldMapper merge(Mapper mergeWith, boolean updateAllTypes) {
        FieldMapper merged = clone();
        merged.doMerge(mergeWith, updateAllTypes);
        return merged;
    }

    /**
     * Merge changes coming from {@code mergeWith} in place.
     * @param updateAllTypes TODO
     */
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        if (!this.getClass().equals(mergeWith.getClass())) {
            String mergedType = mergeWith.getClass().getSimpleName();
            if (mergeWith instanceof FieldMapper) {
                mergedType = ((FieldMapper) mergeWith).contentType();
            }
            throw new IllegalArgumentException("mapper [" + fieldType().name() + "] of different type, current_type [" + contentType() + "], merged_type [" + mergedType + "]");
        }
        FieldMapper fieldMergeWith = (FieldMapper) mergeWith;
        multiFields = multiFields.merge(fieldMergeWith.multiFields);

        // apply changeable values
        this.fieldType = fieldMergeWith.fieldType;
        this.copyTo = fieldMergeWith.copyTo;
    }

    @Override
    public FieldMapper updateFieldType(Map<String, MappedFieldType> fullNameToFieldType) {
        final MappedFieldType newFieldType = fullNameToFieldType.get(fieldType.name());
        if (newFieldType == null) {
            // this field does not exist in the mappings yet
            // this can happen if this mapper represents a mapping update
            return this;
        } else if (fieldType.getClass() != newFieldType.getClass()) {
            throw new IllegalStateException("Mixing up field types: " +
                fieldType.getClass() + " != " + newFieldType.getClass() + " on field " + fieldType.name());
        }
        MultiFields updatedMultiFields = multiFields.updateFieldType(fullNameToFieldType);
        if (fieldType == newFieldType && multiFields == updatedMultiFields) {
            return this; // no change
        }
        FieldMapper updated = clone();
        updated.fieldType = newFieldType;
        updated.multiFields = updatedMultiFields;
        return updated;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(simpleName());
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);
        doXContentBody(builder, includeDefaults, params);
        return builder.endObject();
    }

    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {

        builder.field("type", contentType());

        if (includeDefaults || fieldType().boost() != 1.0f) {
            builder.field("boost", fieldType().boost());
        }

        boolean indexed =  fieldType().indexOptions() != IndexOptions.NONE;
        boolean defaultIndexed = defaultFieldType.indexOptions() != IndexOptions.NONE;
        if (includeDefaults || indexed != defaultIndexed ||
            fieldType().tokenized() != defaultFieldType.tokenized()) {
            builder.field("index", indexTokenizeOption(indexed, fieldType().tokenized()));
        }
        if (includeDefaults || fieldType().stored() != defaultFieldType.stored()) {
            builder.field("store", fieldType().stored());
        }
        doXContentDocValues(builder, includeDefaults);
        if (includeDefaults || fieldType().storeTermVectors() != defaultFieldType.storeTermVectors()) {
            builder.field("term_vector", termVectorOptionsToString(fieldType()));
        }
        if (includeDefaults || fieldType().omitNorms() != defaultFieldType.omitNorms()) {
            builder.field("norms", fieldType().omitNorms() == false);
        }
        if (indexed && (includeDefaults || fieldType().indexOptions() != defaultFieldType.indexOptions())) {
            builder.field("index_options", indexOptionToString(fieldType().indexOptions()));
        }
        if (includeDefaults || fieldType().eagerGlobalOrdinals() != defaultFieldType.eagerGlobalOrdinals()) {
            builder.field("eager_global_ordinals", fieldType().eagerGlobalOrdinals());
        }

        if (fieldType().similarity() != null) {
            builder.field("similarity", fieldType().similarity().name());
        } else if (includeDefaults) {
            builder.field("similarity", SimilarityService.DEFAULT_SIMILARITY);
        }

        multiFields.toXContent(builder, params);
        copyTo.toXContent(builder, params);
    }

    protected final void doXContentAnalyzers(XContentBuilder builder, boolean includeDefaults) throws IOException {
        if (fieldType.tokenized() == false) {
            return;
        }
        if (fieldType().indexAnalyzer() == null) {
            if (includeDefaults) {
                builder.field("analyzer", "default");
            }
        } else {
            boolean hasDefaultIndexAnalyzer = fieldType().indexAnalyzer().name().equals("default");
            boolean hasDifferentSearchAnalyzer = fieldType().searchAnalyzer().name().equals(fieldType().indexAnalyzer().name()) == false;
            boolean hasDifferentSearchQuoteAnalyzer = fieldType().searchAnalyzer().name().equals(fieldType().searchQuoteAnalyzer().name()) == false;
            if (includeDefaults || hasDefaultIndexAnalyzer == false || hasDifferentSearchAnalyzer || hasDifferentSearchQuoteAnalyzer) {
                builder.field("analyzer", fieldType().indexAnalyzer().name());
                if (includeDefaults || hasDifferentSearchAnalyzer || hasDifferentSearchQuoteAnalyzer) {
                    builder.field("search_analyzer", fieldType().searchAnalyzer().name());
                    if (includeDefaults || hasDifferentSearchQuoteAnalyzer) {
                        builder.field("search_quote_analyzer", fieldType().searchQuoteAnalyzer().name());
                    }
                }
            }
        }
    }

    protected void doXContentDocValues(XContentBuilder builder, boolean includeDefaults) throws IOException {
        if (includeDefaults || defaultFieldType.hasDocValues() != fieldType().hasDocValues()) {
            builder.field("doc_values", fieldType().hasDocValues());
        }
    }

    protected static String indexOptionToString(IndexOptions indexOption) {
        switch (indexOption) {
            case DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS:
                return TypeParsers.INDEX_OPTIONS_OFFSETS;
            case DOCS_AND_FREQS:
                return TypeParsers.INDEX_OPTIONS_FREQS;
            case DOCS_AND_FREQS_AND_POSITIONS:
                return TypeParsers.INDEX_OPTIONS_POSITIONS;
            case DOCS:
                return TypeParsers.INDEX_OPTIONS_DOCS;
            default:
                throw new IllegalArgumentException("Unknown IndexOptions [" + indexOption + "]");
        }
    }

    public static String termVectorOptionsToString(FieldType fieldType) {
        if (!fieldType.storeTermVectors()) {
            return "no";
        } else if (!fieldType.storeTermVectorOffsets() && !fieldType.storeTermVectorPositions()) {
            return "yes";
        } else if (fieldType.storeTermVectorOffsets() && !fieldType.storeTermVectorPositions()) {
            return "with_offsets";
        } else {
            StringBuilder builder = new StringBuilder("with");
            if (fieldType.storeTermVectorPositions()) {
                builder.append("_positions");
            }
            if (fieldType.storeTermVectorOffsets()) {
                builder.append("_offsets");
            }
            if (fieldType.storeTermVectorPayloads()) {
                builder.append("_payloads");
            }
            return builder.toString();
        }
    }

    /* Only protected so that string can override it */
    protected Object indexTokenizeOption(boolean indexed, boolean tokenized) {
        return indexed;
    }

    protected abstract String contentType();

    public static class MultiFields {

        public static MultiFields empty() {
            return new MultiFields(ImmutableOpenMap.<String, FieldMapper>of());
        }

        public static class Builder {

            private final ImmutableOpenMap.Builder<String, Mapper.Builder> mapperBuilders = ImmutableOpenMap.builder();

            public Builder add(Mapper.Builder builder) {
                mapperBuilders.put(builder.name(), builder);
                return this;
            }

            @SuppressWarnings("unchecked")
            public MultiFields build(FieldMapper.Builder mainFieldBuilder, BuilderContext context) {
                if (mapperBuilders.isEmpty()) {
                    return empty();
                } else {
                    context.path().add(mainFieldBuilder.name());
                    ImmutableOpenMap.Builder mapperBuilders = this.mapperBuilders;
                    for (ObjectObjectCursor<String, Mapper.Builder> cursor : this.mapperBuilders) {
                        String key = cursor.key;
                        Mapper.Builder value = cursor.value;
                        Mapper mapper = value.build(context);
                        assert mapper instanceof FieldMapper;
                        mapperBuilders.put(key, mapper);
                    }
                    context.path().remove();
                    ImmutableOpenMap.Builder<String, FieldMapper> mappers = mapperBuilders.cast();
                    return new MultiFields(mappers.build());
                }
            }
        }

        private final ImmutableOpenMap<String, FieldMapper> mappers;

        private MultiFields(ImmutableOpenMap<String, FieldMapper> mappers) {
            ImmutableOpenMap.Builder<String, FieldMapper> builder = new ImmutableOpenMap.Builder<>();
            // we disable the all in multi-field mappers
            for (ObjectObjectCursor<String, FieldMapper> cursor : mappers) {
                builder.put(cursor.key, cursor.value);
            }
            this.mappers = builder.build();
        }

        public void parse(FieldMapper mainField, ParseContext context) throws IOException {
            // TODO: multi fields are really just copy fields, we just need to expose "sub fields" or something that can be part of the mappings
            if (mappers.isEmpty()) {
                return;
            }

            context = context.createMultiFieldContext();

            context.path().add(mainField.simpleName());
            for (ObjectCursor<FieldMapper> cursor : mappers.values()) {
                cursor.value.parse(context);
            }
            context.path().remove();
        }

        public MultiFields merge(MultiFields mergeWith) {
            ImmutableOpenMap.Builder<String, FieldMapper> newMappersBuilder = ImmutableOpenMap.builder(mappers);

            for (ObjectCursor<FieldMapper> cursor : mergeWith.mappers.values()) {
                FieldMapper mergeWithMapper = cursor.value;
                FieldMapper mergeIntoMapper = mappers.get(mergeWithMapper.simpleName());
                if (mergeIntoMapper == null) {
                    newMappersBuilder.put(mergeWithMapper.simpleName(), mergeWithMapper);
                } else {
                    FieldMapper merged = mergeIntoMapper.merge(mergeWithMapper, false);
                    newMappersBuilder.put(merged.simpleName(), merged); // override previous definition
                }
            }

            ImmutableOpenMap<String, FieldMapper> mappers = newMappersBuilder.build();
            return new MultiFields(mappers);
        }

        public MultiFields updateFieldType(Map<String, MappedFieldType> fullNameToFieldType) {
            ImmutableOpenMap.Builder<String, FieldMapper> newMappersBuilder = null;

            for (ObjectCursor<FieldMapper> cursor : mappers.values()) {
                FieldMapper updated = cursor.value.updateFieldType(fullNameToFieldType);
                if (updated != cursor.value) {
                    if (newMappersBuilder == null) {
                        newMappersBuilder = ImmutableOpenMap.builder(mappers);
                    }
                    newMappersBuilder.put(updated.simpleName(), updated);
                }
            }

            if (newMappersBuilder == null) {
                return this;
            }

            ImmutableOpenMap<String, FieldMapper> mappers = newMappersBuilder.build();
            return new MultiFields(mappers);
        }

        public Iterator<Mapper> iterator() {
            return StreamSupport.stream(mappers.values().spliterator(), false).map((p) -> (Mapper)p.value).iterator();
        }

        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (!mappers.isEmpty()) {
                // sort the mappers so we get consistent serialization format
                Mapper[] sortedMappers = mappers.values().toArray(Mapper.class);
                Arrays.sort(sortedMappers, new Comparator<Mapper>() {
                    @Override
                    public int compare(Mapper o1, Mapper o2) {
                        return o1.name().compareTo(o2.name());
                    }
                });
                builder.startObject("fields");
                for (Mapper mapper : sortedMappers) {
                    mapper.toXContent(builder, params);
                }
                builder.endObject();
            }
            return builder;
        }
    }

    /**
     * Represents a list of fields with optional boost factor where the current field should be copied to
     */
    public static class CopyTo {

        private static final CopyTo EMPTY = new CopyTo(Collections.emptyList());

        public static CopyTo empty() {
            return EMPTY;
        }

        private final List<String> copyToFields;

        private CopyTo(List<String> copyToFields) {
            this.copyToFields = copyToFields;
        }

        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (!copyToFields.isEmpty()) {
                builder.startArray("copy_to");
                for (String field : copyToFields) {
                    builder.value(field);
                }
                builder.endArray();
            }
            return builder;
        }

        public static class Builder {
            private final List<String> copyToBuilders = new ArrayList<>();

            public Builder add(String field) {
                copyToBuilders.add(field);
                return this;
            }

            public CopyTo build() {
                if (copyToBuilders.isEmpty()) {
                    return EMPTY;
                }
                return new CopyTo(Collections.unmodifiableList(copyToBuilders));
            }
        }

        public List<String> copyToFields() {
            return copyToFields;
        }
    }

}
