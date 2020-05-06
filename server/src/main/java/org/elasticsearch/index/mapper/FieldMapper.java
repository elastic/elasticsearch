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
import org.elasticsearch.Version;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.AbstractXContentParser;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper.FieldNamesFieldType;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.StreamSupport;

public abstract class FieldMapper extends Mapper implements Cloneable {
    public static final Setting<Boolean> IGNORE_MALFORMED_SETTING =
        Setting.boolSetting("index.mapping.ignore_malformed", false, Property.IndexScope);
    public static final Setting<Boolean> COERCE_SETTING =
        Setting.boolSetting("index.mapping.coerce", false, Property.IndexScope);
    public abstract static class Builder<T extends Builder> extends Mapper.Builder<T> {

        protected final MappedFieldType fieldType;
        protected final MappedFieldType defaultFieldType;
        private final IndexOptions defaultOptions;
        protected boolean omitNormsSet = false;
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
                        throw new IllegalArgumentException("mapper [" + name + "] has different [index] values from other types"
                            + " of the same index");
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

        public T similarity(SimilarityProvider similarity) {
            this.fieldType.setSimilarity(similarity);
            return builder;
        }

        public Builder nullValue(Object nullValue) {
            this.fieldType.setNullValue(nullValue);
            return this;
        }

        public T addMultiField(Mapper.Builder<?> mapperBuilder) {
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
            return fieldType.tokenized() == false;
        }

        protected void setupFieldType(BuilderContext context) {
            fieldType.setName(buildFullName(context));
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

        /** Set metadata on this field. */
        public T meta(Map<String, String> meta) {
            fieldType.setMeta(meta);
            return (T) this;
        }
    }

    protected final Version indexCreatedVersion;
    protected MappedFieldType fieldType;
    protected final MappedFieldType defaultFieldType;
    protected MultiFields multiFields;
    protected CopyTo copyTo;

    protected FieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                          Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName);
        assert indexSettings != null;
        this.indexCreatedVersion = Version.indexCreated(indexSettings);
        if (simpleName.isEmpty()) {
            throw new IllegalArgumentException("name cannot be empty string");
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

    @Override
    public String typeName() {
        return fieldType.typeName();
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
     * Parse the field value using the provided {@link ParseContext}.
     */
    public void parse(ParseContext context) throws IOException {
        try {
            parseCreateField(context);
        } catch (Exception e) {
            String valuePreview = "";
            try {
                XContentParser parser = context.parser();
                Object complexValue = AbstractXContentParser.readValue(parser, HashMap::new);
                if (complexValue == null) {
                    valuePreview = "null";
                } else {
                    valuePreview = complexValue.toString();
                }
            } catch (Exception innerException) {
                throw new MapperParsingException("failed to parse field [{}] of type [{}] in document with id '{}'. " +
                    "Could not parse field value preview,",
                    e, fieldType().name(), fieldType().typeName(), context.sourceToParse().id());
            }

            throw new MapperParsingException("failed to parse field [{}] of type [{}] in document with id '{}'. " +
                "Preview of field's value: '{}'", e, fieldType().name(), fieldType().typeName(),
                context.sourceToParse().id(), valuePreview);
        }
        multiFields.parse(this, context);
    }

    /**
     * Parse the field value and populate the fields on {@link ParseContext#doc()}.
     *
     * Implementations of this method should ensure that on failing to parse parser.currentToken() must be the
     * current failing token
     */
    protected abstract void parseCreateField(ParseContext context) throws IOException;

    /**
     * Given access to a document's _source, return this field's values.
     *
     * In addition to pulling out the values, mappers can parse them into a standard form. This
     * method delegates parsing to {@link #parseSourceValue} for parsing. Most mappers will choose
     * to override {@link #parseSourceValue} -- for example numeric field mappers make sure to
     * parse the  source value into a number of the right type.
     *
     * Some mappers may need more flexibility and can override this entire method instead.
     *
     * @param lookup a lookup structure over the document's source.
     * @return a list a standardized field values.
     */
    public List<?> lookupValues(SourceLookup lookup) {
        Object sourceValue = lookup.extractValue(name());
        if (sourceValue == null) {
            return List.of();
        }

        List<Object> values = new ArrayList<>();
        if (this instanceof ArrayValueMapperParser) {
            return (List<?>) parseSourceValue(sourceValue);
        } else {
            List<?> sourceValues = sourceValue instanceof List ? (List<?>) sourceValue : List.of(sourceValue);
            for (Object value : sourceValues) {
                Object parsedValue = parseSourceValue(value);
                values.add(parsedValue);
            }
        }
        return values;
    }

    /**
     * Given a value that has been extracted from a document's source, parse it into a standard
     * format. This parsing logic should closely mirror the value parsing in
     * {@link #parseCreateField} or {@link #parse}.
     *
     * Note that when overriding this method, {@link #lookupValues} should *not* be overridden.
     */
    protected abstract Object parseSourceValue(Object value);

    protected void createFieldNamesField(ParseContext context) {
        FieldNamesFieldType fieldNamesFieldType = context.docMapper().metadataMapper(FieldNamesFieldMapper.class).fieldType();
        if (fieldNamesFieldType != null && fieldNamesFieldType.isEnabled()) {
            for (String fieldName : FieldNamesFieldMapper.extractFieldNames(fieldType().name())) {
                context.doc().add(new Field(FieldNamesFieldMapper.NAME, fieldName, fieldNamesFieldType));
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
    public final FieldMapper merge(Mapper mergeWith) {
        FieldMapper merged = clone();
        List<String> conflicts = new ArrayList<>();
        if (mergeWith instanceof FieldMapper == false) {
            throw new IllegalArgumentException("mapper [" + fieldType.name() + "] cannot be changed from type ["
            + contentType() + "] to [" + mergeWith.getClass().getSimpleName() + "]");
        }
        FieldMapper toMerge = (FieldMapper) mergeWith;
        merged.mergeSharedOptions(toMerge, conflicts);
        merged.mergeOptions(toMerge, conflicts);
        if (conflicts.isEmpty() == false) {
            throw new IllegalArgumentException("Mapper for [" + name() +
                "] conflicts with existing mapping:\n" + conflicts.toString());
        }
        merged.multiFields = multiFields.merge(toMerge.multiFields);
        // apply changeable values
        merged.fieldType = toMerge.fieldType;
        merged.copyTo = toMerge.copyTo;
        return merged;
    }

    private void mergeSharedOptions(FieldMapper mergeWith, List<String> conflicts) {

        if (Objects.equals(this.contentType(), mergeWith.contentType()) == false) {
            throw new IllegalArgumentException("mapper [" + fieldType().name() + "] cannot be changed from type [" + contentType()
                + "] to [" + mergeWith.contentType() + "]");
        }

        MappedFieldType other = mergeWith.fieldType;

        boolean indexed =  fieldType.indexOptions() != IndexOptions.NONE;
        boolean mergeWithIndexed = other.indexOptions() != IndexOptions.NONE;
        // TODO: should be validating if index options go "up" (but "down" is ok)
        if (indexed != mergeWithIndexed) {
            conflicts.add("mapper [" + name() + "] has different [index] values");
        }
        if (fieldType.stored() != other.stored()) {
            conflicts.add("mapper [" + name() + "] has different [store] values");
        }
        if (fieldType.hasDocValues() != other.hasDocValues()) {
            conflicts.add("mapper [" + name() + "] has different [doc_values] values");
        }
        if (fieldType.omitNorms() && !other.omitNorms()) {
            conflicts.add("mapper [" + name() + "] has different [norms] values, cannot change from disable to enabled");
        }
        if (fieldType.storeTermVectors() != other.storeTermVectors()) {
            conflicts.add("mapper [" + name() + "] has different [store_term_vector] values");
        }
        if (fieldType.storeTermVectorOffsets() != other.storeTermVectorOffsets()) {
            conflicts.add("mapper [" + name() + "] has different [store_term_vector_offsets] values");
        }
        if (fieldType.storeTermVectorPositions() != other.storeTermVectorPositions()) {
            conflicts.add("mapper [" + name() + "] has different [store_term_vector_positions] values");
        }
        if (fieldType.storeTermVectorPayloads() != other.storeTermVectorPayloads()) {
            conflicts.add("mapper [" + name() + "] has different [store_term_vector_payloads] values");
        }

        // null and "default"-named index analyzers both mean the default is used
        if (fieldType.indexAnalyzer() == null || "default".equals(fieldType.indexAnalyzer().name())) {
            if (other.indexAnalyzer() != null && "default".equals(other.indexAnalyzer().name()) == false) {
                conflicts.add("mapper [" + name() + "] has different [analyzer]");
            }
        } else if (other.indexAnalyzer() == null || "default".equals(other.indexAnalyzer().name())) {
            conflicts.add("mapper [" + name() + "] has different [analyzer]");
        } else if (fieldType.indexAnalyzer().name().equals(other.indexAnalyzer().name()) == false) {
            conflicts.add("mapper [" + name() + "] has different [analyzer]");
        }

        if (Objects.equals(fieldType.similarity(), other.similarity()) == false) {
            conflicts.add("mapper [" + name() + "] has different [similarity]");
        }
    }

    /**
     * Merge type-specific options and check for incompatible settings in mappings to be merged
     */
    protected abstract void mergeOptions(FieldMapper other, List<String> conflicts);

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
        if (includeDefaults || indexed != defaultIndexed) {
            builder.field("index", indexed);
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

        if (includeDefaults || fieldType().meta().isEmpty() == false) {
            builder.field("meta", new TreeMap<>(fieldType().meta())); // ensure consistent order
        }
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
            final String searchAnalyzerName = fieldType().searchAnalyzer().name();
            boolean hasDifferentSearchAnalyzer = searchAnalyzerName.equals(fieldType().indexAnalyzer().name()) == false;
            boolean hasDifferentSearchQuoteAnalyzer = searchAnalyzerName.equals(fieldType().searchQuoteAnalyzer().name()) == false;
            if (includeDefaults || hasDefaultIndexAnalyzer == false || hasDifferentSearchAnalyzer || hasDifferentSearchQuoteAnalyzer) {
                builder.field("analyzer", fieldType().indexAnalyzer().name());
                if (includeDefaults || hasDifferentSearchAnalyzer || hasDifferentSearchQuoteAnalyzer) {
                    builder.field("search_analyzer", searchAnalyzerName);
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
            // TODO: multi fields are really just copy fields, we just need to expose "sub fields" or something that can be part
            // of the mappings
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
                    FieldMapper merged = mergeIntoMapper.merge(mergeWithMapper);
                    newMappersBuilder.put(merged.simpleName(), merged); // override previous definition
                }
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
