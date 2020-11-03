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
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.AbstractXContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper.FieldNamesFieldType;
import org.elasticsearch.index.mapper.Mapper.TypeParser.ParserContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

public abstract class FieldMapper extends Mapper implements Cloneable {
    public static final Setting<Boolean> IGNORE_MALFORMED_SETTING =
        Setting.boolSetting("index.mapping.ignore_malformed", false, Property.IndexScope);
    public static final Setting<Boolean> COERCE_SETTING =
        Setting.boolSetting("index.mapping.coerce", false, Property.IndexScope);

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(FieldMapper.class);

    protected MappedFieldType mappedFieldType;
    protected MultiFields multiFields;
    protected CopyTo copyTo;

    protected FieldMapper(String simpleName, MappedFieldType mappedFieldType,
                          MultiFields multiFields, CopyTo copyTo) {
        super(simpleName);
        if (mappedFieldType.name().isEmpty()) {
            throw new IllegalArgumentException("name cannot be empty string");
        }
        this.mappedFieldType = mappedFieldType;
        this.multiFields = multiFields;
        this.copyTo = Objects.requireNonNull(copyTo);
    }

    @Override
    public String name() {
        return fieldType().name();
    }

    @Override
    public String typeName() {
        return mappedFieldType.typeName();
    }

    public MappedFieldType fieldType() {
        return mappedFieldType;
    }

    /**
     * List of fields where this field should be copied to
     */
    public CopyTo copyTo() {
        return copyTo;
    }

    public MultiFields multiFields() {
        return multiFields;
    }

    /**
     * Whether this mapper can handle an array value during document parsing. If true,
     * when an array is encountered during parsing, the document parser will pass the
     * whole array to the mapper. If false, the array is split into individual values
     * and each value is passed to the mapper for parsing.
     */
    public boolean parsesArrayValue() {
        return false;
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

    protected final void createFieldNamesField(ParseContext context) {
        assert fieldType().hasDocValues() == false : "_field_names should only be used when doc_values are turned off";
        FieldNamesFieldType fieldNamesFieldType = context.docMapper().metadataMapper(FieldNamesFieldMapper.class).fieldType();
        if (fieldNamesFieldType != null && fieldNamesFieldType.isEnabled()) {
            for (String fieldName : FieldNamesFieldMapper.extractFieldNames(fieldType().name())) {
                context.doc().add(new Field(FieldNamesFieldMapper.NAME, fieldName, FieldNamesFieldMapper.Defaults.FIELD_TYPE));
            }
        }
    }

    @Override
    public Iterator<Mapper> iterator() {
        return multiFields.iterator();
    }

    @Override
    public final void validate(MappingLookup mappers) {
        if (this.copyTo() != null && this.copyTo().copyToFields().isEmpty() == false) {
            if (mappers.isMultiField(this.name())) {
                throw new IllegalArgumentException("[copy_to] may not be used to copy from a multi-field: [" + this.name() + "]");
            }

            final String sourceScope = mappers.getNestedScope(this.name());
            for (String copyTo : this.copyTo().copyToFields()) {
                if (mappers.isMultiField(copyTo)) {
                    throw new IllegalArgumentException("[copy_to] may not be used to copy to a multi-field: [" + copyTo + "]");
                }
                if (mappers.isObjectField(copyTo)) {
                    throw new IllegalArgumentException("Cannot copy to field [" + copyTo + "] since it is mapped as an object");
                }

                final String targetScope = mappers.getNestedScope(copyTo);
                checkNestedScopeCompatibility(sourceScope, targetScope);
            }
        }
        for (Mapper multiField : multiFields()) {
            multiField.validate(mappers);
        }
        doValidate(mappers);
    }

    protected void doValidate(MappingLookup mappers) { }

    private static void checkNestedScopeCompatibility(String source, String target) {
        boolean targetIsParentOfSource;
        if (source == null || target == null) {
            targetIsParentOfSource = target == null;
        } else {
            targetIsParentOfSource = source.equals(target) || source.startsWith(target + ".");
        }
        if (targetIsParentOfSource == false) {
            throw new IllegalArgumentException(
                "Illegal combination of [copy_to] and [nested] mappings: [copy_to] may only copy data to the current nested " +
                    "document or any of its parents, however one [copy_to] directive is trying to copy data from nested object [" +
                    source + "] to [" + target + "]");
        }
    }

    /**
     * Returns a {@link Builder} to be used for merging and serialization
     *
     * Implement as follows:
     * {@code return new MyBuilder(simpleName()).init(this); }
     */
    public abstract Builder getMergeBuilder();

    @Override
    public final FieldMapper merge(Mapper mergeWith) {

        if (mergeWith instanceof FieldMapper == false) {
            throw new IllegalArgumentException("mapper [" + name() + "] cannot be changed from type ["
                + contentType() + "] to [" + mergeWith.getClass().getSimpleName() + "]");
        }
        checkIncomingMergeType((FieldMapper)mergeWith);

        Builder builder = getMergeBuilder();
        if (builder == null) {
            return (FieldMapper) mergeWith;
        }
        Conflicts conflicts = new Conflicts(name());
        builder.merge((FieldMapper) mergeWith, conflicts);
        conflicts.check();
        return builder.build(new BuilderContext(Settings.EMPTY, parentPath(name())));
    }

    private static ContentPath parentPath(String name) {
        int endPos = name.lastIndexOf(".");
        if (endPos == -1) {
            return new ContentPath(0);
        }
        return new ContentPath(name.substring(0, endPos));
    }

    protected void checkIncomingMergeType(FieldMapper mergeWith) {
        if (Objects.equals(this.getClass(), mergeWith.getClass()) == false) {
            throw new IllegalArgumentException("mapper [" + name() + "] cannot be changed from type ["
                + contentType() + "] to [" + mergeWith.contentType() + "]");
        }
        if (Objects.equals(contentType(), mergeWith.contentType()) == false) {
            throw new IllegalArgumentException("mapper [" + name() + "] cannot be changed from type ["
                + contentType() + "] to [" + mergeWith.contentType() + "]");
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(simpleName());
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);
        doXContentBody(builder, includeDefaults, params);
        return builder.endObject();
    }

    protected boolean indexedByDefault() {
        return true;
    }

    protected boolean docValuesByDefault() {
        return true;
    }

    protected boolean storedByDefault() {
        return false;
    }

    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        builder.field("type", contentType());
        getMergeBuilder().toXContent(builder, includeDefaults);
        multiFields.toXContent(builder, params);
        copyTo.toXContent(builder, params);
    }

    protected abstract String contentType();

    public static class MultiFields implements Iterable<Mapper> {

        public static MultiFields empty() {
            return new MultiFields(ImmutableOpenMap.<String, FieldMapper>of());
        }

        public static class Builder {

            private final ImmutableOpenMap.Builder<String, Mapper.Builder> mapperBuilders = ImmutableOpenMap.builder();

            public Builder add(Mapper.Builder builder) {
                mapperBuilders.put(builder.name(), builder);
                return this;
            }

            public Builder add(Mapper mapper) {
                mapperBuilders.put(mapper.simpleName(), new Mapper.Builder(mapper.simpleName()) {
                    @Override
                    public Mapper build(BuilderContext context) {
                        return mapper;
                    }
                });
                return this;
            }

            public Builder update(Mapper toMerge, ContentPath contentPath) {
                if (mapperBuilders.containsKey(toMerge.simpleName()) == false) {
                    add(toMerge);
                } else {
                    Mapper.Builder builder = mapperBuilders.get(toMerge.simpleName());
                    Mapper existing = builder.build(new BuilderContext(Settings.EMPTY, contentPath));
                    add(existing.merge(toMerge));
                }
                return this;
            }

            @SuppressWarnings("unchecked")
            public MultiFields build(Mapper.Builder mainFieldBuilder, BuilderContext context) {
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

        @Override
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

            public void reset(CopyTo copyTo) {
                copyToBuilders.clear();
                copyToBuilders.addAll(copyTo.copyToFields);
            }
        }

        public List<String> copyToFields() {
            return copyToFields;
        }
    }

    /**
     * Serializes a parameter
     */
    protected interface Serializer<T> {
        void serialize(XContentBuilder builder, String name, T value) throws IOException;
    }

    protected interface MergeValidator<T> {
        boolean canMerge(T previous, T current, Conflicts conflicts);
    }

    /**
     * Check on whether or not a parameter should be serialized
     */
    protected interface SerializerCheck<T> {
        /**
         * Check on whether or not a parameter should be serialized
         * @param includeDefaults   if defaults have been requested
         * @param isConfigured      if the parameter has a different value to the default
         * @param value             the parameter value
         * @return {@code true} if the value should be serialized
         */
        boolean check(boolean includeDefaults, boolean isConfigured, T value);
    }

    /**
     * A configurable parameter for a field mapper
     * @param <T> the type of the value the parameter holds
     */
    public static final class Parameter<T> implements Supplier<T> {

        public final String name;
        private final List<String> deprecatedNames = new ArrayList<>();
        private final Supplier<T> defaultValue;
        private final TriFunction<String, ParserContext, Object, T> parser;
        private final Function<FieldMapper, T> initializer;
        private boolean acceptsNull = false;
        private Consumer<T> validator = null;
        private Serializer<T> serializer = XContentBuilder::field;
        private SerializerCheck<T> serializerCheck = (includeDefaults, isConfigured, value) -> includeDefaults || isConfigured;
        private Function<T, String> conflictSerializer = Objects::toString;
        private boolean deprecated;
        private MergeValidator<T> mergeValidator;
        private T value;
        private boolean isSet;

        /**
         * Creates a new Parameter
         * @param name          the parameter name, used in parsing and serialization
         * @param updateable    whether the parameter can be updated with a new value during a mapping update
         * @param defaultValue  the default value for the parameter, used if unspecified in mappings
         * @param parser        a function that converts an object to a parameter value
         * @param initializer   a function that reads a parameter value from an existing mapper
         */
        public Parameter(String name, boolean updateable, Supplier<T> defaultValue,
                         TriFunction<String, ParserContext, Object, T> parser, Function<FieldMapper, T> initializer) {
            this.name = name;
            this.defaultValue = Objects.requireNonNull(defaultValue);
            this.value = null;
            this.parser = parser;
            this.initializer = initializer;
            this.mergeValidator = (previous, toMerge, conflicts) -> updateable || Objects.equals(previous, toMerge);
        }

        /**
         * Returns the current value of the parameter
         */
        public T getValue() {
            return isSet ? value : defaultValue.get();
        }

        @Override
        public T get() {
            return getValue();
        }

        /**
         * Returns the default value of the parameter
         */
        public T getDefaultValue() {
            return defaultValue.get();
        }

        /**
         * Sets the current value of the parameter
         */
        public void setValue(T value) {
            this.isSet = true;
            this.value = value;
        }

        public boolean isConfigured() {
            return isSet && Objects.equals(value, defaultValue.get()) == false;
        }

        /**
         * Allows the parameter to accept a {@code null} value
         */
        public Parameter<T> acceptsNull() {
            this.acceptsNull = true;
            return this;
        }

        /**
         * Adds a deprecated parameter name.
         *
         * If this parameter name is encountered during parsing, a deprecation warning will
         * be emitted.  The parameter will be serialized with its main name.
         */
        public Parameter<T> addDeprecatedName(String deprecatedName) {
            this.deprecatedNames.add(deprecatedName);
            return this;
        }

        /**
         * Deprecates the entire parameter.
         *
         * If this parameter is encountered during parsing, a deprecation warning will
         * be emitted.
         */
        public Parameter<T> deprecated() {
            this.deprecated = true;
            return this;
        }

        /**
         * Adds validation to a parameter, called after parsing and merging
         */
        public Parameter<T> setValidator(Consumer<T> validator) {
            this.validator = validator;
            return this;
        }

        /**
         * Configure a custom serializer for this parameter
         */
        public Parameter<T> setSerializer(Serializer<T> serializer, Function<T, String> conflictSerializer) {
            this.serializer = serializer;
            this.conflictSerializer = conflictSerializer;
            return this;
        }

        /**
         * Configure a custom serialization check for this parameter
         */
        public Parameter<T> setSerializerCheck(SerializerCheck<T> check) {
            this.serializerCheck = check;
            return this;
        }

        /**
         * Always serialize this parameter, no matter its value
         */
        public Parameter<T> alwaysSerialize() {
            this.serializerCheck = (id, ic, v) -> true;
            return this;
        }

        /**
         * Never serialize this parameter, no matter its value
         */
        public Parameter<T> neverSerialize() {
            this.serializerCheck = (id, ic, v) -> false;
            return this;
        }

        /**
         * Sets a custom merge validator.  By default, merges are accepted if the
         * parameter is updateable, or if the previous and new values are equal
         */
        public Parameter<T> setMergeValidator(MergeValidator<T> mergeValidator) {
            this.mergeValidator = mergeValidator;
            return this;
        }

        private void validate() {
            if (validator != null) {
                validator.accept(getValue());
            }
        }

        private void init(FieldMapper toInit) {
            setValue(initializer.apply(toInit));
        }

        private void parse(String field, ParserContext context, Object in) {
            setValue(parser.apply(field, context, in));
        }

        private void merge(FieldMapper toMerge, Conflicts conflicts) {
            T value = initializer.apply(toMerge);
            T current = getValue();
            if (mergeValidator.canMerge(current, value, conflicts)) {
                setValue(value);
            } else {
                conflicts.addConflict(name, conflictSerializer.apply(current), conflictSerializer.apply(value));
            }
        }

        protected void toXContent(XContentBuilder builder, boolean includeDefaults) throws IOException {
            if (serializerCheck.check(includeDefaults, isConfigured(), get())) {
                serializer.serialize(builder, name, getValue());
            }
        }

        /**
         * Defines a parameter that takes the values {@code true} or {@code false}
         * @param name          the parameter name
         * @param updateable    whether the parameter can be changed by a mapping update
         * @param initializer   a function that reads the parameter value from an existing mapper
         * @param defaultValue  the default value, to be used if the parameter is undefined in a mapping
         */
        public static Parameter<Boolean> boolParam(String name, boolean updateable,
                                                   Function<FieldMapper, Boolean> initializer, boolean defaultValue) {
            return new Parameter<>(name, updateable, () -> defaultValue, (n, c, o) -> XContentMapValues.nodeBooleanValue(o), initializer);
        }

        /**
         * Defines a parameter that takes the values {@code true} or {@code false}, and will always serialize
         * its value if configured.
         * @param name          the parameter name
         * @param updateable    whether the parameter can be changed by a mapping update
         * @param initializer   a function that reads the parameter value from an existing mapper
         * @param defaultValue  the default value, to be used if the parameter is undefined in a mapping
         */
        public static Parameter<Explicit<Boolean>> explicitBoolParam(String name, boolean updateable,
                                                                     Function<FieldMapper, Explicit<Boolean>> initializer,
                                                                     boolean defaultValue) {
            Explicit<Boolean> defaultExplicit = new Explicit<>(defaultValue, false);
            return new Parameter<>(name, updateable, () -> defaultExplicit,
                (n, c, o) -> new Explicit<>(XContentMapValues.nodeBooleanValue(o), true), initializer)
                .setSerializer((b, n, v) -> b.field(n, v.value()), v -> Boolean.toString(v.value()));
        }

        /**
         * Defines a parameter that takes a double value
         * @param name          the parameter name
         * @param updateable    whether the parameter can be changed by a mapping update
         * @param initializer   a function that reads the parameter value from an existing mapper
         * @param defaultValue  the default value, to be used if the parameter is undefined in a mapping
         */
        public static Parameter<Double> doubleParam(String name, boolean updateable,
                                                    Function<FieldMapper, Double> initializer, double defaultValue) {
            return new Parameter<>(name, updateable, () -> defaultValue, (n, c, o) -> XContentMapValues.nodeDoubleValue(o), initializer);
        }

        /**
         * Defines a parameter that takes a float value
         * @param name          the parameter name
         * @param updateable    whether the parameter can be changed by a mapping update
         * @param initializer   a function that reads the parameter value from an existing mapper
         * @param defaultValue  the default value, to be used if the parameter is undefined in a mapping
         */
        public static Parameter<Float> floatParam(String name, boolean updateable,
                                                  Function<FieldMapper, Float> initializer, float defaultValue) {
            return new Parameter<>(name, updateable, () -> defaultValue, (n, c, o) -> XContentMapValues.nodeFloatValue(o), initializer);
        }

        /**
         * Defines a parameter that takes an integer value
         * @param name          the parameter name
         * @param updateable    whether the parameter can be changed by a mapping update
         * @param initializer   a function that reads the parameter value from an existing mapper
         * @param defaultValue  the default value, to be used if the parameter is undefined in a mapping
         */
        public static Parameter<Integer> intParam(String name, boolean updateable,
                                                  Function<FieldMapper, Integer> initializer, int defaultValue) {
            return new Parameter<>(name, updateable, () -> defaultValue, (n, c, o) -> XContentMapValues.nodeIntegerValue(o), initializer);
        }

        /**
         * Defines a parameter that takes a string value
         * @param name          the parameter name
         * @param updateable    whether the parameter can be changed by a mapping update
         * @param initializer   a function that reads the parameter value from an existing mapper
         * @param defaultValue  the default value, to be used if the parameter is undefined in a mapping
         */
        public static Parameter<String> stringParam(String name, boolean updateable,
                                                    Function<FieldMapper, String> initializer, String defaultValue) {
            return new Parameter<>(name, updateable, () -> defaultValue,
                (n, c, o) -> XContentMapValues.nodeStringValue(o), initializer);
        }

        @SuppressWarnings("unchecked")
        public static Parameter<List<String>> stringArrayParam(String name, boolean updateable,
                                                               Function<FieldMapper, List<String>> initializer, List<String> defaultValue) {
            return new Parameter<>(name, updateable, () -> defaultValue,
                (n, c, o) -> {
                    List<Object> values = (List<Object>) o;
                    List<String> strValues = new ArrayList<>();
                    for (Object item : values) {
                        strValues.add(item.toString());
                    }
                    return strValues;
                }, initializer);
        }

        /**
         * Defines a parameter that takes one of a restricted set of string values
         * @param name          the parameter name
         * @param updateable    whether the parameter can be changed by a mapping update
         * @param initializer   a function that reads the parameter value from an existing mapper
         * @param values        the set of values that the parameter can take.  The first value in the list
         *                      is the default value, to be used if the parameter is undefined in a mapping
         */
        public static Parameter<String> restrictedStringParam(String name, boolean updateable,
                                                              Function<FieldMapper, String> initializer, String... values) {
            assert values.length > 0;
            Set<String> acceptedValues = new LinkedHashSet<>(Arrays.asList(values));
            return stringParam(name, updateable, initializer, values[0])
                .setValidator(v -> {
                    if (acceptedValues.contains(v)) {
                        return;
                    }
                    throw new MapperParsingException("Unknown value [" + v + "] for field [" + name +
                        "] - accepted values are " + acceptedValues.toString());
                });
        }

        /**
         * Defines a parameter that takes an analyzer name
         * @param name              the parameter name
         * @param updateable        whether the parameter can be changed by a mapping update
         * @param initializer       a function that reads the parameter value from an existing mapper
         * @param defaultAnalyzer   the default value, to be used if the parameter is undefined in a mapping
         */
        public static Parameter<NamedAnalyzer> analyzerParam(String name, boolean updateable,
                                                             Function<FieldMapper, NamedAnalyzer> initializer,
                                                             Supplier<NamedAnalyzer> defaultAnalyzer) {
            return new Parameter<>(name, updateable, defaultAnalyzer, (n, c, o) -> {
                String analyzerName = o.toString();
                NamedAnalyzer a = c.getIndexAnalyzers().get(analyzerName);
                if (a == null) {
                    throw new IllegalArgumentException("analyzer [" + analyzerName + "] has not been configured in mappings");
                }
                return a;
            }, initializer).setSerializer((b, n, v) -> b.field(n, v.name()), NamedAnalyzer::name);
        }

        /**
         * Declares a metadata parameter
         */
        public static Parameter<Map<String, String>> metaParam() {
            return new Parameter<>("meta", true, Collections::emptyMap,
                (n, c, o) -> TypeParsers.parseMeta(n, o), m -> m.fieldType().meta());
        }

        public static Parameter<Boolean> indexParam(Function<FieldMapper, Boolean> initializer, boolean defaultValue) {
            return Parameter.boolParam("index", false, initializer, defaultValue);
        }

        public static Parameter<Boolean> storeParam(Function<FieldMapper, Boolean> initializer, boolean defaultValue) {
            return Parameter.boolParam("store", false, initializer, defaultValue);
        }

        public static Parameter<Boolean> docValuesParam(Function<FieldMapper, Boolean> initializer, boolean defaultValue) {
            return Parameter.boolParam("doc_values", false, initializer, defaultValue);
        }

    }

    public static final class Conflicts {

        private final String mapperName;
        private final List<String> conflicts = new ArrayList<>();

        Conflicts(String mapperName) {
            this.mapperName = mapperName;
        }

        public void addConflict(String parameter, String conflict) {
            conflicts.add("Conflict in parameter [" + parameter + "]: " + conflict);
        }

        void addConflict(String parameter, String existing, String toMerge) {
            conflicts.add("Cannot update parameter [" + parameter + "] from [" + existing + "] to [" + toMerge + "]");
        }

        void check() {
            if (conflicts.isEmpty()) {
                return;
            }
            String message = "Mapper for [" + mapperName + "] conflicts with existing mapper:\n\t"
                + String.join("\n\t", conflicts);
            throw new IllegalArgumentException(message);
        }

    }

    /**
     * A Builder for a ParametrizedFieldMapper
     */
    public abstract static class Builder extends Mapper.Builder {

        protected final MultiFields.Builder multiFieldsBuilder = new MultiFields.Builder();
        protected final CopyTo.Builder copyTo = new CopyTo.Builder();

        /**
         * Creates a new Builder with a field name
         */
        protected Builder(String name) {
            super(name);
        }

        /**
         * Initialises all parameters from an existing mapper
         */
        public Builder init(FieldMapper initializer) {
            for (Parameter<?> param : getParameters()) {
                param.init(initializer);
            }
            for (Mapper subField : initializer.multiFields) {
                multiFieldsBuilder.add(subField);
            }
            return this;
        }

        private void merge(FieldMapper in, Conflicts conflicts) {
            for (Parameter<?> param : getParameters()) {
                param.merge(in, conflicts);
            }
            for (Mapper newSubField : in.multiFields) {
                multiFieldsBuilder.update(newSubField, parentPath(newSubField.name()));
            }
            this.copyTo.reset(in.copyTo);
            validate();
        }

        private void validate() {
            for (Parameter<?> param : getParameters()) {
                param.validate();
            }
        }

        /**
         * @return the list of parameters defined for this mapper
         */
        protected abstract List<Parameter<?>> getParameters();

        @Override
        public abstract FieldMapper build(BuilderContext context);

        /**
         * Builds the full name of the field, taking into account parent objects
         */
        protected String buildFullName(BuilderContext context) {
            return context.path().pathAsText(name);
        }

        /**
         * Writes the current builder parameter values as XContent
         */
        protected final void toXContent(XContentBuilder builder, boolean includeDefaults) throws IOException {
            for (Parameter<?> parameter : getParameters()) {
                parameter.toXContent(builder, includeDefaults);
            }
        }

        /**
         * Parse mapping parameters from a map of mappings
         * @param name              the field mapper name
         * @param parserContext     the parser context
         * @param fieldNode         the root node of the map of mappings for this field
         */
        public final void parse(String name, ParserContext parserContext, Map<String, Object> fieldNode) {
            Map<String, Parameter<?>> paramsMap = new HashMap<>();
            Map<String, Parameter<?>> deprecatedParamsMap = new HashMap<>();
            for (Parameter<?> param : getParameters()) {
                paramsMap.put(param.name, param);
                for (String deprecatedName : param.deprecatedNames) {
                    deprecatedParamsMap.put(deprecatedName, param);
                }
            }
            String type = (String) fieldNode.remove("type");
            for (Iterator<Map.Entry<String, Object>> iterator = fieldNode.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                final String propName = entry.getKey();
                final Object propNode = entry.getValue();
                if (Objects.equals("fields", propName)) {
                    TypeParsers.parseMultiField(multiFieldsBuilder::add, name, parserContext, propName, propNode);
                    iterator.remove();
                    continue;
                }
                if (Objects.equals("copy_to", propName)) {
                    TypeParsers.parseCopyFields(propNode).forEach(copyTo::add);
                    iterator.remove();
                    continue;
                }
                if (Objects.equals("boost", propName)) {
                    if (parserContext.indexVersionCreated().before(Version.V_8_0_0)) {
                        deprecationLogger.deprecate(
                            "boost",
                            "Parameter [boost] on field [{}] is deprecated and has no effect",
                            name);
                        iterator.remove();
                        continue;
                    } else {
                        throw new MapperParsingException("Unknown parameter [boost] on mapper [" + name + "]");
                    }
                }
                Parameter<?> parameter = deprecatedParamsMap.get(propName);
                if (parameter != null) {
                    deprecationLogger.deprecate(propName, "Parameter [{}] on mapper [{}] is deprecated, use [{}]",
                        propName, name, parameter.name);
                } else {
                    parameter = paramsMap.get(propName);
                }
                if (parameter == null) {
                    if (isDeprecatedParameter(propName, parserContext.indexVersionCreated())) {
                        deprecationLogger.deprecate(propName,
                            "Parameter [{}] has no effect on type [{}] and will be removed in future", propName, type);
                        iterator.remove();
                        continue;
                    }
                    throw new MapperParsingException("unknown parameter [" + propName
                        + "] on mapper [" + name + "] of type [" + type + "]");
                }
                if (parameter.deprecated) {
                    deprecationLogger.deprecate(propName,
                        "Parameter [{}] is deprecated and will be removed in a future version",
                        propName);
                }
                if (propNode == null && parameter.acceptsNull == false) {
                    throw new MapperParsingException("[" + propName + "] on mapper [" + name
                        + "] of type [" + type + "] must not have a [null] value");
                }
                parameter.parse(name, parserContext, propNode);
                iterator.remove();
            }
            validate();
        }

        // These parameters were previously *always* parsed by TypeParsers#parseField(), even if they
        // made no sense; if we've got here, that means that they're not declared on a current mapper,
        // and so we emit a deprecation warning rather than failing a previously working mapping.
        private static final Set<String> DEPRECATED_PARAMS
            = Set.of("store", "meta", "index", "doc_values", "index_options", "similarity");

        private static boolean isDeprecatedParameter(String propName, Version indexCreatedVersion) {
            if (indexCreatedVersion.onOrAfter(Version.V_8_0_0)) {
                return false;
            }
            return DEPRECATED_PARAMS.contains(propName);
        }
    }

    /**
     * TypeParser implementation that automatically handles parsing
     */
    public static final class TypeParser implements Mapper.TypeParser {

        private final BiFunction<String, ParserContext, Builder> builderFunction;

        /**
         * Creates a new TypeParser
         * @param builderFunction a function that produces a Builder from a name and parsercontext
         */
        public TypeParser(BiFunction<String, ParserContext, Builder> builderFunction) {
            this.builderFunction = builderFunction;
        }

        @Override
        public Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = builderFunction.apply(name, parserContext);
            builder.parse(name, parserContext, node);
            return builder;
        }
    }

}
