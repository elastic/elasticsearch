/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.AbstractXContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.lookup.SearchLookup;

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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class FieldMapper extends Mapper implements Cloneable {
    public static final Setting<Boolean> IGNORE_MALFORMED_SETTING =
        Setting.boolSetting("index.mapping.ignore_malformed", false, Property.IndexScope);
    public static final Setting<Boolean> COERCE_SETTING =
        Setting.boolSetting("index.mapping.coerce", false, Property.IndexScope);

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(FieldMapper.class);

    protected final MappedFieldType mappedFieldType;
    protected final Map<String, NamedAnalyzer> indexAnalyzers;
    protected final MultiFields multiFields;
    protected final CopyTo copyTo;
    protected final boolean hasScript;
    protected final String onScriptError;

    /**
     * Create a FieldMapper with no index analyzers
     * @param simpleName        the leaf name of the mapper
     * @param mappedFieldType   the MappedFieldType associated with this mapper
     * @param multiFields       sub fields of this mapper
     * @param copyTo            copyTo fields of this mapper
     */
    protected FieldMapper(String simpleName, MappedFieldType mappedFieldType,
                          MultiFields multiFields, CopyTo copyTo) {
        this(simpleName, mappedFieldType, Collections.emptyMap(), multiFields, copyTo, false, null);
    }

    /**
     * Create a FieldMapper with no index analyzers
     * @param simpleName        the leaf name of the mapper
     * @param mappedFieldType   the MappedFieldType associated with this mapper
     * @param multiFields       sub fields of this mapper
     * @param copyTo            copyTo fields of this mapper
     * @param hasScript         whether a script is defined for the field
     * @param onScriptError     the behaviour for when the defined script fails at runtime
     */
    protected FieldMapper(String simpleName, MappedFieldType mappedFieldType,
                          MultiFields multiFields, CopyTo copyTo,
                          boolean hasScript, String onScriptError) {
        this(simpleName, mappedFieldType, Collections.emptyMap(), multiFields, copyTo, hasScript, onScriptError);
    }


    /**
     * Create a FieldMapper with a single associated index analyzer
     * @param simpleName        the leaf name of the mapper
     * @param mappedFieldType   the MappedFieldType associated with this mapper
     * @param indexAnalyzer     the index-time analyzer to use for this field
     * @param multiFields       sub fields of this mapper
     * @param copyTo            copyTo fields of this mapper
     */
    protected FieldMapper(String simpleName, MappedFieldType mappedFieldType,
                          NamedAnalyzer indexAnalyzer,
                          MultiFields multiFields, CopyTo copyTo) {
        this(simpleName, mappedFieldType, Collections.singletonMap(mappedFieldType.name(), indexAnalyzer), multiFields, copyTo,
             false, null);
    }

    /**
     * Create a FieldMapper with a single associated index analyzer
     * @param simpleName        the leaf name of the mapper
     * @param mappedFieldType   the MappedFieldType associated with this mapper
     * @param indexAnalyzer     the index-time analyzer to use for this field
     * @param multiFields       sub fields of this mapper
     * @param copyTo            copyTo fields of this mapper
     * @param hasScript         whether a script is defined for the field
     * @param onScriptError     the behaviour for when the defined script fails at runtime
     */
    protected FieldMapper(String simpleName, MappedFieldType mappedFieldType,
                          NamedAnalyzer indexAnalyzer,
                          MultiFields multiFields, CopyTo copyTo,
                          boolean hasScript, String onScriptError) {
        this(simpleName, mappedFieldType, Collections.singletonMap(mappedFieldType.name(), indexAnalyzer), multiFields, copyTo,
            hasScript, onScriptError);
    }

    /**
     * Create a FieldMapper that indexes into multiple analyzed fields
     * @param simpleName        the leaf name of the mapper
     * @param mappedFieldType   the MappedFieldType associated with this mapper
     * @param indexAnalyzers    a map of field names to analyzers, one for each analyzed field
     *                          the mapper will add
     * @param multiFields       sub fields of this mapper
     * @param copyTo            copyTo fields of this mapper
     * @param hasScript         whether a script is defined for the field
     * @param onScriptError     the behaviour for when the defined script fails at runtime
     */
    protected FieldMapper(String simpleName, MappedFieldType mappedFieldType,
                          Map<String, NamedAnalyzer> indexAnalyzers,
                          MultiFields multiFields, CopyTo copyTo,
                          boolean hasScript, String onScriptError) {
        super(simpleName);
        if (mappedFieldType.name().isEmpty()) {
            throw new IllegalArgumentException("name cannot be empty string");
        }
        this.mappedFieldType = mappedFieldType;
        this.indexAnalyzers = indexAnalyzers;
        this.multiFields = multiFields;
        this.copyTo = Objects.requireNonNull(copyTo);
        this.hasScript = hasScript;
        this.onScriptError = onScriptError;
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
     * Parse the field value using the provided {@link DocumentParserContext}.
     */
    public void parse(DocumentParserContext context) throws IOException {
        try {
            if (hasScript) {
                throw new IllegalArgumentException("Cannot index data directly into a field with a [script] parameter");
            }
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
     * Parse the field value and populate the fields on {@link DocumentParserContext#doc()}.
     *
     * Implementations of this method should ensure that on failing to parse parser.currentToken() must be the
     * current failing token
     */
    protected abstract void parseCreateField(DocumentParserContext context) throws IOException;

    /**
     * @return whether this field mapper uses a script to generate its values
     */
    public final boolean hasScript() {
        return hasScript;
    }

    /**
     * Execute the index-time script associated with this field mapper.
     *
     * This method should only be called if {@link #hasScript()} has returned {@code true}
     * @param searchLookup  a SearchLookup to be passed the script
     * @param readerContext a LeafReaderContext exposing values from an incoming document
     * @param doc           the id of the document to execute the script against
     * @param documentParserContext  the ParseContext over the incoming document
     */
    public final void executeScript(SearchLookup searchLookup, LeafReaderContext readerContext, int doc,
                                    DocumentParserContext documentParserContext) {
        try {
            indexScriptValues(searchLookup, readerContext, doc, documentParserContext);
        } catch (Exception e) {
            if ("continue".equals(onScriptError)) {
                documentParserContext.addIgnoredField(name());
            } else {
                throw new MapperParsingException("Error executing script on field [" + name() + "]", e);
            }
        }
    }

    /**
     * Run the script associated with the field and index the values that it emits
     *
     * This method should only be called if {@link #hasScript()} has returned {@code true}
     * @param searchLookup  a SearchLookup to be passed the script
     * @param readerContext a LeafReaderContext exposing values from an incoming document
     * @param doc           the id of the document to execute the script against
     * @param documentParserContext  the ParseContext over the incoming document
     */
    protected void indexScriptValues(SearchLookup searchLookup, LeafReaderContext readerContext, int doc,
                                     DocumentParserContext documentParserContext) {
        throw new UnsupportedOperationException("FieldMapper " + name() + " does not support [script]");
    }

    @Override
    public Iterator<Mapper> iterator() {
        Iterator<FieldMapper> multiFieldsIterator = multiFields.iterator();
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return multiFieldsIterator.hasNext();
            }

            @Override
            public Mapper next() {
                return multiFieldsIterator.next();
            }
        };
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
        return builder.build(Builder.parentPath(name()));
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
        doXContentBody(builder, params);
        return builder.endObject();
    }

    protected void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field("type", contentType());
        getMergeBuilder().toXContent(builder, params);
        multiFields.toXContent(builder, params);
        copyTo.toXContent(builder, params);
    }

    protected abstract String contentType();

    public final Map<String, NamedAnalyzer> indexAnalyzers() {
        return indexAnalyzers;
    }

    public static class MultiFields implements Iterable<FieldMapper>, ToXContent {

        private static final MultiFields EMPTY = new MultiFields(Collections.emptyMap());

        public static MultiFields empty() {
            return EMPTY;
        }

        public static class Builder {

            private final Map<String, Function<ContentPath, FieldMapper>> mapperBuilders = new HashMap<>();

            public Builder add(FieldMapper.Builder builder) {
                mapperBuilders.put(builder.name(), builder::build);
                return this;
            }

            public Builder add(FieldMapper mapper) {
                mapperBuilders.put(mapper.simpleName(), context -> mapper);
                return this;
            }

            public Builder update(FieldMapper toMerge, ContentPath contentPath) {
                if (mapperBuilders.containsKey(toMerge.simpleName()) == false) {
                    add(toMerge);
                } else {
                    FieldMapper existing = mapperBuilders.get(toMerge.simpleName()).apply(contentPath);
                    add(existing.merge(toMerge));
                }
                return this;
            }

            public boolean hasMultiFields() {
                return mapperBuilders.isEmpty() == false;
            }

            public MultiFields build(Mapper.Builder mainFieldBuilder, ContentPath contentPath) {
                if (mapperBuilders.isEmpty()) {
                    return empty();
                } else {
                    Map<String, FieldMapper> mappers = new HashMap<>();
                    contentPath.add(mainFieldBuilder.name());
                    for (Map.Entry<String, Function<ContentPath, FieldMapper>> entry : this.mapperBuilders.entrySet()) {
                        String key = entry.getKey();
                        FieldMapper mapper = entry.getValue().apply(contentPath);
                        mappers.put(key, mapper);
                    }
                    contentPath.remove();
                    return new MultiFields(Collections.unmodifiableMap(mappers));
                }
            }
        }

        private final Map<String, FieldMapper> mappers;

        private MultiFields(Map<String, FieldMapper> mappers) {
            this.mappers = mappers;
        }

        public void parse(FieldMapper mainField, DocumentParserContext context) throws IOException {
            // TODO: multi fields are really just copy fields, we just need to expose "sub fields" or something that can be part
            // of the mappings
            if (mappers.isEmpty()) {
                return;
            }

            context = context.createMultiFieldContext();

            context.path().add(mainField.simpleName());
            for (FieldMapper mapper : mappers.values()) {
                mapper.parse(context);
            }
            context.path().remove();
        }

        public MultiFields merge(MultiFields mergeWith) {
            Map<String, FieldMapper> newMappers = new HashMap<>();
            for (FieldMapper mapper : mergeWith.mappers.values()) {
                FieldMapper mergeIntoMapper = mappers.get(mapper.simpleName());
                if (mergeIntoMapper == null) {
                    newMappers.put(mapper.simpleName(), mapper);
                } else {
                    FieldMapper merged = mergeIntoMapper.merge(mapper);
                    newMappers.put(merged.simpleName(), merged); // override previous definition
                }
            }
            return new MultiFields(Collections.unmodifiableMap(newMappers));
        }

        @Override
        public Iterator<FieldMapper> iterator() {
            return mappers.values().iterator();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (mappers.isEmpty() == false) {
                // sort the mappers so we get consistent serialization format
                List<FieldMapper> sortedMappers = new ArrayList<>(mappers.values());
                sortedMappers.sort(Comparator.comparing(FieldMapper::name));
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
            if (copyToFields.isEmpty() == false) {
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

            public boolean hasValues() {
                return copyToBuilders.isEmpty() == false;
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
    public interface Serializer<T> {
        void serialize(XContentBuilder builder, String name, T value) throws IOException;
    }

    protected interface MergeValidator<T> {
        boolean canMerge(T previous, T current, Conflicts conflicts);
    }

    /**
     * Check on whether or not a parameter should be serialized
     */
    public interface SerializerCheck<T> {
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
        private final TriFunction<String, MappingParserContext, Object, T> parser;
        private final Function<FieldMapper, T> initializer;
        private boolean acceptsNull = false;
        private List<Consumer<T>> validators = new ArrayList<>();
        private Serializer<T> serializer = XContentBuilder::field;
        private SerializerCheck<T> serializerCheck = (includeDefaults, isConfigured, value) -> includeDefaults || isConfigured;
        private Function<T, String> conflictSerializer = Objects::toString;
        private boolean deprecated;
        private MergeValidator<T> mergeValidator;
        private T value;
        private boolean isSet;
        private final List<Parameter<?>> requires = new ArrayList<>();
        private final List<Parameter<?>> precludes = new ArrayList<>();

        /**
         * Creates a new Parameter
         * @param name          the parameter name, used in parsing and serialization
         * @param updateable    whether the parameter can be updated with a new value during a mapping update
         * @param defaultValue  the default value for the parameter, used if unspecified in mappings
         * @param parser        a function that converts an object to a parameter value
         * @param initializer   a function that reads a parameter value from an existing mapper
         */
        public Parameter(String name, boolean updateable, Supplier<T> defaultValue,
                         TriFunction<String, MappingParserContext, Object, T> parser, Function<FieldMapper, T> initializer) {
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

        public boolean canAcceptNull() {
            return acceptsNull;
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
         * Adds validation to a parameter, called after parsing and merging. Multiple
         * validators can be added and all of them will be executed.
         */
        public Parameter<T> addValidator(Consumer<T> validator) {
            this.validators.add(validator);
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

        public Parameter<T> requiresParameters(Parameter<?>... ps) {
            this.requires.addAll(Arrays.asList(ps));
            return this;
        }

        public Parameter<T> precludesParameters(Parameter<?>... ps) {
            this.precludes.addAll(Arrays.asList(ps));
            return this;
        }

        void validate() {
            // Iterate over the list of validators and execute them one by one.
            for (Consumer<T> v : validators) {
                v.accept(getValue());
            }
            if (this.isConfigured()) {
                for (Parameter<?> p : requires) {
                    if (p.isConfigured() == false) {
                        throw new IllegalArgumentException("Field [" + name + "] requires field [" + p.name + "] to be configured");
                    }
                }
                for (Parameter<?> p : precludes) {
                    if (p.isConfigured()) {
                        throw new IllegalArgumentException("Field [" + p.name + "] cannot be set in conjunction with field [" + name + "]");
                    }
                }
            }
        }

        private void init(FieldMapper toInit) {
            setValue(initializer.apply(toInit));
        }

        /**
         * Parse the field value from an Object
         * @param field     the field name
         * @param context   the parser context
         * @param in        the object
         */
        public void parse(String field, MappingParserContext context, Object in) {
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
                .addValidator(v -> {
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

        /**
         * Defines a script parameter
         * @param initializer   retrieves the equivalent parameter from an existing FieldMapper for use in merges
         * @return a script parameter
         */
        public static Parameter<Script> scriptParam(
            Function<FieldMapper, Script> initializer
        ) {
            return new FieldMapper.Parameter<>(
                "script",
                false,
                () -> null,
                (n, c, o) -> {
                    if (o == null) {
                        return null;
                    }
                    Script script = Script.parse(o);
                    if (script.getType() == ScriptType.STORED) {
                        throw new IllegalArgumentException("stored scripts are not supported on field [" + n + "]");
                    }
                    return script;
                },
                initializer
            ).acceptsNull();
        }

        /**
         * Defines an on_script_error parameter
         * @param initializer   retrieves the equivalent parameter from an existing FieldMapper for use in merges
         * @param dependentScriptParam the corresponding required script parameter
         * @return a new on_error_script parameter
         */
        public static Parameter<String> onScriptErrorParam(Function<FieldMapper, String> initializer,
                                                           Parameter<Script> dependentScriptParam) {
            return Parameter.restrictedStringParam(
                "on_script_error",
                true,
                initializer,
                "fail", "continue").requiresParameters(dependentScriptParam);
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
    public abstract static class Builder extends Mapper.Builder implements ToXContentFragment {

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
            for (FieldMapper subField : initializer.multiFields) {
                multiFieldsBuilder.add(subField);
            }
            return this;
        }

        private void merge(FieldMapper in, Conflicts conflicts) {
            for (Parameter<?> param : getParameters()) {
                param.merge(in, conflicts);
            }
            for (FieldMapper newSubField : in.multiFields) {
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
        public abstract FieldMapper build(ContentPath context);

        /**
         * Builds the full name of the field, taking into account parent objects
         */
        protected String buildFullName(ContentPath contentPath) {
            return contentPath.pathAsText(name);
        }

        protected void addScriptValidation(
            Parameter<Script> scriptParam,
            Parameter<Boolean> indexParam,
            Parameter<Boolean> docValuesParam
        ) {
            scriptParam.addValidator(s -> {
                if (s != null && indexParam.get() == false && docValuesParam.get() == false) {
                    throw new MapperParsingException("Cannot define script on field with index:false and doc_values:false");
                }
                if (s != null && multiFieldsBuilder.hasMultiFields()) {
                    throw new MapperParsingException("Cannot define multifields on a field with a script");
                }
                if (s != null && copyTo.hasValues()) {
                    throw new MapperParsingException("Cannot define copy_to parameter on a field with a script");
                }
            });
        }

        /**
         * Writes the current builder parameter values as XContent
         */
        @Override
        public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            boolean includeDefaults = params.paramAsBoolean("include_defaults", false);
            for (Parameter<?> parameter : getParameters()) {
                parameter.toXContent(builder, includeDefaults);
            }
            return builder;
        }

        /**
         * Parse mapping parameters from a map of mappings
         * @param name              the field mapper name
         * @param parserContext     the parser context
         * @param fieldNode         the root node of the map of mappings for this field
         */
        public final void parse(String name, MappingParserContext parserContext, Map<String, Object> fieldNode) {
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
                            DeprecationCategory.API,
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
                    deprecationLogger.deprecate(DeprecationCategory.API, propName, "Parameter [{}] on mapper [{}] is deprecated, use [{}]",
                        propName, name, parameter.name);
                } else {
                    parameter = paramsMap.get(propName);
                }
                if (parameter == null) {
                    if (isDeprecatedParameter(propName, parserContext.indexVersionCreated())) {
                        deprecationLogger.deprecate(DeprecationCategory.API, propName,
                            "Parameter [{}] has no effect on type [{}] and will be removed in future", propName, type);
                        iterator.remove();
                        continue;
                    }
                    if (parserContext.isFromDynamicTemplate() && parserContext.indexVersionCreated().before(Version.V_8_0_0)) {
                        // The parameter is unknown, but this mapping is from a dynamic template.
                        // Until 7.x it was possible to use unknown parameters there, so for bwc we need to ignore it
                        deprecationLogger.deprecate(DeprecationCategory.API, propName,
                            "Parameter [{}] is used in a dynamic template mapping and has no effect on type [{}]. "
                            + "Usage will result in an error in future major versions and should be removed.",
                            propName,
                            type
                        );
                        iterator.remove();
                        continue;
                    }
                    throw new MapperParsingException(
                        "unknown parameter [" + propName + "] on mapper [" + name + "] of type [" + type + "]"
                    );
                }
                if (parameter.deprecated) {
                    deprecationLogger.deprecate(DeprecationCategory.API, propName,
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

        protected static ContentPath parentPath(String name) {
            int endPos = name.lastIndexOf(".");
            if (endPos == -1) {
                return new ContentPath(0);
            }
            return new ContentPath(name.substring(0, endPos));
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

    public static BiConsumer<String, MappingParserContext> notInMultiFields(String type) {
        return (n, c) -> {
            if (c.isWithinMultiField()) {
                throw new MapperParsingException("Field [" + n + "] of type [" + type + "] can't be used in multifields");
            }
        };
    }

    /**
     * TypeParser implementation that automatically handles parsing
     */
    public static final class TypeParser implements Mapper.TypeParser {

        private final BiFunction<String, MappingParserContext, Builder> builderFunction;
        private final BiConsumer<String, MappingParserContext> contextValidator;

        /**
         * Creates a new TypeParser
         * @param builderFunction a function that produces a Builder from a name and parsercontext
         */
        public TypeParser(BiFunction<String, MappingParserContext, Builder> builderFunction) {
            this(builderFunction, (n, c) -> {});
        }

        public TypeParser(
            BiFunction<String, MappingParserContext, Builder> builderFunction,
            BiConsumer<String, MappingParserContext> contextValidator
        ) {
            this.builderFunction = builderFunction;
            this.contextValidator = contextValidator;
        }

        @Override
        public Builder parse(String name, Map<String, Object> node, MappingParserContext parserContext) throws MapperParsingException {
            contextValidator.accept(name, parserContext);
            Builder builder = builderFunction.apply(name, parserContext);
            builder.parse(name, parserContext, node);
            return builder;
        }
    }

}
