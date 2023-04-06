/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.support.AbstractXContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;

public abstract class FieldMapper extends Mapper implements Cloneable {
    private static final Logger logger = LogManager.getLogger(FieldMapper.class);

    public static final Setting<Boolean> IGNORE_MALFORMED_SETTING = Setting.boolSetting(
        "index.mapping.ignore_malformed",
        false,
        Property.IndexScope
    );
    public static final Setting<Boolean> COERCE_SETTING = Setting.boolSetting("index.mapping.coerce", false, Property.IndexScope);

    protected static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(FieldMapper.class);
    @SuppressWarnings("rawtypes")
    static final Parameter<?>[] EMPTY_PARAMETERS = new Parameter[0];

    protected final MappedFieldType mappedFieldType;
    protected final MultiFields multiFields;
    protected final CopyTo copyTo;
    protected final boolean hasScript;
    protected final OnScriptError onScriptError;

    /**
     * @param simpleName        the leaf name of the mapper
     * @param mappedFieldType   the MappedFieldType associated with this mapper
     * @param multiFields       sub fields of this mapper
     * @param copyTo            copyTo fields of this mapper
     */
    protected FieldMapper(String simpleName, MappedFieldType mappedFieldType, MultiFields multiFields, CopyTo copyTo) {
        this(simpleName, mappedFieldType, multiFields, copyTo, false, null);
    }

    /**
     * @param simpleName        the leaf name of the mapper
     * @param mappedFieldType   the MappedFieldType associated with this mapper
     * @param multiFields       sub fields of this mapper
     * @param copyTo            copyTo fields of this mapper
     * @param hasScript         whether a script is defined for the field
     * @param onScriptError     the behaviour for when the defined script fails at runtime
     */
    protected FieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        boolean hasScript,
        OnScriptError onScriptError
    ) {
        super(simpleName);
        // could be blank but not empty on indices created < 8.6.0
        assert mappedFieldType.name().isEmpty() == false;
        this.mappedFieldType = mappedFieldType;
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
     * Will this field ignore malformed values for this field and accept the
     * document ({@code true}) or will it reject documents with malformed
     * values for this field ({@code false}). Some fields don't have a concept
     * of "malformed" and will return {@code false} here.
     */
    public boolean ignoreMalformed() {
        return false;
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
                throwIndexingWithScriptParam();
            }
            parseCreateField(context);
        } catch (Exception e) {
            rethrowAsDocumentParsingException(context, e);
        }
        // TODO: multi fields are really just copy fields, we just need to expose "sub fields" or something that can be part
        // of the mappings
        if (multiFields.mappers.length != 0) {
            doParseMultiFields(context);
        }
    }

    private void doParseMultiFields(DocumentParserContext context) throws IOException {
        context.path().add(simpleName());
        for (FieldMapper mapper : multiFields.mappers) {
            mapper.parse(context);
        }
        context.path().remove();
    }

    private static void throwIndexingWithScriptParam() {
        throw new IllegalArgumentException("Cannot index data directly into a field with a [script] parameter");
    }

    private void rethrowAsDocumentParsingException(DocumentParserContext context, Exception e) {
        String valuePreview;
        try {
            XContentParser parser = context.parser();
            Object complexValue = AbstractXContentParser.readValue(parser, HashMap::new);
            if (complexValue == null) {
                valuePreview = "null";
            } else {
                valuePreview = complexValue.toString();
            }
        } catch (Exception innerException) {
            throw new DocumentParsingException(
                context.parser().getTokenLocation(),
                String.format(
                    Locale.ROOT,
                    "failed to parse field [%s] of type [%s] in %s. Could not parse field value preview,",
                    fieldType().name(),
                    fieldType().typeName(),
                    context.documentDescription()
                ),
                e
            );
        }

        throw new DocumentParsingException(
            context.parser().getTokenLocation(),
            String.format(
                Locale.ROOT,
                "failed to parse field [%s] of type [%s] in %s. Preview of field's value: '%s'",
                fieldType().name(),
                fieldType().typeName(),
                context.documentDescription(),
                valuePreview
            ),
            e
        );
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
    public final void executeScript(
        SearchLookup searchLookup,
        LeafReaderContext readerContext,
        int doc,
        DocumentParserContext documentParserContext
    ) {
        try {
            indexScriptValues(searchLookup, readerContext, doc, documentParserContext);
        } catch (Exception e) {
            if (onScriptError == OnScriptError.CONTINUE) {
                documentParserContext.addIgnoredField(name());
            } else {
                throw new DocumentParsingException(XContentLocation.UNKNOWN, "Error executing script on field [" + name() + "]", e);
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
    protected void indexScriptValues(
        SearchLookup searchLookup,
        LeafReaderContext readerContext,
        int doc,
        DocumentParserContext documentParserContext
    ) {
        throw new UnsupportedOperationException("FieldMapper " + name() + " does not support [script]");
    }

    @Override
    public Iterator<Mapper> iterator() {
        return Iterators.forArray(multiFields.mappers);
    }

    @Override
    public final void validate(MappingLookup mappers) {
        if (this.copyTo() != null && this.copyTo().copyToFields().isEmpty() == false) {
            if (mappers.isMultiField(this.name())) {
                throw new IllegalArgumentException("[copy_to] may not be used to copy from a multi-field: [" + this.name() + "]");
            }

            final String sourceScope = mappers.nestedLookup().getNestedParent(this.name());
            for (String copyTo : this.copyTo().copyToFields()) {
                if (mappers.isMultiField(copyTo)) {
                    throw new IllegalArgumentException("[copy_to] may not be used to copy to a multi-field: [" + copyTo + "]");
                }
                if (mappers.isObjectField(copyTo)) {
                    throw new IllegalArgumentException("Cannot copy to field [" + copyTo + "] since it is mapped as an object");
                }

                final String targetScope = mappers.nestedLookup().getNestedParent(copyTo);
                checkNestedScopeCompatibility(sourceScope, targetScope);
            }
        }
        for (Mapper multiField : multiFields().mappers) {
            multiField.validate(mappers);
        }
        doValidate(mappers);
    }

    protected void doValidate(MappingLookup mappers) {}

    private static void checkNestedScopeCompatibility(String source, String target) {
        boolean targetIsParentOfSource;
        if (source == null || target == null) {
            targetIsParentOfSource = target == null;
        } else {
            targetIsParentOfSource = source.equals(target) || source.startsWith(target + ".");
        }
        if (targetIsParentOfSource == false) {
            throw new IllegalArgumentException(
                "Illegal combination of [copy_to] and [nested] mappings: [copy_to] may only copy data to the current nested "
                    + "document or any of its parents, however one [copy_to] directive is trying to copy data from nested object ["
                    + source
                    + "] to ["
                    + target
                    + "]"
            );
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
    public final FieldMapper merge(Mapper mergeWith, MapperBuilderContext mapperBuilderContext) {
        if (mergeWith == this) {
            return this;
        }
        if (mergeWith instanceof FieldMapper == false) {
            throw new IllegalArgumentException(
                "mapper ["
                    + name()
                    + "] cannot be changed from type ["
                    + contentType()
                    + "] to ["
                    + mergeWith.getClass().getSimpleName()
                    + "]"
            );
        }
        checkIncomingMergeType((FieldMapper) mergeWith);

        Builder builder = getMergeBuilder();
        if (builder == null) {
            return (FieldMapper) mergeWith;
        }
        Conflicts conflicts = new Conflicts(name());
        builder.merge((FieldMapper) mergeWith, conflicts, mapperBuilderContext);
        conflicts.check();
        return builder.build(mapperBuilderContext);
    }

    protected void checkIncomingMergeType(FieldMapper mergeWith) {
        if (Objects.equals(this.getClass(), mergeWith.getClass()) == false) {
            throw new IllegalArgumentException(
                "mapper [" + name() + "] cannot be changed from type [" + contentType() + "] to [" + mergeWith.contentType() + "]"
            );
        }
        if (Objects.equals(contentType(), mergeWith.contentType()) == false) {
            throw new IllegalArgumentException(
                "mapper [" + name() + "] cannot be changed from type [" + contentType() + "] to [" + mergeWith.contentType() + "]"
            );
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
        copyTo.toXContent(builder);
    }

    protected abstract String contentType();

    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of();
    }

    public static final class MultiFields implements Iterable<FieldMapper>, ToXContent {

        private static final MultiFields EMPTY = new MultiFields(new FieldMapper[0]);

        public static MultiFields empty() {
            return EMPTY;
        }

        public static class Builder {

            private final Map<String, Function<MapperBuilderContext, FieldMapper>> mapperBuilders = new HashMap<>();

            public Builder add(FieldMapper.Builder builder) {
                mapperBuilders.put(builder.name(), builder::build);
                return this;
            }

            public Builder add(FieldMapper mapper) {
                mapperBuilders.put(mapper.simpleName(), context -> mapper);
                return this;
            }

            public Builder update(FieldMapper toMerge, MapperBuilderContext context) {
                if (mapperBuilders.containsKey(toMerge.simpleName()) == false) {
                    add(toMerge);
                } else {
                    FieldMapper existing = mapperBuilders.get(toMerge.simpleName()).apply(context);
                    add(existing.merge(toMerge, context));
                }
                return this;
            }

            public boolean hasMultiFields() {
                return mapperBuilders.isEmpty() == false;
            }

            public MultiFields build(Mapper.Builder mainFieldBuilder, MapperBuilderContext context) {
                if (mapperBuilders.isEmpty()) {
                    return empty();
                } else {
                    FieldMapper[] mappers = new FieldMapper[mapperBuilders.size()];
                    context = context.createChildContext(mainFieldBuilder.name());
                    int i = 0;
                    for (Map.Entry<String, Function<MapperBuilderContext, FieldMapper>> entry : this.mapperBuilders.entrySet()) {
                        mappers[i++] = entry.getValue().apply(context);
                    }
                    return new MultiFields(mappers);
                }
            }
        }

        private final FieldMapper[] mappers;

        private MultiFields(FieldMapper[] mappers) {
            this.mappers = mappers;
            // sort for consistent iteration order + serialization
            Arrays.sort(this.mappers, Comparator.comparing(FieldMapper::name));
        }

        public void parse(FieldMapper mainField, DocumentParserContext context, Supplier<DocumentParserContext> multiFieldContextSupplier)
            throws IOException {
            // TODO: multi fields are really just copy fields, we just need to expose "sub fields" or something that can be part
            // of the mappings
            if (mappers.length == 0) {
                return;
            }
            context.path().add(mainField.simpleName());
            for (FieldMapper mapper : mappers) {
                mapper.parse(multiFieldContextSupplier.get());
            }
            context.path().remove();
        }

        @Override
        public Iterator<FieldMapper> iterator() {
            return Iterators.forArray(mappers);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (mappers.length != 0) {
                builder.startObject("fields");
                for (Mapper mapper : mappers) {
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

        public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
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
        private List<String> deprecatedNames = List.of();
        private final Supplier<T> defaultValue;
        private final TriFunction<String, MappingParserContext, Object, T> parser;
        private final Function<FieldMapper, T> initializer;
        private boolean acceptsNull = false;
        private Consumer<T> validator;
        private final Serializer<T> serializer;
        private SerializerCheck<T> serializerCheck = (includeDefaults, isConfigured, value) -> includeDefaults || isConfigured;
        private final Function<T, String> conflictSerializer;
        private boolean deprecated;
        private MergeValidator<T> mergeValidator;
        private T value;
        private boolean isSet;
        private List<Parameter<?>> requires = List.of();
        private List<Parameter<?>> precludes = List.of();

        /**
         * Creates a new Parameter
         * @param name          the parameter name, used in parsing and serialization
         * @param updateable    whether the parameter can be updated with a new value during a mapping update
         * @param defaultValue  the default value for the parameter, used if unspecified in mappings
         * @param parser        a function that converts an object to a parameter value
         * @param initializer   a function that reads a parameter value from an existing mapper
         * @param serializer    a function that serializes a parameter value, prefer type specific x-content generation methods here for
         *                      good performance as this is used on the hot-path during cluster state updates.
         *                      This should explicitly not be linked with {@link XContentBuilder#field(String, Object)} by callers through
         *                      the use of default values or other indirection to this constructor.
         * @param conflictSerializer a function that serializes a parameter value on conflict
         */
        public Parameter(
            String name,
            boolean updateable,
            Supplier<T> defaultValue,
            TriFunction<String, MappingParserContext, Object, T> parser,
            Function<FieldMapper, T> initializer,
            Serializer<T> serializer,
            Function<T, String> conflictSerializer
        ) {
            this.name = name;
            this.defaultValue = Objects.requireNonNull(defaultValue);
            this.value = null;
            this.parser = parser;
            this.initializer = initializer;
            this.mergeValidator = updateable
                ? (previous, toMerge, conflicts) -> true
                : (previous, toMerge, conflicts) -> Objects.equals(previous, toMerge);
            this.serializer = serializer;
            this.conflictSerializer = conflictSerializer;
        }

        /**
         * Returns the current value of the parameter
         */
        public T getValue() {
            return isSet ? value : getDefaultValue();
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
            return isSet && Objects.equals(value, getDefaultValue()) == false;
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
            this.deprecatedNames = CollectionUtils.appendToCopyNoNullElements(this.deprecatedNames, deprecatedName);
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
            this.validator = this.validator == null ? validator : this.validator.andThen(validator);
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

        public Parameter<T> requiresParameter(Parameter<?> ps) {
            this.requires = CollectionUtils.appendToCopyNoNullElements(this.requires, ps);
            return this;
        }

        public Parameter<T> precludesParameters(Parameter<?>... ps) {
            this.precludes = CollectionUtils.appendToCopyNoNullElements(this.precludes, ps);
            return this;
        }

        void validate() {
            // Iterate over the list of validators and execute them one by one.
            if (validator != null) {
                validator.accept(getValue());
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
            T value = getValue();
            if (serializerCheck.check(includeDefaults, isConfigured(), value)) {
                serializer.serialize(builder, name, value);
            }
        }

        /**
         * Defines a parameter that takes the values {@code true} or {@code false}
         * @param name          the parameter name
         * @param updateable    whether the parameter can be changed by a mapping update
         * @param initializer   a function that reads the parameter value from an existing mapper
         * @param defaultValue  the default value, to be used if the parameter is undefined in a mapping
         */
        public static Parameter<Boolean> boolParam(
            String name,
            boolean updateable,
            Function<FieldMapper, Boolean> initializer,
            boolean defaultValue
        ) {
            return new Parameter<>(
                name,
                updateable,
                defaultValue ? () -> true : () -> false,
                (n, c, o) -> XContentMapValues.nodeBooleanValue(o),
                initializer,
                XContentBuilder::field,
                Objects::toString
            );
        }

        public static Parameter<Boolean> boolParam(
            String name,
            boolean updateable,
            Function<FieldMapper, Boolean> initializer,
            Supplier<Boolean> defaultValue
        ) {
            return new Parameter<>(
                name,
                updateable,
                defaultValue,
                (n, c, o) -> XContentMapValues.nodeBooleanValue(o),
                initializer,
                XContentBuilder::field,
                Objects::toString
            );
        }

        /**
         * Defines a parameter that takes the values {@code true} or {@code false}, and will always serialize
         * its value if configured.
         * @param name          the parameter name
         * @param updateable    whether the parameter can be changed by a mapping update
         * @param initializer   a function that reads the parameter value from an existing mapper
         * @param defaultValue  the default value, to be used if the parameter is undefined in a mapping
         */
        public static Parameter<Explicit<Boolean>> explicitBoolParam(
            String name,
            boolean updateable,
            Function<FieldMapper, Explicit<Boolean>> initializer,
            boolean defaultValue
        ) {
            return new Parameter<>(
                name,
                updateable,
                defaultValue ? () -> Explicit.IMPLICIT_TRUE : () -> Explicit.IMPLICIT_FALSE,
                (n, c, o) -> Explicit.explicitBoolean(XContentMapValues.nodeBooleanValue(o)),
                initializer,
                (b, n, v) -> b.field(n, v.value()),
                v -> Boolean.toString(v.value())
            );
        }

        /**
         * Defines a parameter that takes an integer value
         * @param name          the parameter name
         * @param updateable    whether the parameter can be changed by a mapping update
         * @param initializer   a function that reads the parameter value from an existing mapper
         * @param defaultValue  the default value, to be used if the parameter is undefined in a mapping
         */
        public static Parameter<Integer> intParam(
            String name,
            boolean updateable,
            Function<FieldMapper, Integer> initializer,
            int defaultValue
        ) {
            return new Parameter<>(
                name,
                updateable,
                () -> defaultValue,
                (n, c, o) -> XContentMapValues.nodeIntegerValue(o),
                initializer,
                XContentBuilder::field,
                Objects::toString
            );
        }

        /**
         * Defines a parameter that takes a string value
         * @param name          the parameter name
         * @param updateable    whether the parameter can be changed by a mapping update
         * @param initializer   a function that reads the parameter value from an existing mapper
         * @param defaultValue  the default value, to be used if the parameter is undefined in a mapping
         */
        public static Parameter<String> stringParam(
            String name,
            boolean updateable,
            Function<FieldMapper, String> initializer,
            String defaultValue
        ) {
            return stringParam(name, updateable, initializer, defaultValue, XContentBuilder::field);
        }

        public static Parameter<String> stringParam(
            String name,
            boolean updateable,
            Function<FieldMapper, String> initializer,
            String defaultValue,
            Serializer<String> serializer
        ) {
            return new Parameter<>(
                name,
                updateable,
                defaultValue == null ? () -> null : () -> defaultValue,
                (n, c, o) -> XContentMapValues.nodeStringValue(o),
                initializer,
                serializer,
                Function.identity()
            );
        }

        @SuppressWarnings("unchecked")
        public static Parameter<List<String>> stringArrayParam(
            String name,
            boolean updateable,
            Function<FieldMapper, List<String>> initializer
        ) {
            return new Parameter<>(name, updateable, List::of, (n, c, o) -> {
                List<Object> values = (List<Object>) o;
                List<String> strValues = new ArrayList<>();
                for (Object item : values) {
                    strValues.add(item.toString());
                }
                return strValues;
            }, initializer, XContentBuilder::stringListField, Objects::toString);
        }

        /**
         * Defines a parameter that takes any of the values of an enumeration.
         *
         * @param name          the parameter name
         * @param updateable    whether the parameter can be changed by a mapping update
         * @param initializer   a function that reads the parameter value from an existing mapper
         * @param defaultValue  the default value, to be used if the parameter is undefined in a mapping
         * @param enumClass     the enumeration class the parameter takes values from
         */
        public static <T extends Enum<T>> Parameter<T> enumParam(
            String name,
            boolean updateable,
            Function<FieldMapper, T> initializer,
            T defaultValue,
            Class<T> enumClass
        ) {
            Set<T> acceptedValues = EnumSet.allOf(enumClass);
            return restrictedEnumParam(name, updateable, initializer, defaultValue, enumClass, acceptedValues);
        }

        /**
         * Defines a parameter that takes one of a restricted set of values from an enumeration.
         *
         * @param name            the parameter name
         * @param updateable      whether the parameter can be changed by a mapping update
         * @param initializer     a function that reads the parameter value from an existing mapper
         * @param defaultValue    the default value, to be used if the parameter is undefined in a mapping
         * @param enumClass       the enumeration class the parameter takes values from
         * @param acceptedValues  the set of values that the parameter can take
         */
        public static <T extends Enum<T>> Parameter<T> restrictedEnumParam(
            String name,
            boolean updateable,
            Function<FieldMapper, T> initializer,
            T defaultValue,
            Class<T> enumClass,
            Set<T> acceptedValues
        ) {
            assert acceptedValues.size() > 0;
            return new Parameter<>(name, updateable, () -> defaultValue, (n, c, o) -> {
                if (o == null) {
                    return defaultValue;
                }
                EnumSet<T> enumSet = EnumSet.allOf(enumClass);
                for (T t : enumSet) {
                    // the string representation may differ from the actual name of the enum type (e.g. lowercase vs uppercase)
                    if (t.toString().equals(o.toString())) {
                        return t;
                    }
                }
                throw new MapperParsingException(
                    "Unknown value [" + o + "] for field [" + name + "] - accepted values are " + acceptedValues
                );
            }, initializer, XContentBuilder::field, Objects::toString).addValidator(v -> {
                if (v != null && acceptedValues.contains(v) == false) {
                    throw new MapperParsingException(
                        "Unknown value [" + v + "] for field [" + name + "] - accepted values are " + acceptedValues
                    );
                }
            });
        }

        /**
         * Defines a parameter that takes an analyzer name
         * @param name              the parameter name
         * @param updateable        whether the parameter can be changed by a mapping update
         * @param initializer       a function that reads the parameter value from an existing mapper
         * @param defaultAnalyzer   the default value, to be used if the parameter is undefined in a mapping
         * @param indexCreatedVersion the version on which this index was created
         */
        public static Parameter<NamedAnalyzer> analyzerParam(
            String name,
            boolean updateable,
            Function<FieldMapper, NamedAnalyzer> initializer,
            Supplier<NamedAnalyzer> defaultAnalyzer,
            Version indexCreatedVersion
        ) {
            return new Parameter<>(name, updateable, defaultAnalyzer, (n, c, o) -> {
                String analyzerName = o.toString();
                NamedAnalyzer a = c.getIndexAnalyzers().get(analyzerName);
                if (a == null) {
                    if (indexCreatedVersion.isLegacyIndexVersion()) {
                        logger.warn(() -> format("Could not find analyzer [%s] of legacy index, falling back to default", analyzerName));
                        a = defaultAnalyzer.get();
                    } else {
                        throw new IllegalArgumentException("analyzer [" + analyzerName + "] has not been configured in mappings");
                    }
                }
                return a;
            }, initializer, (b, n, v) -> b.field(n, v.name()), NamedAnalyzer::name);
        }

        /**
         * Defines a parameter that takes an analyzer name
         * @param name              the parameter name
         * @param updateable        whether the parameter can be changed by a mapping update
         * @param initializer       a function that reads the parameter value from an existing mapper
         * @param defaultAnalyzer   the default value, to be used if the parameter is undefined in a mapping
         */
        public static Parameter<NamedAnalyzer> analyzerParam(
            String name,
            boolean updateable,
            Function<FieldMapper, NamedAnalyzer> initializer,
            Supplier<NamedAnalyzer> defaultAnalyzer
        ) {
            return analyzerParam(name, updateable, initializer, defaultAnalyzer, Version.CURRENT);
        }

        /**
         * Declares a metadata parameter
         */
        public static Parameter<Map<String, String>> metaParam() {
            return new Parameter<>(
                "meta",
                true,
                Collections::emptyMap,
                (n, c, o) -> TypeParsers.parseMeta(n, o),
                m -> m.fieldType().meta(),
                XContentBuilder::stringStringMap,
                Objects::toString
            );
        }

        public static Parameter<Boolean> indexParam(Function<FieldMapper, Boolean> initializer, boolean defaultValue) {
            return Parameter.boolParam("index", false, initializer, defaultValue);
        }

        public static Parameter<Boolean> indexParam(Function<FieldMapper, Boolean> initializer, Supplier<Boolean> defaultValue) {
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
        public static Parameter<Script> scriptParam(Function<FieldMapper, Script> initializer) {
            return new FieldMapper.Parameter<>("script", false, () -> null, (n, c, o) -> {
                if (o == null) {
                    return null;
                }
                Script script = Script.parse(o);
                if (script.getType() == ScriptType.STORED) {
                    throw new IllegalArgumentException("stored scripts are not supported on field [" + n + "]");
                }
                return script;
            }, initializer, XContentBuilder::field, Objects::toString).acceptsNull();
        }

        /**
         * Defines an on_script_error parameter
         * @param initializer   retrieves the equivalent parameter from an existing FieldMapper for use in merges
         * @param dependentScriptParam the corresponding required script parameter
         * @return a new on_error_script parameter
         */
        public static Parameter<OnScriptError> onScriptErrorParam(
            Function<FieldMapper, OnScriptError> initializer,
            Parameter<Script> dependentScriptParam
        ) {
            return Parameter.enumParam("on_script_error", true, initializer, OnScriptError.FAIL, OnScriptError.class)
                .requiresParameter(dependentScriptParam);
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
            String message = "Mapper for [" + mapperName + "] conflicts with existing mapper:\n\t" + String.join("\n\t", conflicts);
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
            for (FieldMapper subField : initializer.multiFields.mappers) {
                multiFieldsBuilder.add(subField);
            }
            return this;
        }

        protected void merge(FieldMapper in, Conflicts conflicts, MapperBuilderContext mapperBuilderContext) {
            for (Parameter<?> param : getParameters()) {
                param.merge(in, conflicts);
            }
            MapperBuilderContext childContext = mapperBuilderContext.createChildContext(in.simpleName());
            for (FieldMapper newSubField : in.multiFields.mappers) {
                multiFieldsBuilder.update(newSubField, childContext);
            }
            this.copyTo.reset(in.copyTo);
            validate();
        }

        protected final void validate() {
            for (Parameter<?> param : getParameters()) {
                param.validate();
            }
        }

        /**
         * @return the list of parameters defined for this mapper
         */
        protected abstract Parameter<?>[] getParameters();

        @Override
        public abstract FieldMapper build(MapperBuilderContext context);

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
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
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
            final Parameter<?>[] params = getParameters();
            // we know the paramsMap size up-front
            Map<String, Parameter<?>> paramsMap = Maps.newHashMapWithExpectedSize(params.length);
            // don't know this map's size up-front
            Map<String, Parameter<?>> deprecatedParamsMap = new HashMap<>();
            for (Parameter<?> param : params) {
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
                switch (propName) {
                    case "fields" -> {
                        TypeParsers.parseMultiField(multiFieldsBuilder::add, name, parserContext, propName, propNode);
                        iterator.remove();
                        continue;
                    }
                    case "copy_to" -> {
                        TypeParsers.parseCopyFields(propNode).forEach(copyTo::add);
                        iterator.remove();
                        continue;
                    }
                    case "boost" -> {
                        if (parserContext.indexVersionCreated().onOrAfter(Version.V_8_0_0)) {
                            throw new MapperParsingException("Unknown parameter [boost] on mapper [" + name + "]");
                        }
                        deprecationLogger.warn(
                            DeprecationCategory.API,
                            "boost",
                            "Parameter [boost] on field [{}] is deprecated and has no effect",
                            name
                        );
                        iterator.remove();
                        continue;
                    }
                }
                Parameter<?> parameter = deprecatedParamsMap.get(propName);
                if (parameter != null) {
                    deprecationLogger.warn(
                        DeprecationCategory.API,
                        propName,
                        "Parameter [{}] on mapper [{}] is deprecated, use [{}]",
                        propName,
                        name,
                        parameter.name
                    );
                } else {
                    parameter = paramsMap.get(propName);
                }
                if (parameter == null) {
                    if (parserContext.indexVersionCreated().isLegacyIndexVersion()) {
                        // ignore unknown parameters on legacy indices
                        handleUnknownParamOnLegacyIndex(propName, propNode);
                        iterator.remove();
                        continue;
                    }
                    if (isDeprecatedParameter(propName, parserContext.indexVersionCreated())) {
                        deprecationLogger.warn(
                            DeprecationCategory.API,
                            propName,
                            "Parameter [{}] has no effect on type [{}] and will be removed in future",
                            propName,
                            type
                        );
                        iterator.remove();
                        continue;
                    }
                    if (parserContext.isFromDynamicTemplate() && parserContext.indexVersionCreated().before(Version.V_8_0_0)) {
                        // The parameter is unknown, but this mapping is from a dynamic template.
                        // Until 7.x it was possible to use unknown parameters there, so for bwc we need to ignore it
                        deprecationLogger.warn(
                            DeprecationCategory.API,
                            propName,
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
                    deprecationLogger.warn(
                        DeprecationCategory.API,
                        propName,
                        "Parameter [{}] is deprecated and will be removed in a future version",
                        propName
                    );
                }
                if (propNode == null && parameter.acceptsNull == false) {
                    throw new MapperParsingException(
                        "[" + propName + "] on mapper [" + name + "] of type [" + type + "] must not have a [null] value"
                    );
                }
                parameter.parse(name, parserContext, propNode);
                iterator.remove();
            }
            validate();
        }

        protected void handleUnknownParamOnLegacyIndex(String propName, Object propNode) {
            // ignore
        }

        // These parameters were previously *always* parsed by TypeParsers#parseField(), even if they
        // made no sense; if we've got here, that means that they're not declared on a current mapper,
        // and so we emit a deprecation warning rather than failing a previously working mapping.
        private static final Set<String> DEPRECATED_PARAMS = Set.of("store", "meta", "index", "doc_values", "index_options", "similarity");

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
        private final Version minimumCompatibilityVersion; // see Mapper.TypeParser#supportsVersion()

        /**
         * Creates a new TypeParser
         * @param builderFunction a function that produces a Builder from a name and parsercontext
         */
        public TypeParser(BiFunction<String, MappingParserContext, Builder> builderFunction) {
            this(builderFunction, (n, c) -> {}, Version.CURRENT.minimumIndexCompatibilityVersion());
        }

        /**
         * Variant of {@link #TypeParser(BiFunction)} that allows to defining a minimumCompatibilityVersion to
         * allow parsing mapping definitions of legacy indices (see {@link Mapper.TypeParser#supportsVersion(Version)}).
         */
        public TypeParser(BiFunction<String, MappingParserContext, Builder> builderFunction, Version minimumCompatibilityVersion) {
            this(builderFunction, (n, c) -> {}, minimumCompatibilityVersion);
        }

        public TypeParser(
            BiFunction<String, MappingParserContext, Builder> builderFunction,
            BiConsumer<String, MappingParserContext> contextValidator
        ) {
            this(builderFunction, contextValidator, Version.CURRENT.minimumIndexCompatibilityVersion());
        }

        private TypeParser(
            BiFunction<String, MappingParserContext, Builder> builderFunction,
            BiConsumer<String, MappingParserContext> contextValidator,
            Version minimumCompatibilityVersion
        ) {
            this.builderFunction = builderFunction;
            this.contextValidator = contextValidator;
            this.minimumCompatibilityVersion = minimumCompatibilityVersion;
        }

        @Override
        public Builder parse(String name, Map<String, Object> node, MappingParserContext parserContext) throws MapperParsingException {
            contextValidator.accept(name, parserContext);
            Builder builder = builderFunction.apply(name, parserContext);
            builder.parse(name, parserContext, node);
            return builder;
        }

        @Override
        public boolean supportsVersion(Version indexCreatedVersion) {
            return indexCreatedVersion.onOrAfter(minimumCompatibilityVersion);
        }
    }

}
