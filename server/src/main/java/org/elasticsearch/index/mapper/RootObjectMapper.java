/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.mapper.DynamicTemplate.XContentFieldType;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.ObjectMapper.TypeParser.parseObjectOrDocumentTypeProperties;
import static org.elasticsearch.index.mapper.ObjectMapper.TypeParser.parseSubobjects;
import static org.elasticsearch.index.mapper.TypeParsers.parseDateTimeFormatter;

public class RootObjectMapper extends ObjectMapper {
    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(RootObjectMapper.class);
    private static final int MAX_NESTING_LEVEL_FOR_PASS_THROUGH_OBJECTS = 20;

    public static class Defaults {
        public static final Explicit<DateFormatter[]> DYNAMIC_DATE_TIME_FORMATTERS = new Explicit<>(
            new DateFormatter[] {
                DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
                DateFormatter.forPattern("yyyy/MM/dd HH:mm:ss||yyyy/MM/dd||epoch_millis") },
            false
        );
        public static final Explicit<Boolean> DATE_DETECTION = Explicit.IMPLICIT_TRUE;
        public static final Explicit<Boolean> NUMERIC_DETECTION = Explicit.IMPLICIT_FALSE;
        private static final Explicit<DynamicTemplate[]> DYNAMIC_TEMPLATES = new Explicit<>(new DynamicTemplate[0], false);
    }

    public static class Builder extends ObjectMapper.Builder {
        protected Explicit<DynamicTemplate[]> dynamicTemplates = Defaults.DYNAMIC_TEMPLATES;
        protected Explicit<DateFormatter[]> dynamicDateTimeFormatters = Defaults.DYNAMIC_DATE_TIME_FORMATTERS;

        protected final Map<String, RuntimeField> runtimeFields = new HashMap<>();
        protected Explicit<Boolean> dateDetection = Defaults.DATE_DETECTION;
        protected Explicit<Boolean> numericDetection = Defaults.NUMERIC_DETECTION;
        protected RootObjectMapperNamespaceValidator namespaceValidator;
        protected boolean sliceEnabled;

        public Builder(String name) {
            this(name, ObjectMapper.Defaults.SUBOBJECTS);
        }

        public Builder(String name, Explicit<Subobjects> subobjects) {
            super(name, subobjects);
        }

        public Builder addNamespaceValidator(RootObjectMapperNamespaceValidator namespaceValidator) {
            this.namespaceValidator = namespaceValidator;
            return this;
        }

        public Builder setSliceEnabled(boolean sliceEnabled) {
            this.sliceEnabled = sliceEnabled;
            return this;
        }

        public Builder dynamicDateTimeFormatter(Collection<DateFormatter> dateTimeFormatters) {
            this.dynamicDateTimeFormatters = new Explicit<>(dateTimeFormatters.toArray(new DateFormatter[0]), true);
            return this;
        }

        public Builder dynamicTemplates(Collection<DynamicTemplate> templates) {
            this.dynamicTemplates = new Explicit<>(templates.toArray(new DynamicTemplate[0]), true);
            return this;
        }

        @Override
        public RootObjectMapper.Builder add(Mapper.Builder builder) {
            super.add(builder);
            return this;
        }

        public RootObjectMapper.Builder addRuntimeField(RuntimeField runtimeField) {
            this.runtimeFields.put(runtimeField.name(), runtimeField);
            return this;
        }

        public RootObjectMapper.Builder addRuntimeFields(Map<String, RuntimeField> runtimeFields) {
            this.runtimeFields.putAll(runtimeFields);
            return this;
        }

        @Override
        RootObjectMapper.Builder newEmptyBuilder() {
            RootObjectMapper.Builder builder = new RootObjectMapper.Builder(leafName(), subobjects);
            builder.enabled = this.enabled;
            builder.dynamic = this.dynamic;
            builder.sourceKeepMode = this.sourceKeepMode;
            builder.namespaceValidator = this.namespaceValidator;
            builder.sliceEnabled = this.sliceEnabled;
            return builder;
        }

        @Override
        public Mapper.Builder mergeWith(Mapper.Builder incoming, MapperMergeContext parentContext) {
            MapperMergeContext objectMergeContext = parentContext.createChildContext(null, this.dynamic);
            if (incoming instanceof ObjectMapper.Builder incomingObj) {
                this.merge(incomingObj, objectMergeContext, leafName());
                return this;
            }
            MapperErrors.throwObjectMappingConflictError(parentContext.getMapperBuilderContext().buildFullName(incoming.leafName()));
            return null;
        }

        @Override
        void merge(ObjectMapper.Builder mergeWith, MapperMergeContext objectMergeContext, String fullPath) {
            if (objectMergeContext.getMapperBuilderContext().isStrictColumnar()
                && mergeWith.enabled.explicit()
                && mergeWith.enabled.value() == false) {
                throw new MapperParsingException(
                    "[enabled] cannot be set to [false] on the root object in strict columnar index modes;"
                        + " this would prevent all fields from being indexed"
                );
            }
            super.merge(mergeWith, objectMergeContext, fullPath);
            if (mergeWith instanceof RootObjectMapper.Builder rootMergeWith) {
                if (rootMergeWith.numericDetection.explicit()) {
                    this.numericDetection = rootMergeWith.numericDetection;
                }
                if (rootMergeWith.dateDetection.explicit()) {
                    this.dateDetection = rootMergeWith.dateDetection;
                }
                if (rootMergeWith.dynamicDateTimeFormatters.explicit()) {
                    this.dynamicDateTimeFormatters = rootMergeWith.dynamicDateTimeFormatters;
                }
                if (rootMergeWith.dynamicTemplates.explicit()) {
                    if (objectMergeContext.getMapperBuilderContext().getMergeReason() == MergeReason.INDEX_TEMPLATE) {
                        Map<String, DynamicTemplate> templatesByKey = new LinkedHashMap<>();
                        for (DynamicTemplate template : this.dynamicTemplates.value()) {
                            templatesByKey.put(template.name(), template);
                        }
                        for (DynamicTemplate template : rootMergeWith.dynamicTemplates.value()) {
                            templatesByKey.put(template.name(), template);
                        }
                        DynamicTemplate[] mergedTemplates = templatesByKey.values().toArray(new DynamicTemplate[0]);
                        this.dynamicTemplates = new Explicit<>(mergedTemplates, true);
                    } else {
                        this.dynamicTemplates = rootMergeWith.dynamicTemplates;
                    }
                }
                for (Map.Entry<String, RuntimeField> runtimeField : rootMergeWith.runtimeFields.entrySet()) {
                    if (runtimeField.getValue() == null) {
                        this.runtimeFields.remove(runtimeField.getKey());
                    } else if (this.runtimeFields.containsKey(runtimeField.getKey())) {
                        this.runtimeFields.put(runtimeField.getKey(), runtimeField.getValue());
                    } else if (objectMergeContext.decrementFieldBudgetIfPossible(1)) {
                        this.runtimeFields.put(runtimeField.getValue().name(), runtimeField.getValue());
                    }
                }
                // Transfer per-prefix settings parsed from the stored source (prefix_properties key).
                // When re-parsing a stored flat mapping there are no ObjectMapper.Builder children for
                // asFlattenedFieldBuilders to harvest, so processField writes directly into the incoming
                // builder's map; we merge them here to ensure they are not lost.
                for (Map.Entry<String, PrefixProperties> e : rootMergeWith.prefixProperties.entrySet()) {
                    this.prefixProperties.merge(e.getKey(), e.getValue(), PrefixProperties::merge);
                }
            }
        }

        @Override
        public RootObjectMapper build(MapperBuilderContext context) {
            if (context.isStrictColumnar() && enabled.explicit() && enabled.value() == false) {
                throw new MapperParsingException(
                    "[enabled] cannot be set to [false] on the root object in strict columnar index modes;"
                        + " this would prevent all fields from being indexed"
                );
            }
            // Build child mappers first so that flattenBuildersIfNeeded has a chance to populate
            // prefixProperties before we pass them to the RootObjectMapper constructor.
            Map<String, Mapper> mappers = buildMappers(context.createChildContext(null, dynamic));
            return new RootObjectMapper(
                leafName(),
                enabled,
                subobjects,
                sourceKeepMode,
                dynamic,
                mappers,
                prefixProperties,
                new HashMap<>(runtimeFields),
                dynamicDateTimeFormatters,
                dynamicTemplates,
                dateDetection,
                numericDetection,
                namespaceValidator,
                sliceEnabled
            );
        }
    }

    private final Explicit<DateFormatter[]> dynamicDateTimeFormatters;
    private final Explicit<Boolean> dateDetection;
    private final Explicit<Boolean> numericDetection;
    private final Explicit<DynamicTemplate[]> dynamicTemplates;
    private final Map<String, RuntimeField> runtimeFields;
    private final RootObjectMapperNamespaceValidator namespaceValidator;
    private final boolean sliceEnabled;
    /**
     * Per-prefix settings captured during auto-flattening in strict columnar mode.
     * Keys are the full dotted paths of declared object mappers (e.g. {@code "attributes"},
     * {@code "resource.sub"}); values hold any combination of {@link PrefixProperties#dynamic},
     * {@link PrefixProperties#passthrough}, and {@link PrefixProperties#enabled} settings for
     * that prefix. Kept in sorted key order for longest-prefix resolution and deterministic
     * serialization. Empty for non-strict-columnar indices.
     */
    private final NavigableMap<String, PrefixProperties> prefixProperties;

    RootObjectMapper(
        String name,
        Explicit<Boolean> enabled,
        Explicit<Subobjects> subobjects,
        Optional<SourceKeepMode> sourceKeepMode,
        Dynamic dynamic,
        Map<String, Mapper> mappers,
        Map<String, PrefixProperties> prefixProperties,
        Map<String, RuntimeField> runtimeFields,
        Explicit<DateFormatter[]> dynamicDateTimeFormatters,
        Explicit<DynamicTemplate[]> dynamicTemplates,
        Explicit<Boolean> dateDetection,
        Explicit<Boolean> numericDetection,
        RootObjectMapperNamespaceValidator namespaceValidator,
        boolean sliceEnabled
    ) {
        super(name, name, enabled, subobjects, sourceKeepMode, dynamic, mappers);
        this.runtimeFields = runtimeFields;
        this.dynamicTemplates = dynamicTemplates;
        this.dynamicDateTimeFormatters = dynamicDateTimeFormatters;
        this.dateDetection = dateDetection;
        this.numericDetection = numericDetection;
        this.namespaceValidator = namespaceValidator == null ? new DefaultRootObjectMapperNamespaceValidator() : namespaceValidator;
        this.sliceEnabled = sliceEnabled;
        this.prefixProperties = Collections.unmodifiableNavigableMap(new TreeMap<>(prefixProperties));
    }

    @Override
    public RootObjectMapper.Builder newBuilder(IndexVersion indexVersionCreated) {
        RootObjectMapper.Builder builder = new RootObjectMapper.Builder(this.fullPath(), subobjects);
        builder.enabled = enabled;
        builder.dynamic = dynamic;
        builder.sourceKeepMode = sourceKeepMode;
        builder.sliceEnabled = sliceEnabled;
        return builder;
    }

    @Override
    RootObjectMapper withoutMappers() {
        return new RootObjectMapper(
            leafName(),
            enabled,
            subobjects,
            sourceKeepMode,
            dynamic,
            Map.of(),
            prefixProperties,
            Map.of(),
            dynamicDateTimeFormatters,
            dynamicTemplates,
            dateDetection,
            numericDetection,
            namespaceValidator,
            sliceEnabled
        );
    }

    /**
     * Public API
     */
    public boolean dateDetection() {
        return this.dateDetection.value();
    }

    /**
     * Public API
     */
    public boolean numericDetection() {
        return this.numericDetection.value();
    }

    /**
     * Public API
     */
    public DateFormatter[] dynamicDateTimeFormatters() {
        return dynamicDateTimeFormatters.value();
    }

    /**
     * Public API
     */
    public DynamicTemplate[] dynamicTemplates() {
        return dynamicTemplates.value();
    }

    public Collection<RuntimeField> runtimeFields() {
        return runtimeFields.values();
    }

    RuntimeField getRuntimeField(String name) {
        return runtimeFields.get(name);
    }

    /**
     * Resolves the effective {@code dynamic} value for an unmapped field at {@code fullPath} in strict columnar mode.
     * Uses a longest-prefix match against the per-object {@code dynamic} settings and {@code enabled:false} prefixes
     * captured during auto-flattening. If no prefix matches, returns {@code fallback} (the root-level dynamic).
     *
     * <p>An {@code enabled:false} prefix always resolves to {@link Dynamic#FALSE}, causing the entire subtree
     * under that prefix to be dropped at index time — consistent with the existing columnar {@code dynamic:false}
     * drop behaviour.
     *
     * <p>For example, if the mapping declared {@code attributes} with {@code dynamic:false},
     * {@code resource} with {@code dynamic:strict}, and {@code disabled} with {@code enabled:false}, then:
     * <ul>
     *   <li>{@code "attributes.foo"} → {@link Dynamic#FALSE}</li>
     *   <li>{@code "resource.bar"} → {@link Dynamic#STRICT}</li>
     *   <li>{@code "disabled.anything"} → {@link Dynamic#FALSE} (enabled:false wins)</li>
     *   <li>{@code "other.baz"} → {@code fallback}</li>
     * </ul>
     *
     * @param fullPath the full dotted path of the field being resolved (e.g. {@code "attributes.foo"})
     * @param fallback the dynamic value to return when no prefix matches (typically the root dynamic)
     * @return the resolved dynamic value
     */
    public Dynamic resolveDynamic(String fullPath, Dynamic fallback) {
        if (prefixProperties.isEmpty()) {
            return fallback;
        }
        // headMap(fullPath, inclusive=true) limits candidates to prefixes <= fullPath in sort order.
        // Iterating in descending order finds the longest (closest) matching prefix first.
        // A prefix matches if it is exactly fullPath or a dotted ancestor of fullPath.
        // TODO: for large prefix sets a trie would reduce scan cost; acceptable for now given the
        // small number of prefixes expected in practice.
        for (Map.Entry<String, PrefixProperties> entry : prefixProperties.headMap(fullPath, true).descendingMap().entrySet()) {
            PrefixProperties pp = entry.getValue();
            if (pp.dynamic() == null && Boolean.FALSE.equals(pp.enabled()) == false) {
                continue; // passthrough-only (or enabled:true) prefix with no dynamic; skip
            }
            String prefix = entry.getKey();
            if (fullPath.equals(prefix) || fullPath.startsWith(prefix + ".")) {
                if (Boolean.FALSE.equals(pp.enabled())) {
                    return Dynamic.FALSE; // disabled ancestor always wins
                }
                return pp.dynamic();
            }
        }
        return fallback;
    }

    // package-private accessor used by tests and MappingLookup → FieldTypeLookup for prefix-based resolution
    Map<String, PrefixProperties> getPrefixProperties() {
        return prefixProperties;
    }

    @Override
    protected void doXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        final boolean includeDefaults = params.paramAsBoolean("include_defaults", false);

        if (dynamicDateTimeFormatters.explicit() || includeDefaults) {
            builder.startArray("dynamic_date_formats");
            for (DateFormatter dateTimeFormatter : dynamicDateTimeFormatters.value()) {
                builder.value(dateTimeFormatter.pattern());
            }
            builder.endArray();
        }

        if (dynamicTemplates.explicit() || includeDefaults) {
            builder.startArray("dynamic_templates");
            for (DynamicTemplate dynamicTemplate : dynamicTemplates.value()) {
                builder.startObject();
                builder.field(dynamicTemplate.name(), dynamicTemplate);
                builder.endObject();
            }
            builder.endArray();
        }

        if (dateDetection.explicit() || includeDefaults) {
            builder.field("date_detection", dateDetection.value());
        }
        if (numericDetection.explicit() || includeDefaults) {
            builder.field("numeric_detection", numericDetection.value());
        }

        if (runtimeFields.isEmpty() == false) {
            builder.startObject("runtime");
            List<RuntimeField> sortedRuntimeFields = runtimeFields.values()
                .stream()
                .sorted(Comparator.comparing(RuntimeField::name))
                .toList();
            for (RuntimeField fieldType : sortedRuntimeFields) {
                fieldType.toXContent(builder, params);
            }
            builder.endObject();
        }

        if (prefixProperties.isEmpty() == false) {
            // Keys are in sorted order (guaranteed by the NavigableMap field type) for deterministic
            // serialization: the mapping source is byte-compared across nodes, so output order must
            // be stable.
            //
            // "prefix_properties" is an umbrella for per-prefix object settings in strict columnar
            // mode. Each entry is keyed by the full dotted prefix path and holds its facets as an
            // object (e.g. {"dynamic": "strict", "passthrough": 1, "enabled": false}). To add a new
            // facet: add it to the {@link PrefixProperties} record, populate it in
            // asFlattenedFieldBuilders gated on isStrictColumnar(), serialize/parse it in the loop
            // below, and propagate it in PrefixProperties#merge. Follow-ups in #151524.
            builder.startObject("prefix_properties");
            for (Map.Entry<String, PrefixProperties> entry : prefixProperties.entrySet()) {
                PrefixProperties pp = entry.getValue();
                builder.startObject(entry.getKey());
                if (pp.dynamic() != null) {
                    builder.field("dynamic", pp.dynamic().name().toLowerCase(Locale.ROOT));
                }
                if (pp.passthrough() != null) {
                    builder.field("passthrough", pp.passthrough());
                }
                if (pp.enabled() != null) {
                    builder.field("enabled", pp.enabled());
                }
                builder.endObject();
            }
            builder.endObject();
        }
    }

    private static void validateDynamicTemplate(MappingParserContext parserContext, DynamicTemplate template) {

        if (containsSnippet(template.getMapping(), "{name}")) {
            // Can't validate template, because field names can't be guessed up front.
            return;
        }

        final XContentFieldType[] types = template.getXContentFieldTypes();

        Exception lastError = null;

        for (XContentFieldType fieldType : types) {
            String dynamicType = template.isRuntimeMapping() ? fieldType.defaultRuntimeMappingType() : fieldType.defaultMappingType();
            String mappingType = template.mappingType(dynamicType);
            try {
                if (template.isRuntimeMapping()) {
                    RuntimeField.Parser parser = parserContext.runtimeFieldParser(mappingType);
                    if (parser == null) {
                        throw new IllegalArgumentException("No runtime field found for type [" + mappingType + "]");
                    }
                    validate(template, dynamicType, (name, mapping) -> parser.parse(name, mapping, parserContext));
                } else {
                    Mapper.TypeParser typeParser = parserContext.typeParser(mappingType);
                    if (typeParser == null) {
                        throw new IllegalArgumentException("No mapper found for type [" + mappingType + "]");
                    }
                    validate(
                        template,
                        dynamicType,
                        (name, mapping) -> typeParser.parse(name, mapping, parserContext).build(MapperBuilderContext.root(false, false))
                    );
                }
                lastError = null; // ok, the template is valid for at least one type
                break;
            } catch (Exception e) {
                lastError = e;
            }
        }
        if (lastError != null) {
            String format = "dynamic template [%s] has invalid content [%s], "
                + "attempted to validate it with the following match_mapping_type: %s";
            String message = String.format(Locale.ROOT, format, template.getName(), Strings.toString(template), Arrays.toString(types));
            final boolean failInvalidDynamicTemplates = parserContext.indexVersionCreated().onOrAfter(IndexVersions.V_8_0_0);
            if (failInvalidDynamicTemplates) {
                throw new IllegalArgumentException(message, lastError);
            } else {
                DEPRECATION_LOGGER.warn(
                    DeprecationCategory.TEMPLATES,
                    "invalid_dynamic_template",
                    "{}, last error: [{}]",
                    message,
                    lastError.getMessage()
                );
            }
        }
    }

    private static void validate(DynamicTemplate template, String dynamicType, BiConsumer<String, Map<String, Object>> mappingConsumer) {
        String templateName = "__dynamic__" + template.name();
        Map<String, Object> fieldTypeConfig = template.mappingForName(templateName, dynamicType);
        mappingConsumer.accept(templateName, fieldTypeConfig);
        fieldTypeConfig.remove("type");
        if (fieldTypeConfig.isEmpty() == false) {
            throw new IllegalArgumentException("Unknown mapping attributes [" + fieldTypeConfig + "]");
        }
    }

    private static boolean containsSnippet(Map<?, ?> map, String snippet) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = entry.getKey().toString();
            if (key.contains(snippet)) {
                return true;
            }
            Object value = entry.getValue();
            if (containsSnippet(value, snippet)) {
                return true;
            }
        }

        return false;
    }

    private static boolean containsSnippet(List<?> list, String snippet) {
        for (Object value : list) {
            if (containsSnippet(value, snippet)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsSnippet(Object value, String snippet) {
        if (value instanceof Map) {
            return containsSnippet((Map<?, ?>) value, snippet);
        } else if (value instanceof List) {
            return containsSnippet((List<?>) value, snippet);
        } else if (value instanceof String) {
            return ((String) value).contains(snippet);
        }
        return false;
    }

    @Override
    protected boolean isRoot() {
        return true;
    }

    public static RootObjectMapper.Builder parse(String name, Map<String, Object> node, MappingParserContext parserContext)
        throws MapperParsingException {
        Explicit<Subobjects> subobjects = parseSubobjects(node, parserContext);
        RootObjectMapper.Builder builder = new Builder(name, subobjects);
        builder.addNamespaceValidator(parserContext.getNamespaceValidator());
        builder.setSliceEnabled(parserContext.getIndexSettings().isSliceEnabled());
        Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            String fieldName = entry.getKey();
            Object fieldNode = entry.getValue();
            if (parseObjectOrDocumentTypeProperties(fieldName, fieldNode, parserContext, builder)
                || processField(builder, fieldName, fieldNode, parserContext)) {
                iterator.remove();
            }
        }
        if (builder.sourceKeepMode.orElse(SourceKeepMode.NONE) == SourceKeepMode.ALL) {
            throw new MapperParsingException(
                "root object can't be configured with [" + Mapper.SYNTHETIC_SOURCE_KEEP_PARAM + ":" + SourceKeepMode.ALL + "]"
            );
        }
        return builder;
    }

    @SuppressWarnings("unchecked")
    private static boolean processField(
        RootObjectMapper.Builder builder,
        String fieldName,
        Object fieldNode,
        MappingParserContext parserContext
    ) {
        if (fieldName.equals("date_formats") || fieldName.equals("dynamic_date_formats")) {
            if (fieldNode instanceof List) {
                List<DateFormatter> formatters = new ArrayList<>();
                for (Object formatter : (List<?>) fieldNode) {
                    if (formatter.toString().startsWith("epoch_")) {
                        throw new MapperParsingException("Epoch [" + formatter + "] is not supported as dynamic date format");
                    }
                    formatters.add(parseDateTimeFormatter(formatter));
                }
                builder.dynamicDateTimeFormatter(formatters);
            } else if ("none".equals(fieldNode.toString())) {
                builder.dynamicDateTimeFormatter(Collections.emptyList());
            } else {
                builder.dynamicDateTimeFormatter(Collections.singleton(parseDateTimeFormatter(fieldNode)));
            }
            return true;
        } else if (fieldName.equals("dynamic_templates")) {
            /*
              "dynamic_templates" : [
                  {
                      "template_1" : {
                          "match" : "*_test",
                          "match_mapping_type" : "string",
                          "mapping" : { "type" : "keyword", "store" : "yes" }
                      }
                  }
              ]
            */
            if ((fieldNode instanceof List) == false) {
                throw new MapperParsingException("Dynamic template syntax error. An array of named objects is expected.");
            }
            List<?> tmplNodes = (List<?>) fieldNode;
            List<DynamicTemplate> templates = new ArrayList<>();
            for (Object tmplNode : tmplNodes) {
                Map<String, Object> tmpl = (Map<String, Object>) tmplNode;
                if (tmpl.size() != 1) {
                    throw new MapperParsingException("A dynamic template must be defined with a name");
                }
                Map.Entry<String, Object> entry = tmpl.entrySet().iterator().next();
                String templateName = entry.getKey();
                Map<String, Object> templateParams = (Map<String, Object>) entry.getValue();
                DynamicTemplate template = DynamicTemplate.parse(templateName, templateParams);
                validateDynamicTemplate(parserContext.createDynamicTemplateContext(null), template);
                templates.add(template);
            }
            builder.dynamicTemplates(templates);
            return true;
        } else if (fieldName.equals("date_detection")) {
            builder.dateDetection = Explicit.explicitBoolean(nodeBooleanValue(fieldNode, "date_detection"));
            return true;
        } else if (fieldName.equals("numeric_detection")) {
            builder.numericDetection = Explicit.explicitBoolean(nodeBooleanValue(fieldNode, "numeric_detection"));
            return true;
        } else if (fieldName.equals("runtime")) {
            if (fieldNode instanceof Map) {
                Map<String, RuntimeField> fields = RuntimeField.parseRuntimeFields((Map<String, Object>) fieldNode, parserContext, true);
                builder.addRuntimeFields(fields);
                return true;
            } else {
                throw new ElasticsearchParseException("runtime must be a map type");
            }
        } else if (fieldName.equals("prefix_properties")) {
            if ((fieldNode instanceof Map) == false) {
                throw new MapperParsingException("[prefix_properties] must be an object");
            }
            for (Map.Entry<String, Object> prefixEntry : ((Map<String, Object>) fieldNode).entrySet()) {
                String prefix = prefixEntry.getKey();
                Object prefixNode = prefixEntry.getValue();
                if ((prefixNode instanceof Map) == false) {
                    throw new MapperParsingException("[prefix_properties." + prefix + "] must be an object");
                }
                Map<String, Object> facets = (Map<String, Object>) prefixNode;
                Dynamic dynamic = null;
                Integer passthrough = null;
                Boolean enabled = null;
                Object dynamicVal = facets.get("dynamic");
                if (dynamicVal != null) {
                    dynamic = parsePrefixDynamic(prefix, dynamicVal);
                }
                Object passthroughVal = facets.get("passthrough");
                if (passthroughVal != null) {
                    passthrough = parsePrefixPassthrough(prefix, passthroughVal);
                }
                Object enabledVal = facets.get("enabled");
                if (enabledVal != null) {
                    enabled = parsePrefixEnabled(prefix, enabledVal);
                }
                builder.prefixProperties.put(prefix, new PrefixProperties(dynamic, passthrough, enabled));
            }
            return true;
        }
        return false;
    }

    /**
     * Parses a {@code dynamic} value for use in {@code prefix_properties.<prefix>.dynamic}.
     * Accepts {@code true}, {@code false}, and {@code strict} (case-insensitive).
     * {@code runtime} is explicitly rejected because it is not supported in strict columnar mode.
     */
    private static Dynamic parsePrefixDynamic(String key, Object value) {
        if (value instanceof String == false) {
            throw new MapperParsingException("[prefix_properties." + key + ".dynamic] must be a string value");
        }
        String str = (String) value;
        if (str.equalsIgnoreCase("runtime")) {
            throw new MapperParsingException("[prefix_properties." + key + ".dynamic] does not support [runtime]");
        }
        try {
            return Dynamic.valueOf(str.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new MapperParsingException("unknown value [" + str + "] for [prefix_properties." + key + ".dynamic]");
        }
    }

    private static int parsePrefixPassthrough(String key, Object value) {
        if (value instanceof Integer i) {
            return i;
        }
        if (value instanceof Number n) {
            return n.intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException e) {
            throw new MapperParsingException("[prefix_properties." + key + ".passthrough] must be an integer value");
        }
    }

    private static boolean parsePrefixEnabled(String key, Object value) {
        return nodeBooleanValue(value, "prefix_properties." + key + ".enabled");
    }

    @Override
    public int getTotalFieldsCount() {
        return super.getTotalFieldsCount() - 1 + runtimeFields.size();
    }

    /**
     * Overrides in order to run the namespace validator first and then delegates to the
     * standard validateSubField on the parent class
     */
    @Override
    protected void validateSubField(Mapper mapper, MappingLookup mappers) {
        namespaceValidator.validateNamespace(subobjects(), mapper.leafName());
        if (sliceEnabled && SliceIndexing.PARAM_NAME.equals(mapper.leafName())) {
            throw new IllegalArgumentException(
                "["
                    + SliceIndexing.PARAM_NAME
                    + "] is a reserved field name and cannot be used when ["
                    + IndexSettings.SLICE_ENABLED.getKey()
                    + "] is true"
            );
        }
        super.validateSubField(mapper, mappers);
    }
}
