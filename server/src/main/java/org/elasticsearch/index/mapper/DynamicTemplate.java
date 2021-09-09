/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

public class DynamicTemplate implements ToXContentObject {

    public enum MatchType {
        SIMPLE {
            @Override
            public boolean matches(String pattern, String value) {
                return Regex.simpleMatch(pattern, value);
            }
            @Override
            public String toString() {
                return "simple";
            }
        },
        REGEX {
            @Override
            public boolean matches(String pattern, String value) {
                return value.matches(pattern);
            }
            @Override
            public String toString() {
                return "regex";
            }
        };

        public static MatchType fromString(String value) {
            for (MatchType v : values()) {
                if (v.toString().equals(value)) {
                    return v;
                }
            }
            throw new IllegalArgumentException("No matching pattern matched on [" + value + "]");
        }

        /** Whether {@code value} matches {@code regex}. */
        public abstract boolean matches(String regex, String value);
    }

    /** The type of a field as detected while parsing a json document. */
    public enum XContentFieldType {
        OBJECT {
            @Override
            boolean supportsRuntimeField() {
                return false;
            }
        },
        STRING {
            @Override
            public String defaultMappingType() {
                return TextFieldMapper.CONTENT_TYPE;
            }
            @Override
            String defaultRuntimeMappingType() {
                return KeywordFieldMapper.CONTENT_TYPE;
            }
        },
        LONG,
        DOUBLE {
            @Override
            public String defaultMappingType() {
                return NumberFieldMapper.NumberType.FLOAT.typeName();
            }
        },
        BOOLEAN,
        DATE,
        BINARY {
            @Override
            boolean supportsRuntimeField() {
                return false;
            }
        };

        public static XContentFieldType fromString(String value) {
            for (XContentFieldType v : values()) {
                if (v.toString().equals(value)) {
                    return v;
                }
            }
            throw new IllegalArgumentException("No field type matched on [" + value + "], possible values are "
                    + Arrays.toString(values()));
        }

        /**
         * The default mapping type to use for fields of this {@link XContentFieldType}.
         * By default, the lowercase field type is used.
         */
        String defaultMappingType() {
            return toString();
        }

        /**
         * The default mapping type to use for fields of this {@link XContentFieldType} when defined as runtime fields
         * By default, the lowercase field type is used.
         */
        String defaultRuntimeMappingType() {
            return toString();
        }

        /**
         * Returns true if the field type supported as runtime field, false otherwise.
         * Whenever a match_mapping_type has not been defined in a dynamic template, if a runtime mapping has been specified only
         * field types that are supported as runtime field will match the template.
         * Also, it is not possible to define a dynamic template that defines a runtime field and explicitly matches a type that
         * is not supported as runtime field.
         */
        boolean supportsRuntimeField() {
            return true;
        }

        @Override
        public final String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    @SuppressWarnings("unchecked")
    static DynamicTemplate parse(String name, Map<String, Object> conf) throws MapperParsingException {
        String match = null;
        String pathMatch = null;
        String unmatch = null;
        String pathUnmatch = null;
        Map<String, Object> mapping = null;
        boolean runtime = false;
        String matchMappingType = null;
        String matchPattern = MatchType.SIMPLE.toString();

        for (Map.Entry<String, Object> entry : conf.entrySet()) {
            String propName = entry.getKey();
            if ("match".equals(propName)) {
                match = entry.getValue().toString();
            } else if ("path_match".equals(propName)) {
                pathMatch = entry.getValue().toString();
            } else if ("unmatch".equals(propName)) {
                unmatch = entry.getValue().toString();
            } else if ("path_unmatch".equals(propName)) {
                pathUnmatch = entry.getValue().toString();
            } else if ("match_mapping_type".equals(propName)) {
                matchMappingType = entry.getValue().toString();
            } else if ("match_pattern".equals(propName)) {
                matchPattern = entry.getValue().toString();
            } else if ("mapping".equals(propName)) {
                if (mapping != null) {
                    throw new MapperParsingException("mapping and runtime cannot be both specified in the same dynamic template ["
                        + name + "]");
                }
                mapping = (Map<String, Object>) entry.getValue();
                runtime = false;
            } else if ("runtime".equals(propName)) {
                if (mapping != null) {
                    throw new MapperParsingException("mapping and runtime cannot be both specified in the same dynamic template ["
                        + name + "]");

                }
                mapping = (Map<String, Object>) entry.getValue();
                runtime = true;
            } else {
                // unknown parameters were ignored before but still carried through serialization
                // so we need to ignore them at parsing time for old indices
                throw new IllegalArgumentException("Illegal dynamic template parameter: [" + propName + "]");
            }
        }
        if (mapping == null) {
            throw new MapperParsingException("template [" + name + "] must have either mapping or runtime set");
        }

        final XContentFieldType[] xContentFieldTypes;
        if ("*".equals(matchMappingType) || (matchMappingType == null && (match != null || pathMatch != null))) {
            if (runtime) {
                xContentFieldTypes = Arrays.stream(XContentFieldType.values())
                    .filter(XContentFieldType::supportsRuntimeField)
                    .toArray(XContentFieldType[]::new);
            } else {
                xContentFieldTypes = XContentFieldType.values();
            }
        } else if (matchMappingType != null) {
            final XContentFieldType xContentFieldType = XContentFieldType.fromString(matchMappingType);
            if (runtime && xContentFieldType.supportsRuntimeField() == false) {
                throw new MapperParsingException("Dynamic template [" + name + "] defines a runtime field but type ["
                    + xContentFieldType + "] is not supported as runtime field");
            }
            xContentFieldTypes = new XContentFieldType[]{xContentFieldType};
        } else {
            xContentFieldTypes = new XContentFieldType[0];
        }

        final MatchType matchType = MatchType.fromString(matchPattern);

        // Validate that the pattern
        for (String regex : new String[] { pathMatch, match, pathUnmatch, unmatch }) {
            if (regex == null) {
                continue;
            }
            try {
                matchType.matches(regex, "");
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Pattern [" + regex + "] of type [" + matchType
                    + "] is invalid. Cannot create dynamic template [" + name + "].", e);
            }
        }

        return new DynamicTemplate(name, pathMatch, pathUnmatch, match, unmatch, xContentFieldTypes, matchType, mapping, runtime);
    }

    private final String name;
    private final String pathMatch;
    private final String pathUnmatch;
    private final String match;
    private final String unmatch;
    private final MatchType matchType;
    private final XContentFieldType[] xContentFieldTypes;
    private final Map<String, Object> mapping;
    private final boolean runtimeMapping;

    private DynamicTemplate(String name, String pathMatch, String pathUnmatch, String match, String unmatch,
                            XContentFieldType[] xContentFieldTypes, MatchType matchType, Map<String, Object> mapping,
                            boolean runtimeMapping) {
        this.name = name;
        this.pathMatch = pathMatch;
        this.pathUnmatch = pathUnmatch;
        this.match = match;
        this.unmatch = unmatch;
        this.matchType = matchType;
        this.xContentFieldTypes = xContentFieldTypes;
        this.mapping = mapping;
        this.runtimeMapping = runtimeMapping;
    }

    public String name() {
        return this.name;
    }

    public String pathMatch() {
        return pathMatch;
    }

    public String match() {
        return match;
    }

    public boolean match(String templateName, String path, String fieldName, XContentFieldType xcontentFieldType) {
        // If the template name parameter is specified, then we will check only the name of the template and ignore other matches.
        if (templateName != null) {
            return templateName.equals(name);
        }
        if (pathMatch != null && matchType.matches(pathMatch, path) == false) {
            return false;
        }
        if (match != null && matchType.matches(match, fieldName) == false) {
            return false;
        }
        if (pathUnmatch != null && matchType.matches(pathUnmatch, path)) {
            return false;
        }
        if (unmatch != null && matchType.matches(unmatch, fieldName)) {
            return false;
        }
        if (Arrays.stream(xContentFieldTypes).noneMatch(xcontentFieldType::equals)) {
            return false;
        }
        if (runtimeMapping && xcontentFieldType.supportsRuntimeField() == false) {
            return false;
        }
        return true;
    }

    public String mappingType(String dynamicType) {
        String type;
        if (mapping.containsKey("type")) {
            type = mapping.get("type").toString();
            type = type.replace("{dynamic_type}", dynamicType);
            type = type.replace("{dynamicType}", dynamicType);
        } else {
            type = dynamicType;
        }
        if (type.equals(mapping.get("type")) == false // either the type was not set, or we updated it through replacements
                && TextFieldMapper.CONTENT_TYPE.equals(type)) { // and the result is "text"
            // now that string has been splitted into text and keyword, we use text for
            // dynamic mappings. However before it used to be possible to index as a keyword
            // by setting index=not_analyzed, so for now we will use a keyword field rather
            // than a text field if index=not_analyzed and the field type was not specified
            // explicitly
            // TODO: remove this in 6.0
            // TODO: how to do it in the future?
            final Object index = mapping.get("index");
            if ("not_analyzed".equals(index) || "no".equals(index)) {
                return KeywordFieldMapper.CONTENT_TYPE;
            }
        }
        return type;
     }

    public boolean isRuntimeMapping() {
        return runtimeMapping;
    }

    public Map<String, Object> mappingForName(String name, String dynamicType) {
        return processMap(mapping, name, dynamicType);
    }

    private static Map<String, Object> processMap(Map<String, Object> map, String name, String dynamicType) {
        Map<String, Object> processedMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey().replace("{name}", name).replace("{dynamic_type}", dynamicType)
                .replace("{dynamicType}", dynamicType);
            processedMap.put(key, extractValue(entry.getValue(), name, dynamicType));
        }
        return processedMap;
    }

    private static List<?> processList(List<?> list, String name, String dynamicType) {
        List<Object> processedList = new ArrayList<>(list.size());
        for (Object value : list) {
            processedList.add(extractValue(value, name, dynamicType));
        }
        return processedList;
    }

    @SuppressWarnings("unchecked")
    private static Object extractValue(Object value, String name, String dynamicType) {
        if (value instanceof Map) {
            return processMap((Map<String, Object>) value, name, dynamicType);
        } else if (value instanceof List) {
            return processList((List<?>) value, name, dynamicType);
        } else if (value instanceof String) {
            return value.toString().replace("{name}", name)
                .replace("{dynamic_type}", dynamicType)
                .replace("{dynamicType}", dynamicType);
        }
        return value;
    }

    String getName() {
        return name;
    }

    XContentFieldType[] getXContentFieldTypes() {
        return xContentFieldTypes;
    }

    Map<String, Object> getMapping() {
        return mapping;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (match != null) {
            builder.field("match", match);
        }
        if (pathMatch != null) {
            builder.field("path_match", pathMatch);
        }
        if (unmatch != null) {
            builder.field("unmatch", unmatch);
        }
        if (pathUnmatch != null) {
            builder.field("path_unmatch", pathUnmatch);
        }
        // We have more than one types when (1) `match_mapping_type` is "*", and (2) match and/or path_match are defined but
        // not `match_mapping_type`. In the latter the template implicitly accepts all types and we don't need to serialize
        // the `match_mapping_type` values.
        if (xContentFieldTypes.length > 1 && match == null && pathMatch == null) {
            builder.field("match_mapping_type", "*");
        } else if (xContentFieldTypes.length == 1) {
            builder.field("match_mapping_type", xContentFieldTypes[0]);
        }
        if (matchType != MatchType.SIMPLE) {
            builder.field("match_pattern", matchType);
        }
        // use a sorted map for consistent serialization
        if (runtimeMapping) {
            builder.field("runtime", new TreeMap<>(mapping));
        } else {
            builder.field("mapping", new TreeMap<>(mapping));
        }
        builder.endObject();
        return builder;
    }
}
