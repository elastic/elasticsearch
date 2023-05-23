/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Stream;

public class DynamicTemplate implements ToXContentObject {

    public enum MatchType {
        /**
         * DEFAULT is set when the user did not explicitly set a match_pattern in their dynamic_templates entry.
         * DEFAULT uses the functionality of SIMPLE for matching, but does additional validation
         * to detect whether the user appears to be using regex notation, in order to provide a warning.
         */
        DEFAULT {
            @Override
            public boolean matches(String pattern, String value) {
                return MatchType.SIMPLE.matches(pattern, value);
            }

            @Override
            public String toString() {
                return "default";
            }

            /**
             * Attempts to determine if the String passed in appears to be
             * regular expression rather than a simple wildcard match or a
             * regular ES field name (or dotted path) with no regex metacharacters.
             *
             * The method looks for the presence of standard regex metacharacters
             * like $, ^, |, .*, or paired braces or brackets. If found, it will then
             * attempt to compile it as a regex to determine if it is a valid regex.
             *
             * It will add a {@link HeaderWarning} if the user appears to be passing in a regex
             * in a default (simple) match scenario. This method will not throw an Exception.
             *
             * @param pattern pattern to evaluate
             */
            @Override
            public void validate(String pattern, String templateName) {
                // regexes cannot start with '*' so we assume this is valid (not a regex)
                if (pattern.startsWith("*")) {
                    return;
                }

                boolean regexCandidate = false;
                if (pattern.startsWith(".*")) {  // .* is valid for SIMPLE because of ES dotted field names, except at string start
                    regexCandidate = true;
                } else if (pattern.contains("^")) {
                    regexCandidate = true;
                } else if (pattern.contains("$")) {
                    regexCandidate = true;
                } else if (pattern.contains("|")) {
                    regexCandidate = true;
                } else if (pattern.contains("[") && pattern.contains("]")) {
                    regexCandidate = true;
                } else if (pattern.contains("{") && pattern.contains("}")) {
                    regexCandidate = true;
                }

                if (regexCandidate) {
                    try {
                        Pattern.compile(pattern);
                        // simple patterns only warrant a warning and only if the user did not explicitly set match_pattern=simple
                        String warning = "Pattern ["
                            + pattern
                            + "] in dynamic template ["
                            + templateName
                            + "] appears to be a regular expression, not a simple wildcard pattern"
                            + ". To remove this warning, set [match_pattern] in the dynamic template.";
                        HeaderWarning.addWarning(warning);
                    } catch (PatternSyntaxException e) {
                        // ignore since it is not a valid regex
                    }
                }
            }
        },
        SIMPLE {
            @Override
            public boolean matches(String pattern, String value) {
                return Regex.simpleMatch(pattern, value);
            }

            @Override
            public String toString() {
                return "simple";
            }

            @Override
            public void validate(String pattern, String templateName) {
                // no-op. All patterns are valid if user explicitly set match_pattern=simple
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

            /**
             * If pattern is valid, no exception is thrown.
             * @param pattern to evaluate
             * @throws MapperParsingException if pattern is not a valid Java regex
             */
            @Override
            public void validate(String pattern, String templateName) {
                try {
                    // if it parses as a Java regex, it is valid
                    this.matches(pattern, "");
                } catch (PatternSyntaxException e) {
                    // fail dynamic_template calls that have invalid regexes
                    throw new MapperParsingException(
                        Strings.format(
                            "Pattern [%s] of type [regex] is invalid. Cannot create dynamic template [%s].",
                            pattern,
                            templateName
                        ),
                        e
                    );
                }
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

        /**
         * Validates {@code pattern} for the MatchType.
         * @param pattern to validate
         * @param templateName naming of the dynamic_template passed in (for use in warnings/error messages)
         * @throws IllegalArgumentException if the pattern is not valid
         */
        public abstract void validate(String pattern, String templateName);
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
            throw new IllegalArgumentException(
                "No field type matched on [" + value + "], possible values are " + Arrays.toString(values())
            );
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
        List<String> match = new ArrayList<>(4); // these pattern lists will typically be very small
        List<String> pathMatch = new ArrayList<>(4);
        List<String> unmatch = new ArrayList<>(4);
        List<String> pathUnmatch = new ArrayList<>(4);
        Map<String, Object> mapping = null;
        boolean runtime = false;
        String matchMappingType = null;
        String matchPattern = MatchType.DEFAULT.toString();

        for (Map.Entry<String, Object> entry : conf.entrySet()) {
            String propName = entry.getKey();
            if ("match".equals(propName)) {
                addEntriesToPatternList(match, propName, entry);
            } else if ("path_match".equals(propName)) {
                addEntriesToPatternList(pathMatch, propName, entry);
            } else if ("unmatch".equals(propName)) {
                addEntriesToPatternList(unmatch, propName, entry);
            } else if ("path_unmatch".equals(propName)) {
                addEntriesToPatternList(pathUnmatch, propName, entry);
            } else if ("match_mapping_type".equals(propName)) {
                matchMappingType = entry.getValue().toString();
            } else if ("match_pattern".equals(propName)) {
                matchPattern = entry.getValue().toString();
            } else if ("mapping".equals(propName)) {
                if (mapping != null) {
                    throw new MapperParsingException(
                        "mapping and runtime cannot be both specified in the same dynamic template [" + name + "]"
                    );
                }
                mapping = (Map<String, Object>) entry.getValue();
                runtime = false;
            } else if ("runtime".equals(propName)) {
                if (mapping != null) {
                    throw new MapperParsingException(
                        "mapping and runtime cannot be both specified in the same dynamic template [" + name + "]"
                    );

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
        if ("*".equals(matchMappingType) || (matchMappingType == null && matchPatternsAreDefined(match, pathMatch))) {
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
                throw new MapperParsingException(
                    "Dynamic template ["
                        + name
                        + "] defines a runtime field but type ["
                        + xContentFieldType
                        + "] is not supported as runtime field"
                );
            }
            xContentFieldTypes = new XContentFieldType[] { xContentFieldType };
        } else {
            xContentFieldTypes = new XContentFieldType[0];
        }

        final MatchType matchType = MatchType.fromString(matchPattern);
        List<String> allPatterns = Stream.of(match.stream(), unmatch.stream(), pathMatch.stream(), pathUnmatch.stream())
            .flatMap(s -> s)
            .toList();
        for (String pattern : allPatterns) {
            // no need to check return value - the method impls either have side effects (set header warnings)
            // or throw an exception that should be sent back to the user
            matchType.validate(pattern, name);
        }

        return new DynamicTemplate(name, pathMatch, pathUnmatch, match, unmatch, xContentFieldTypes, matchType, mapping, runtime);
    }

    /**
     * @param match list of match patterns (can be empty but not null)
     * @param pathMatch list of pathMatch patterns (can be empty but not null)
     * @return return true if there is at least 1 match or pathMatch pattern defined
     */
    private static boolean matchPatternsAreDefined(List<String> match, List<String> pathMatch) {
        return match.size() + pathMatch.size() > 0;
    }

    private static void addEntriesToPatternList(List<String> matchList, String propName, Map.Entry<String, Object> entry) {
        if (entry.getValue() instanceof List<?> ls) {
            for (Object o : ls) {
                if (o instanceof String s) {
                    matchList.add(s);
                } else {
                    // for array-based matches, we enforce that only strings are allowed as un/match values in the array
                    throw new MapperParsingException(
                        Strings.format("[%s] values must either be a string or list of strings, but was [%s]", propName, entry.getValue())
                    );
                }
            }
        } else {
            // to preserve backwards compatability for single-valued matches, we convert whatever the user provided to a string
            matchList.add(entry.getValue().toString());
        }
    }

    private final String name;
    private final List<String> pathMatch;
    private final List<String> pathUnmatch;
    private final List<String> match;
    private final List<String> unmatch;
    private final MatchType matchType;
    private final XContentFieldType[] xContentFieldTypes;
    private final Map<String, Object> mapping;
    private final boolean runtimeMapping;

    private DynamicTemplate(
        String name,
        List<String> pathMatch,
        List<String> pathUnmatch,
        List<String> match,
        List<String> unmatch,
        XContentFieldType[] xContentFieldTypes,
        MatchType matchType,
        Map<String, Object> mapping,
        boolean runtimeMapping
    ) {
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

    public List<String> pathMatch() {
        return pathMatch;
    }

    public List<String> match() {
        return match;
    }

    public boolean match(String templateName, String path, String fieldName, XContentFieldType xcontentFieldType) {
        // If the template name parameter is specified, then we will check only the name of the template and ignore other matches.
        if (templateName != null) {
            return templateName.equals(name);
        }
        if (pathMatch.isEmpty() == false) {
            if (pathMatch.stream().anyMatch(m -> matchType.matches(m, path)) == false) {
                return false;
            }
        }
        if (match.isEmpty() == false) {
            if (match.stream().anyMatch(m -> matchType.matches(m, fieldName)) == false) {
                return false;
            }
        }
        for (String um : pathUnmatch) {
            if (matchType.matches(um, path)) {
                return false;
            }
        }
        for (String um : unmatch) {
            if (matchType.matches(um, fieldName)) {
                return false;
            }
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
            String key = entry.getKey()
                .replace("{name}", name)
                .replace("{dynamic_type}", dynamicType)
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
            return value.toString().replace("{name}", name).replace("{dynamic_type}", dynamicType).replace("{dynamicType}", dynamicType);
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
        if (match.isEmpty() == false) {
            if (match.size() == 1) {
                builder.field("match", match.get(0));
            } else {
                builder.field("match", match);
            }
        }
        if (pathMatch.isEmpty() == false) {
            if (pathMatch.size() == 1) {
                builder.field("path_match", pathMatch.get(0));
            } else {
                builder.field("path_match", pathMatch);
            }
        }
        if (unmatch.isEmpty() == false) {
            if (unmatch.size() == 1) {
                builder.field("unmatch", unmatch.get(0));
            } else {
                builder.field("unmatch", unmatch);
            }
        }
        if (pathUnmatch.isEmpty() == false) {
            if (pathUnmatch.size() == 1) {
                builder.field("path_unmatch", pathUnmatch.get(0));
            } else {
                builder.field("path_unmatch", pathUnmatch);
            }
        }
        // We have more than one types when (1) `match_mapping_type` is "*", and (2) match and/or path_match are defined but
        // not `match_mapping_type`. In the latter the template implicitly accepts all types and we don't need to serialize
        // the `match_mapping_type` values.
        if (xContentFieldTypes.length > 1 && match.isEmpty() && pathMatch.isEmpty()) {
            builder.field("match_mapping_type", "*");
        } else if (xContentFieldTypes.length == 1) {
            builder.field("match_mapping_type", xContentFieldTypes[0]);
        }
        if (matchType != MatchType.DEFAULT) {
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
