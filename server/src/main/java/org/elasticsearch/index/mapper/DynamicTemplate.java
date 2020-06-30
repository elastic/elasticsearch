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

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
            public String defaultMappingType() {
                return ObjectMapper.CONTENT_TYPE;
            }
            @Override
            public String toString() {
                return "object";
            }
        },
        STRING {
            @Override
            public String defaultMappingType() {
                return TextFieldMapper.CONTENT_TYPE;
            }
            @Override
            public String toString() {
                return "string";
            }
        },
        LONG {
            @Override
            public String defaultMappingType() {
                return NumberFieldMapper.NumberType.LONG.typeName();
            }
            @Override
            public String toString() {
                return "long";
            }
        },
        DOUBLE {
            @Override
            public String defaultMappingType() {
                return NumberFieldMapper.NumberType.FLOAT.typeName();
            }
            @Override
            public String toString() {
                return "double";
            }
        },
        BOOLEAN {
            @Override
            public String defaultMappingType() {
                return BooleanFieldMapper.CONTENT_TYPE;
            }
            @Override
            public String toString() {
                return "boolean";
            }
        },
        DATE {
            @Override
            public String defaultMappingType() {
                return DateFieldMapper.CONTENT_TYPE;
            }
            @Override
            public String toString() {
                return "date";
            }
        },
        BINARY {
            @Override
            public String defaultMappingType() {
                return BinaryFieldMapper.CONTENT_TYPE;
            }
            @Override
            public String toString() {
                return "binary";
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

        /** The default mapping type to use for fields of this {@link XContentFieldType}. */
        public abstract String defaultMappingType();
    }

    public static DynamicTemplate parse(String name, Map<String, Object> conf) throws MapperParsingException {
        String match = null;
        String pathMatch = null;
        String unmatch = null;
        String pathUnmatch = null;
        Map<String, Object> mapping = null;
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
                mapping = (Map<String, Object>) entry.getValue();
            } else {
                // unknown parameters were ignored before but still carried through serialization
                // so we need to ignore them at parsing time for old indices
                throw new IllegalArgumentException("Illegal dynamic template parameter: [" + propName + "]");
            }
        }

        if (match == null && pathMatch == null && matchMappingType == null) {
            throw new MapperParsingException("template must have match, path_match or match_mapping_type set " + conf.toString());
        }
        if (mapping == null) {
            throw new MapperParsingException("template must have mapping set");
        }

        XContentFieldType xcontentFieldType = null;
        if (matchMappingType != null && matchMappingType.equals("*") == false) {
            xcontentFieldType = XContentFieldType.fromString(matchMappingType);
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

        return new DynamicTemplate(name, pathMatch, pathUnmatch, match, unmatch, xcontentFieldType, matchType, mapping);
    }

    private final String name;

    private final String pathMatch;

    private final String pathUnmatch;

    private final String match;

    private final String unmatch;

    private final MatchType matchType;

    private final XContentFieldType xcontentFieldType;

    private final Map<String, Object> mapping;

    private DynamicTemplate(String name, String pathMatch, String pathUnmatch, String match, String unmatch,
            XContentFieldType xcontentFieldType, MatchType matchType, Map<String, Object> mapping) {
        this.name = name;
        this.pathMatch = pathMatch;
        this.pathUnmatch = pathUnmatch;
        this.match = match;
        this.unmatch = unmatch;
        this.matchType = matchType;
        this.xcontentFieldType = xcontentFieldType;
        this.mapping = mapping;
    }

    public String name() {
        return this.name;
    }

    public String pathMatch() {
        return pathMatch;
    }

    public boolean match(String path, String name, XContentFieldType xcontentFieldType) {
        if (pathMatch != null && !matchType.matches(pathMatch, path)) {
            return false;
        }
        if (match != null && !matchType.matches(match, name)) {
            return false;
        }
        if (pathUnmatch != null && matchType.matches(pathUnmatch, path)) {
            return false;
        }
        if (unmatch != null && matchType.matches(unmatch, name)) {
            return false;
        }
        if (this.xcontentFieldType != null && this.xcontentFieldType != xcontentFieldType) {
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
                && "text".equals(type)) { // and the result is "text"
            // now that string has been splitted into text and keyword, we use text for
            // dynamic mappings. However before it used to be possible to index as a keyword
            // by setting index=not_analyzed, so for now we will use a keyword field rather
            // than a text field if index=not_analyzed and the field type was not specified
            // explicitly
            // TODO: remove this in 6.0
            // TODO: how to do it in the future?
            final Object index = mapping.get("index");
            if ("not_analyzed".equals(index) || "no".equals(index)) {
                type = "keyword";
            }
        }
        return type;
     }

    public Map<String, Object> mappingForName(String name, String dynamicType) {
        return processMap(mapping, name, dynamicType);
    }

    private Map<String, Object> processMap(Map<String, Object> map, String name, String dynamicType) {
        Map<String, Object> processedMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey().replace("{name}", name).replace("{dynamic_type}", dynamicType)
                .replace("{dynamicType}", dynamicType);
            Object value = entry.getValue();
            if (value instanceof Map) {
                value = processMap((Map<String, Object>) value, name, dynamicType);
            } else if (value instanceof List) {
                value = processList((List) value, name, dynamicType);
            } else if (value instanceof String) {
                value = value.toString().replace("{name}", name).replace("{dynamic_type}", dynamicType)
                    .replace("{dynamicType}", dynamicType);
            }
            processedMap.put(key, value);
        }
        return processedMap;
    }

    private List processList(List list, String name, String dynamicType) {
        List processedList = new ArrayList(list.size());
        for (Object value : list) {
            if (value instanceof Map) {
                value = processMap((Map<String, Object>) value, name, dynamicType);
            } else if (value instanceof List) {
                value = processList((List) value, name, dynamicType);
            } else if (value instanceof String) {
                value = value.toString().replace("{name}", name)
                    .replace("{dynamic_type}", dynamicType)
                    .replace("{dynamicType}", dynamicType);
            }
            processedList.add(value);
        }
        return processedList;
    }

    String getName() {
        return name;
    }

    XContentFieldType getXContentFieldType() {
        return xcontentFieldType;
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
        if (xcontentFieldType != null) {
            builder.field("match_mapping_type", xcontentFieldType);
        } else if (match == null && pathMatch == null) {
            builder.field("match_mapping_type", "*");
        }
        if (matchType != MatchType.SIMPLE) {
            builder.field("match_pattern", matchType);
        }
        // use a sorted map for consistent serialization
        builder.field("mapping", new TreeMap<>(mapping));
        builder.endObject();
        return builder;
    }
}
