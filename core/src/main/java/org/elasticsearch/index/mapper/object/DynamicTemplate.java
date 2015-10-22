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

package org.elasticsearch.index.mapper.object;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.MapperParsingException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 */
public class DynamicTemplate {

    public static enum MatchType {
        SIMPLE,
        REGEX;

        public static MatchType fromString(String value) {
            if ("simple".equals(value)) {
                return SIMPLE;
            } else if ("regex".equals(value)) {
                return REGEX;
            }
            throw new IllegalArgumentException("No matching pattern matched on [" + value + "]");
        }
    }

    public static DynamicTemplate parse(String name, Map<String, Object> conf) throws MapperParsingException {
        String match = null;
        String pathMatch = null;
        String unmatch = null;
        String pathUnmatch = null;
        Map<String, Object> mapping = null;
        String matchMappingType = null;
        String matchPattern = "simple";

        for (Map.Entry<String, Object> entry : conf.entrySet()) {
            String propName = Strings.toUnderscoreCase(entry.getKey());
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
            }
        }

        if (match == null && pathMatch == null && matchMappingType == null) {
            throw new MapperParsingException("template must have match, path_match or match_mapping_type set");
        }
        if (mapping == null) {
            throw new MapperParsingException("template must have mapping set");
        }
        return new DynamicTemplate(name, conf, pathMatch, pathUnmatch, match, unmatch, matchMappingType, MatchType.fromString(matchPattern), mapping);
    }

    private final String name;

    private final Map<String, Object> conf;

    private final String pathMatch;

    private final String pathUnmatch;

    private final String match;

    private final String unmatch;

    private final MatchType matchType;

    private final String matchMappingType;

    private final Map<String, Object> mapping;

    public DynamicTemplate(String name, Map<String, Object> conf, String pathMatch, String pathUnmatch, String match, String unmatch, String matchMappingType, MatchType matchType, Map<String, Object> mapping) {
        this.name = name;
        this.conf = new TreeMap<>(conf);
        this.pathMatch = pathMatch;
        this.pathUnmatch = pathUnmatch;
        this.match = match;
        this.unmatch = unmatch;
        this.matchType = matchType;
        this.matchMappingType = matchMappingType;
        this.mapping = mapping;
    }

    public String name() {
        return this.name;
    }

    public Map<String, Object> conf() {
        return this.conf;
    }

    public boolean match(ContentPath path, String name, String dynamicType) {
        if (pathMatch != null && !patternMatch(pathMatch, path.fullPathAsText(name))) {
            return false;
        }
        if (match != null && !patternMatch(match, name)) {
            return false;
        }
        if (pathUnmatch != null && patternMatch(pathUnmatch, path.fullPathAsText(name))) {
            return false;
        }
        if (unmatch != null && patternMatch(unmatch, name)) {
            return false;
        }
        if (matchMappingType != null) {
            if (dynamicType == null) {
                return false;
            }
            if (!patternMatch(matchMappingType, dynamicType)) {
                return false;
            }
        }
        return true;
    }

    public boolean hasType() {
        return mapping.containsKey("type");
    }

    public String mappingType(String dynamicType) {
        return mapping.containsKey("type") ? mapping.get("type").toString().replace("{dynamic_type}", dynamicType).replace("{dynamicType}", dynamicType) : dynamicType;
    }

    private boolean patternMatch(String pattern, String str) {
        if (matchType == MatchType.SIMPLE) {
            return Regex.simpleMatch(pattern, str);
        }
        return str.matches(pattern);
    }

    public Map<String, Object> mappingForName(String name, String dynamicType) {
        return processMap(mapping, name, dynamicType);
    }

    private Map<String, Object> processMap(Map<String, Object> map, String name, String dynamicType) {
        Map<String, Object> processedMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey().replace("{name}", name).replace("{dynamic_type}", dynamicType).replace("{dynamicType}", dynamicType);
            Object value = entry.getValue();
            if (value instanceof Map) {
                value = processMap((Map<String, Object>) value, name, dynamicType);
            } else if (value instanceof List) {
                value = processList((List) value, name, dynamicType);
            } else if (value instanceof String) {
                value = value.toString().replace("{name}", name).replace("{dynamic_type}", dynamicType).replace("{dynamicType}", dynamicType);
            }
            processedMap.put(key, value);
        }
        return processedMap;
    }

    private List processList(List list, String name, String dynamicType) {
        List processedList = new ArrayList();
        for (Object value : list) {
            if (value instanceof Map) {
                value = processMap((Map<String, Object>) value, name, dynamicType);
            } else if (value instanceof List) {
                value = processList((List) value, name, dynamicType);
            } else if (value instanceof String) {
                value = value.toString().replace("{name}", name).replace("{dynamic_type}", dynamicType).replace("{dynamicType}", dynamicType);
            }
            processedList.add(value);
        }
        return processedList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DynamicTemplate that = (DynamicTemplate) o;

        // check if same matching, if so, replace the mapping
        if (match != null ? !match.equals(that.match) : that.match != null) {
            return false;
        }
        if (matchMappingType != null ? !matchMappingType.equals(that.matchMappingType) : that.matchMappingType != null) {
            return false;
        }
        if (matchType != that.matchType) {
            return false;
        }
        if (unmatch != null ? !unmatch.equals(that.unmatch) : that.unmatch != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        // check if same matching, if so, replace the mapping
        int result = match != null ? match.hashCode() : 0;
        result = 31 * result + (unmatch != null ? unmatch.hashCode() : 0);
        result = 31 * result + (matchType != null ? matchType.hashCode() : 0);
        result = 31 * result + (matchMappingType != null ? matchMappingType.hashCode() : 0);
        return result;
    }
}
