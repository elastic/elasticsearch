/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;


import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Index-specific deprecation checks
 */
public class IndexDeprecationChecks {

    private static void fieldLevelMappingIssue(IndexMetaData indexMetaData, BiConsumer<MappingMetaData, Map<String, Object>> checker) {
        for (ObjectCursor<MappingMetaData> mappingMetaData : indexMetaData.getMappings().values()) {
            Map<String, Object> sourceAsMap = mappingMetaData.value.sourceAsMap();
            checker.accept(mappingMetaData.value, sourceAsMap);
        }
    }

    /**
     * iterates through the "properties" field of mappings and returns any predicates that match in the
     * form of issue-strings.
     *
     * @param type the document type
     * @param parentMap the mapping to read properties from
     * @param predicate the predicate to check against for issues, issue is returned if predicate evaluates to true
     * @return a list of issues found in fields
     */
    @SuppressWarnings("unchecked")
    static List<String> findInPropertiesRecursively(String type, Map<String, Object> parentMap,
                                                    Function<Map<?,?>, Boolean> predicate) {
        List<String> issues = new ArrayList<>();
        Map<?, ?> properties = (Map<?, ?>) parentMap.get("properties");
        if (properties == null) {
            return issues;
        }
        for (Map.Entry<?, ?> entry : properties.entrySet()) {
            Map<String, Object> valueMap = (Map<String, Object>) entry.getValue();
            if (predicate.apply(valueMap)) {
                issues.add("[type: " + type + ", field: " + entry.getKey() + "]");
            }

            Map<?, ?> values = (Map<?, ?>) valueMap.get("fields");
            if (values != null) {
                for (Map.Entry<?, ?> multifieldEntry : values.entrySet()) {
                    Map<String, Object> multifieldValueMap = (Map<String, Object>) multifieldEntry.getValue();
                    if (predicate.apply(multifieldValueMap)) {
                        issues.add("[type: " + type + ", field: " + entry.getKey() + ", multifield: " + multifieldEntry.getKey() + "]");
                    }
                    if (multifieldValueMap.containsKey("properties")) {
                        issues.addAll(findInPropertiesRecursively(type, multifieldValueMap, predicate));
                    }
                }
            }
            if (valueMap.containsKey("properties")) {
                issues.addAll(findInPropertiesRecursively(type, valueMap, predicate));
            }
        }

        return issues;
    }

    static DeprecationIssue oldIndicesCheck(IndexMetaData indexMetaData) {
        Version createdWith = indexMetaData.getCreationVersion();
        if (createdWith.before(Version.V_7_0_0)) {
                return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                    "Index created before 7.0",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/master/" +
                        "breaking-changes-8.0.html",
                    "This index was created using version: " + createdWith);
            }
        return null;
    }

    static DeprecationIssue tooManyFieldsCheck(IndexMetaData indexMetaData) {
        if (indexMetaData.getSettings().get(IndexSettings.DEFAULT_FIELD_SETTING.getKey()) == null) {
            AtomicInteger fieldCount = new AtomicInteger(0);

            fieldLevelMappingIssue(indexMetaData, ((mappingMetaData, sourceAsMap) -> {
                fieldCount.addAndGet(countFieldsRecursively(mappingMetaData.type(), sourceAsMap));
            }));

            // We can't get to the setting `indices.query.bool.max_clause_count` from here, so just check the default of that setting.
            // It's also much better practice to set `index.query.default_field` than `indices.query.bool.max_clause_count` - there's a
            // reason we introduced the limit.
            if (fieldCount.get() > 1024) {
                return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                    "Number of fields exceeds automatic field expansion limit",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                        "#_limiting_the_number_of_auto_expanded_fields",
                    "This index has [" + fieldCount.get() + "] fields, which exceeds the automatic field expansion limit of 1024 " +
                        "and does not have [" + IndexSettings.DEFAULT_FIELD_SETTING.getKey() + "] set, which may cause queries which use " +
                        "automatic field expansion, such as query_string, simple_query_string, and multi_match to fail if fields are not " +
                        "explicitly specified in the query.");
            }
        }
        return null;
    }


    private static final Set<String> TYPES_THAT_DONT_COUNT;
    static {
        HashSet<String> typesThatDontCount = new HashSet<>();
        typesThatDontCount.add("binary");
        typesThatDontCount.add("geo_point");
        typesThatDontCount.add("geo_shape");
        TYPES_THAT_DONT_COUNT = Collections.unmodifiableSet(typesThatDontCount);
    }
    /* Counts the number of fields in a mapping, designed to count the as closely as possible to
     * org.elasticsearch.index.search.QueryParserHelper#checkForTooManyFields
     */
    @SuppressWarnings("unchecked")
    static int countFieldsRecursively(String type, Map<String, Object> parentMap) {
        int fields = 0;
        Map<?, ?> properties = (Map<?, ?>) parentMap.get("properties");
        if (properties == null) {
            return fields;
        }
        for (Map.Entry<?, ?> entry : properties.entrySet()) {
            Map<String, Object> valueMap = (Map<String, Object>) entry.getValue();
            if (valueMap.containsKey("type")
                && (valueMap.get("type").equals("object") && valueMap.containsKey("properties") == false) == false
                && (TYPES_THAT_DONT_COUNT.contains(valueMap.get("type")) == false)) {
                fields++;
            }

            Map<?, ?> values = (Map<?, ?>) valueMap.get("fields");
            if (values != null) {
                for (Map.Entry<?, ?> multifieldEntry : values.entrySet()) {
                    Map<String, Object> multifieldValueMap = (Map<String, Object>) multifieldEntry.getValue();
                    if (multifieldValueMap.containsKey("type")
                        && (TYPES_THAT_DONT_COUNT.contains(valueMap.get("type")) == false)) {
                        fields++;
                    }
                    if (multifieldValueMap.containsKey("properties")) {
                        fields += countFieldsRecursively(type, multifieldValueMap);
                    }
                }
            }
            if (valueMap.containsKey("properties")) {
                fields += countFieldsRecursively(type, valueMap);
            }
        }

        return fields;
    }
}
