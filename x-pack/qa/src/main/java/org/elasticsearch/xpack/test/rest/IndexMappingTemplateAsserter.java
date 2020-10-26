/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.test.rest;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Utilities for checking that the current index mappings match
 * the mappings defined in the template.
 *
 * The tests are intended to catch cases where an index mapping has been
 * updated dynamically or a write occurred before the template was put
 * causing the index to have the wrong mappings.
 *
 * These assertions are usually part of upgrade testing.
 */
public class IndexMappingTemplateAsserter {
    /**
     * Assert that the mappings of the ml indices are the same as in the
     * templates. If different this is either a consequence of an unintended
     * write (dynamic update) or the mappings have not been updated after
     * upgrade.
     *
     * A failure here will be very difficult to reproduce as it may be a side
     * effect of a different test running in the cluster.
     *
     * @param client The rest client
     * @throws IOException On error
     */
    public static void assertMlMappingsMatchTemplates(RestClient client) throws IOException {
        // Keys that have been dynamically mapped in the .ml-config index
        // but are not in the template. These can only be fixed with
        // re-index and should be addressed at the next major upgrade.
        // For now this serves as documentation of the missing fields
        Set<String> configIndexExceptions = new HashSet<>();
        configIndexExceptions.add("properties.allow_lazy_start.type");
        configIndexExceptions.add("properties.analysis.properties.classification.properties.randomize_seed.type");
        configIndexExceptions.add("properties.analysis.properties.outlier_detection.properties.compute_feature_influence.type");
        configIndexExceptions.add("properties.analysis.properties.outlier_detection.properties.outlier_fraction.type");
        configIndexExceptions.add("properties.analysis.properties.outlier_detection.properties.standardization_enabled.type");
        configIndexExceptions.add("properties.analysis.properties.regression.properties.randomize_seed.type");
        configIndexExceptions.add("properties.deleting.type");
        configIndexExceptions.add("properties.model_memory_limit.type");

        // renamed to max_trees in 7.7.
        // These exceptions are necessary for Full Cluster Restart tests where the upgrade version is < 7.x
        configIndexExceptions.add("properties.analysis.properties.classification.properties.maximum_number_trees.type");
        configIndexExceptions.add("properties.analysis.properties.regression.properties.maximum_number_trees.type");

        // Excluding those from stats index as some have been renamed and other removed.
        // These exceptions are necessary for Full Cluster Restart tests where the upgrade version is < 7.x
        Set<String> statsIndexException = new HashSet<>();
        statsIndexException.add("properties.hyperparameters.properties.regularization_depth_penalty_multiplier.type");
        statsIndexException.add("properties.hyperparameters.properties.regularization_leaf_weight_penalty_multiplier.type");
        statsIndexException.add("properties.hyperparameters.properties.regularization_soft_tree_depth_limit.type");
        statsIndexException.add("properties.hyperparameters.properties.regularization_soft_tree_depth_tolerance.type");
        statsIndexException.add("properties.hyperparameters.properties.regularization_tree_size_penalty_multiplier.type");

        assertLegacyTemplateMatchesIndexMappings(client, ".ml-config", ".ml-config", false, configIndexExceptions, true);
        // the true parameter means the index may not have been created
        assertLegacyTemplateMatchesIndexMappings(client, ".ml-meta", ".ml-meta", true, Collections.emptySet(), true);
        assertLegacyTemplateMatchesIndexMappings(client, ".ml-stats", ".ml-stats-000001", true, statsIndexException, false);
        assertLegacyTemplateMatchesIndexMappings(client, ".ml-state", ".ml-state-000001", true, Collections.emptySet(), false);
        // Depending on the order Full Cluster restart tests are run there may not be an notifications index yet
        assertLegacyTemplateMatchesIndexMappings(client,
            ".ml-notifications-000001", ".ml-notifications-000001", true, Collections.emptySet(), false);
        assertLegacyTemplateMatchesIndexMappings(client,
            ".ml-inference-000003", ".ml-inference-000003", true, Collections.emptySet(), true);
        // .ml-annotations-6 does not use a template
        // .ml-anomalies-shared uses a template but will have dynamically updated mappings as new jobs are opened
    }

    /**
     * Compares the mappings from the template and the index and asserts they
     * are the same. The assertion error message details the differences in
     * the mappings.
     *
     * The Mappings, which are maps of maps, are flattened with the keys built
     * from the keys of the sub-maps appended to the parent key.
     * This makes diffing the 2 maps easier and diffs more comprehensible.
     *
     * The _meta field is not compared as it contains version numbers that
     * change even when the mappings don't.
     *
     * Mistakes happen and some indices may be stuck with the incorrect mappings
     * that cannot be fixed without re-index. In this case use the {@code exceptions}
     * parameter to filter out fields in the index mapping that are not in the
     * template. Each exception should be a '.' separated path to the value
     * e.g. {@code properties.analysis.analysis_field.type}.
     *
     * @param client                        The rest client to use
     * @param templateName                  The template
     * @param indexName                     The index
     * @param notAnErrorIfIndexDoesNotExist The index may or may not have been created from
     *                                      the template. If {@code true} then the missing
     *                                      index does not cause an error
     * @param exceptions                    List of keys to ignore in the index mappings.
     *                                      Each key is a '.' separated path.
     * @param allowSystemIndexWarnings      Whether deprecation warnings for system index access should be allowed/expected.
     * @throws IOException                  Yes
     */
    @SuppressWarnings("unchecked")
    public static void assertLegacyTemplateMatchesIndexMappings(RestClient client,
                                                                String templateName,
                                                                String indexName,
                                                                boolean notAnErrorIfIndexDoesNotExist,
                                                                Set<String> exceptions,
                                                                boolean allowSystemIndexWarnings) throws IOException {

        Request getTemplate = new Request("GET", "_template/" + templateName);
        Response templateResponse = client.performRequest(getTemplate);
        assertEquals("missing template [" + templateName + "]", 200, templateResponse.getStatusLine().getStatusCode());

        Map<String, Object> templateMappings = (Map<String, Object>) XContentMapValues.extractValue(
            ESRestTestCase.entityAsMap(templateResponse),
            templateName, "mappings");
        assertNotNull(templateMappings);

        Request getIndexMapping = new Request("GET", indexName + "/_mapping");
        if (allowSystemIndexWarnings) {
            final String systemIndexWarning = "this request accesses system indices: [" + indexName + "], but in a future major version, " +
                "direct access to system indices will be prevented by default";
            getIndexMapping.setOptions(ESRestTestCase.expectVersionSpecificWarnings(v -> {
                v.current(systemIndexWarning);
                v.compatible(systemIndexWarning);
            }));
        }
        Response indexMappingResponse;
        try {
            indexMappingResponse = client.performRequest(getIndexMapping);
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404 && notAnErrorIfIndexDoesNotExist) {
                return;
            } else {
                throw e;
            }
        }
        assertEquals("error getting mappings for index [" + indexName + "]",
            200, indexMappingResponse.getStatusLine().getStatusCode());

        Map<String, Object> indexMappings = (Map<String, Object>) XContentMapValues.extractValue(
            ESRestTestCase.entityAsMap(indexMappingResponse),
            indexName, "mappings");
        assertNotNull(indexMappings);

        // ignore the _meta field
        indexMappings.remove("_meta");
        templateMappings.remove("_meta");

        // We cannot do a simple comparison of mappings e.g
        // Objects.equals(indexMappings, templateMappings) because some
        // templates use strings for the boolean values - "true" and "false"
        // which are automatically converted to Booleans causing the equality
        // to fail.
        boolean mappingsAreTheSame = true;

        // flatten the map of maps
        Map<String, Object> flatTemplateMap = flattenMap(templateMappings);
        Map<String, Object> flatIndexMap = flattenMap(indexMappings);

        SortedSet<String> keysInTemplateMissingFromIndex = new TreeSet<>(flatTemplateMap.keySet());
        keysInTemplateMissingFromIndex.removeAll(flatIndexMap.keySet());

        SortedSet<String> keysInIndexMissingFromTemplate = new TreeSet<>(flatIndexMap.keySet());
        keysInIndexMissingFromTemplate.removeAll(flatTemplateMap.keySet());

        // In the case of object fields the 'type: object' mapping is set by default.
        // If this does not explicitly appear in the template it is not an error
        // as ES has added the default to the index mappings
        keysInIndexMissingFromTemplate.removeIf(key -> key.endsWith("type") && "object".equals(flatIndexMap.get(key)));

        // Remove the exceptions
        keysInIndexMissingFromTemplate.removeAll(exceptions);

        StringBuilder errorMesssage = new StringBuilder("Error the template mappings [")
            .append(templateName)
            .append("] and index mappings [")
            .append(indexName)
            .append("] are not the same")
            .append(System.lineSeparator());

        if (keysInTemplateMissingFromIndex.isEmpty() == false) {
            mappingsAreTheSame = false;
            errorMesssage.append("Keys in the template missing from the index mapping: ")
                .append(keysInTemplateMissingFromIndex)
                .append(System.lineSeparator());
        }

        if (keysInIndexMissingFromTemplate.isEmpty() == false) {
            mappingsAreTheSame = false;
            errorMesssage.append("Keys in the index missing from the template mapping: ")
                .append(keysInIndexMissingFromTemplate)
                .append(System.lineSeparator());
        }

        // find values that are different for the same key
        Set<String> commonKeys = new TreeSet<>(flatIndexMap.keySet());
        commonKeys.retainAll(flatTemplateMap.keySet());
        for (String key : commonKeys) {
            Object template = flatTemplateMap.get(key);
            Object index = flatIndexMap.get(key);
            if (Objects.equals(template, index) ==  false) {
                // Both maybe be booleans but different representations
                if (areBooleanObjectsAndEqual(index, template)) {
                    continue;
                }

                mappingsAreTheSame = false;

                errorMesssage.append("Values for key [").append(key).append("] are different").append(System.lineSeparator());
                errorMesssage.append("    template value [").append(template).append("] ").append(template.getClass().getSimpleName())
                    .append(System.lineSeparator());
                errorMesssage.append("    index value [").append(index).append("] ").append(index.getClass().getSimpleName())
                    .append(System.lineSeparator());
            }
        }

        if (mappingsAreTheSame == false) {
            fail(errorMesssage.toString());
        }
    }

    public static void assertLegacyTemplateMatchesIndexMappings(RestClient client,
                                                                String templateName,
                                                                String indexName) throws IOException {
        assertLegacyTemplateMatchesIndexMappings(client, templateName, indexName, false, Collections.emptySet(), false);
    }

    private static boolean areBooleanObjectsAndEqual(Object a, Object b) {
        Boolean left;
        Boolean right;

        if (a instanceof Boolean) {
            left = (Boolean)a;
        } else if (a instanceof String && isBooleanValueString((String)a)) {
            left = Boolean.parseBoolean((String)a);
        } else {
            return false;
        }

        if (b instanceof Boolean) {
            right = (Boolean)b;
        } else if (b instanceof String && isBooleanValueString((String)b)) {
            right = Boolean.parseBoolean((String)b);
        } else {
            return false;
        }

        return left.equals(right);
    }

    /* Boolean.parseBoolean is not strict. Anything that isn't
     * "true" is returned as false. Here we want to know if
     * the string is a boolean value.
     */
    private static boolean isBooleanValueString(String s) {
        return s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false");
    }

    private static Map<String, Object> flattenMap(Map<String, Object> map) {
        return new TreeMap<>(flatten("", map).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    private static Stream<Map.Entry<String, Object>> flatten(String path, Map<String, Object> map) {
        return map.entrySet()
            .stream()
            .flatMap((e) -> extractValue(path, e));
    }

    @SuppressWarnings("unchecked")
    private static Stream<Map.Entry<String, Object>> extractValue(String path, Map.Entry<String, Object> entry) {
        String nextPath = path.isEmpty() ? entry.getKey() : path + "." + entry.getKey();
        if (entry.getValue() instanceof Map<?, ?>) {
            return flatten(nextPath, (Map<String, Object>) entry.getValue());
        } else {
            return Stream.of(new AbstractMap.SimpleEntry<>(nextPath, entry.getValue()));
        }
    }
}
