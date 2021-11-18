/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
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
     */
    public static void assertMlMappingsMatchTemplates(RestClient client) throws Exception {
        // Excluding those from stats index as some have been renamed and other removed.
        // These exceptions are necessary for Full Cluster Restart tests where the upgrade version is < 7.x
        Set<String> statsIndexException = new HashSet<>();
        statsIndexException.add("properties.hyperparameters.properties.regularization_depth_penalty_multiplier.type");
        statsIndexException.add("properties.hyperparameters.properties.regularization_leaf_weight_penalty_multiplier.type");
        statsIndexException.add("properties.hyperparameters.properties.regularization_soft_tree_depth_limit.type");
        statsIndexException.add("properties.hyperparameters.properties.regularization_soft_tree_depth_tolerance.type");
        statsIndexException.add("properties.hyperparameters.properties.regularization_tree_size_penalty_multiplier.type");

        // Excluding this from notifications index as `ignore_above` has been added to the `message.raw` field.
        // The exception is necessary for Full Cluster Restart tests.
        Set<String> notificationsIndexExceptions = new HashSet<>();
        notificationsIndexExceptions.add("properties.message.fields.raw.ignore_above");

        assertComposableTemplateMatchesIndexMappings(client, ".ml-stats", ".ml-stats-000001", true, statsIndexException, false);
        assertComposableTemplateMatchesIndexMappings(client, ".ml-state", ".ml-state-000001", true, Collections.emptySet(), false);
        // Depending on the order Full Cluster restart tests are run there may not be an notifications index yet
        assertComposableTemplateMatchesIndexMappings(
            client,
            ".ml-notifications-000002",
            ".ml-notifications-000002",
            true,
            notificationsIndexExceptions,
            false
        );
        // .ml-annotations-6 does not use a template
        // .ml-anomalies-shared uses a template but will have dynamically updated mappings as new jobs are opened

        // Dynamic mappings updates are banned for system indices.
        // The .ml-config and .ml-meta indices have mappings that allow dynamic updates.
        // The effect is instant error if a document containing an unknown field is added
        // to one of these indices. Assuming we have some sort of test coverage somewhere
        // for new fields, we will very quickly catch any failures to add new fields to
        // the mappings for the .ml-config and .ml-meta indices. So there is no need to
        // test again here.
    }

    /**
     * Compares the mappings from the legacy template and the index and asserts
     * they are the same. The assertion error message details the differences in
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
     */
    @SuppressWarnings("unchecked")
    public static void assertLegacyTemplateMatchesIndexMappings(
        RestClient client,
        String templateName,
        String indexName,
        boolean notAnErrorIfIndexDoesNotExist,
        Set<String> exceptions,
        boolean allowSystemIndexWarnings
    ) throws Exception {

        AtomicReference<Response> templateResponse = new AtomicReference<>();

        ESRestTestCase.assertBusy(() -> {
            Request getTemplate = new Request("GET", "_template/" + templateName);
            templateResponse.set(client.performRequest(getTemplate));
            assertEquals("missing template [" + templateName + "]", 200, templateResponse.get().getStatusLine().getStatusCode());
        });

        Map<String, Object> templateMappings = (Map<String, Object>) XContentMapValues.extractValue(
            ESRestTestCase.entityAsMap(templateResponse.get()),
            templateName,
            "mappings"
        );
        assertNotNull(templateMappings);

        assertTemplateMatchesIndexMappingsCommon(
            client,
            templateName,
            templateMappings,
            indexName,
            notAnErrorIfIndexDoesNotExist,
            exceptions,
            allowSystemIndexWarnings
        );
    }

    /**
     * Compares the mappings from the composable template and the index and asserts
     * they are the same. The assertion error message details the differences in
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
     */
    @SuppressWarnings("unchecked")
    public static void assertComposableTemplateMatchesIndexMappings(
        RestClient client,
        String templateName,
        String indexName,
        boolean notAnErrorIfIndexDoesNotExist,
        Set<String> exceptions,
        boolean allowSystemIndexWarnings
    ) throws Exception {

        AtomicReference<Response> templateResponse = new AtomicReference<>();

        ESRestTestCase.assertBusy(() -> {
            Request getTemplate = new Request("GET", "_index_template/" + templateName);
            templateResponse.set(client.performRequest(getTemplate));
            assertEquals("missing template [" + templateName + "]", 200, templateResponse.get().getStatusLine().getStatusCode());
        });

        Map<String, Object> templateMappings = ((List<Map<String, Object>>) XContentMapValues.extractValue(
            ESRestTestCase.entityAsMap(templateResponse.get()),
            "index_templates",
            "index_template",
            "template",
            "mappings"
        )).get(0);
        assertNotNull(templateMappings);

        assertTemplateMatchesIndexMappingsCommon(
            client,
            templateName,
            templateMappings,
            indexName,
            notAnErrorIfIndexDoesNotExist,
            exceptions,
            allowSystemIndexWarnings
        );
    }

    @SuppressWarnings("unchecked")
    private static void assertTemplateMatchesIndexMappingsCommon(
        RestClient client,
        String templateName,
        Map<String, Object> templateMappings,
        String indexName,
        boolean notAnErrorIfIndexDoesNotExist,
        Set<String> exceptions,
        boolean allowSystemIndexWarnings
    ) throws IOException {

        Request getIndexMapping = new Request("GET", indexName + "/_mapping");
        if (allowSystemIndexWarnings) {
            final String systemIndexWarning = "this request accesses system indices: ["
                + indexName
                + "], but in a future major version, "
                + "direct access to system indices will be prevented by default";
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
        assertEquals("error getting mappings for index [" + indexName + "]", 200, indexMappingResponse.getStatusLine().getStatusCode());

        Map<String, Object> indexMappings = (Map<String, Object>) XContentMapValues.extractValue(
            ESRestTestCase.entityAsMap(indexMappingResponse),
            indexName,
            "mappings"
        );
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
        keysInTemplateMissingFromIndex.removeAll(exceptions);

        SortedSet<String> keysInIndexMissingFromTemplate = new TreeSet<>(flatIndexMap.keySet());
        keysInIndexMissingFromTemplate.removeAll(flatTemplateMap.keySet());

        // In the case of object fields the 'type: object' mapping is set by default.
        // If this does not explicitly appear in the template it is not an error
        // as ES has added the default to the index mappings
        keysInIndexMissingFromTemplate.removeIf(key -> key.endsWith("type") && "object".equals(flatIndexMap.get(key)));

        // Remove the exceptions
        keysInIndexMissingFromTemplate.removeAll(exceptions);

        StringBuilder errorMesssage = new StringBuilder("Error the template mappings [").append(templateName)
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
            if (Objects.equals(template, index) == false) {
                // Both maybe be booleans but different representations
                if (areBooleanObjectsAndEqual(index, template)) {
                    continue;
                }

                mappingsAreTheSame = false;

                errorMesssage.append("Values for key [").append(key).append("] are different").append(System.lineSeparator());
                errorMesssage.append("    template value [")
                    .append(template)
                    .append("] ")
                    .append(template.getClass().getSimpleName())
                    .append(System.lineSeparator());
                errorMesssage.append("    index value [")
                    .append(index)
                    .append("] ")
                    .append(index.getClass().getSimpleName())
                    .append(System.lineSeparator());
            }
        }

        if (mappingsAreTheSame == false) {
            fail(errorMesssage.toString());
        }
    }

    private static boolean areBooleanObjectsAndEqual(Object a, Object b) {
        Boolean left;
        Boolean right;

        if (a instanceof Boolean) {
            left = (Boolean) a;
        } else if (a instanceof String && isBooleanValueString((String) a)) {
            left = Boolean.parseBoolean((String) a);
        } else {
            return false;
        }

        if (b instanceof Boolean) {
            right = (Boolean) b;
        } else if (b instanceof String && isBooleanValueString((String) b)) {
            right = Boolean.parseBoolean((String) b);
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
        return map.entrySet().stream().flatMap((e) -> extractValue(path, e));
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
