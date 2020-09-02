/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.test.SecuritySettingsSourceField;
import org.junit.Before;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.test.SecuritySettingsSourceField.basicAuthHeaderValue;

public abstract class AbstractUpgradeTestCase extends ESRestTestCase {

    private static final String BASIC_AUTH_VALUE =
            basicAuthHeaderValue("test_user", SecuritySettingsSourceField.TEST_PASSWORD);

    protected static final Version UPGRADE_FROM_VERSION =
        Version.fromString(System.getProperty("tests.upgrade_from_version"));

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveRollupJobsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveILMPoliciesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveDataStreamsUponCompletion() {
        return true;
    }

    enum ClusterType {
        OLD,
        MIXED,
        UPGRADED;

        public static ClusterType parse(String value) {
            switch (value) {
                case "old_cluster":
                    return OLD;
                case "mixed_cluster":
                    return MIXED;
                case "upgraded_cluster":
                    return UPGRADED;
                default:
                    throw new AssertionError("unknown cluster type: " + value);
            }
        }
    }

    protected static final ClusterType CLUSTER_TYPE = ClusterType.parse(System.getProperty("tests.rest.suite"));

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE)
                .build();
    }

    protected Collection<String> templatesToWaitFor() {
        return Collections.emptyList();
    }

    @Before
    public void setupForTests() throws Exception {
        final Collection<String> expectedTemplates = templatesToWaitFor();

        if (expectedTemplates.isEmpty()) {
            return;
        }

        assertBusy(() -> {
            final Request catRequest = new Request("GET", "_cat/templates?h=n&s=n");
            final Response catResponse = adminClient().performRequest(catRequest);

            final List<String> templates = Streams.readAllLines(catResponse.getEntity().getContent());

            final List<String> missingTemplates = expectedTemplates.stream()
                .filter(each -> templates.contains(each) == false)
                .collect(Collectors.toList());

            // While it's possible to use a Hamcrest matcher for this, the failure is much less legible.
            if (missingTemplates.isEmpty() == false) {
                fail("Some expected templates are missing: " + missingTemplates + ". The templates that exist are: " + templates + "");
            }
        });
    }

    /**
     * Compares the mappings from the template and the index and asserts they
     * are the same.
     *
     * The test is intended to catch cases where an index mapping has been
     * updated dynamically or a write occurred before the template was put.
     * The assertion error message details the differences in the mappings.
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
     * @param templateName The template
     * @param indexName The index
     * @param notAnErrorIfIndexDoesNotExist The index may or may not have been created from
     *                                      the template. If {@code true} then the missing
     *                                      index does not cause an error
     * @param exceptions List of keys to ignore in the index mappings.
     *                   The key is a '.' separated path.
     * @throws IOException Yes
     */
    @SuppressWarnings("unchecked")
    protected void assertLegacyTemplateMatchesIndexMappings(String templateName,
                                                            String indexName,
                                                            boolean notAnErrorIfIndexDoesNotExist,
                                                            Set<String> exceptions) throws IOException {

        Request getTemplate = new Request("GET", "_template/" + templateName);
        Response templateResponse = client().performRequest(getTemplate);
        assertEquals("missing template [" + templateName + "]", 200, templateResponse.getStatusLine().getStatusCode());

        Map<String, Object> templateMappings = (Map<String, Object>) XContentMapValues.extractValue(entityAsMap(templateResponse),
            templateName, "mappings");
        assertNotNull(templateMappings);

        Request getIndexMapping = new Request("GET", indexName + "/_mapping");
        Response indexMappingResponse;
        try {
            indexMappingResponse = client().performRequest(getIndexMapping);
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404 && notAnErrorIfIndexDoesNotExist) {
                return;
            } else {
                throw e;
            }
        }
        assertEquals("error getting mappings for index [" + indexName + "]",
            200, indexMappingResponse.getStatusLine().getStatusCode());

        Map<String, Object> indexMappings = (Map<String, Object>) XContentMapValues.extractValue(entityAsMap(indexMappingResponse),
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

    protected void assertLegacyTemplateMatchesIndexMappings(String templateName,
                                                            String indexName) throws IOException {
        assertLegacyTemplateMatchesIndexMappings(templateName, indexName, false, Collections.emptySet());
    }

    private boolean areBooleanObjectsAndEqual(Object a, Object b) {
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

    /* Boolean.parseBoolean is not strict anything that isn't
     * "true" is returned as false. Here we want to know if
     * s is a boolean.
     */
    private boolean isBooleanValueString(String s) {
        return s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false");
    }

    private Map<String, Object> flattenMap(Map<String, Object> map) {
        return new TreeMap<>(flatten("", map).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    private Stream<Map.Entry<String, Object>> flatten(String path, Map<String, Object> map) {
        return map.entrySet()
            .stream()
            .flatMap((e) -> extractValue(path, e));
    }

    @SuppressWarnings("unchecked")
    private Stream<Map.Entry<String, Object>> extractValue(String path, Map.Entry<String, Object> entry) {
        String nextPath = path.isEmpty() ? entry.getKey() : path + "." + entry.getKey();
        if (entry.getValue() instanceof Map<?, ?>) {
            return flatten(nextPath, (Map<String, Object>) entry.getValue());
        } else {
            return Stream.of(new AbstractMap.SimpleEntry<>(nextPath, entry.getValue()));
        }
    }
}
