/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.enrich.EnrichProcessorFactory.EnrichSpecification;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class EnrichProcessorFactoryTests extends ESTestCase {

    public void testCreateProcessorInstance() throws Exception {
        List<String> enrichValues = Arrays.asList("globalRank", "tldRank", "tld");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, Collections.singletonList("source_index"), "my_key",
            enrichValues);
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null);
        factory.policies = Collections.singletonMap("majestic", policy);

        Map<String, Object> config = new HashMap<>();
        config.put("policy_name", "majestic");
        config.put("enrich_key", "host");
        boolean keyIgnoreMissing = randomBoolean();
        if (keyIgnoreMissing || randomBoolean()) {
            config.put("ignore_missing", keyIgnoreMissing);
        }

        int numRandomValues = randomIntBetween(1, 8);
        List<Tuple<String, String>> randomValues = new ArrayList<>(numRandomValues);
        for (int i = 0; i < numRandomValues; i++) {
            randomValues.add(new Tuple<>(randomFrom(enrichValues), randomAlphaOfLength(4)));
        }

        List<Map<String, Object>> valuesConfig = new ArrayList<>(numRandomValues);
        for (Tuple<String, String> tuple : randomValues) {
            Map<String, Object> entry = new HashMap<>();
            entry.put("source", tuple.v1());
            entry.put("target", tuple.v2());
            valuesConfig.add(entry);
        }
        config.put("enrich_values", valuesConfig);

        ExactMatchProcessor result = (ExactMatchProcessor) factory.create(Collections.emptyMap(), "_tag", config);
        assertThat(result, notNullValue());
        assertThat(result.getPolicyName(), equalTo("majestic"));
        assertThat(result.getEnrichKey(), equalTo("host"));
        assertThat(result.isIgnoreMissing(), is(keyIgnoreMissing));
        assertThat(result.getSpecifications().size(), equalTo(numRandomValues));
        for (int i = 0; i < numRandomValues; i++) {
            EnrichSpecification actual = result.getSpecifications().get(i);
            Tuple<String, String> expected = randomValues.get(i);
            assertThat(actual.sourceField, equalTo(expected.v1()));
            assertThat(actual.targetField, equalTo(expected.v2()));
        }
    }

    public void testPolicyDoesNotExist() {
        List<String> enrichValues = Arrays.asList("globalRank", "tldRank", "tld");
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null);

        Map<String, Object> config = new HashMap<>();
        config.put("policy_name", "majestic");
        config.put("enrich_key", "host");
        boolean keyIgnoreMissing = randomBoolean();
        if (keyIgnoreMissing || randomBoolean()) {
            config.put("ignore_missing", keyIgnoreMissing);
        }

        int numRandomValues = randomIntBetween(1, 8);
        List<Tuple<String, String>> randomValues = new ArrayList<>(numRandomValues);
        for (int i = 0; i < numRandomValues; i++) {
            randomValues.add(new Tuple<>(randomFrom(enrichValues), randomAlphaOfLength(4)));
        }

        List<Map<String, Object>> valuesConfig = new ArrayList<>(numRandomValues);
        for (Tuple<String, String> tuple : randomValues) {
            Map<String, Object> entry = new HashMap<>();
            entry.put("source", tuple.v1());
            entry.put("target", tuple.v2());
            valuesConfig.add(entry);
        }
        config.put("enrich_values", valuesConfig);

        Exception e = expectThrows(IllegalArgumentException.class, () -> factory.create(Collections.emptyMap(), "_tag", config));
        assertThat(e.getMessage(), equalTo("policy [majestic] does not exists"));
    }

    public void testPolicyNameMissing() {
        List<String> enrichValues = Arrays.asList("globalRank", "tldRank", "tld");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, Collections.singletonList("source_index"), "my_key",
            enrichValues);
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null);
        factory.policies = Collections.singletonMap("_name", policy);

        Map<String, Object> config = new HashMap<>();
        config.put("enrich_key", "host");
        boolean keyIgnoreMissing = randomBoolean();
        if (keyIgnoreMissing || randomBoolean()) {
            config.put("ignore_missing", keyIgnoreMissing);
        }

        int numRandomValues = randomIntBetween(1, 8);
        List<Tuple<String, String>> randomValues = new ArrayList<>(numRandomValues);
        for (int i = 0; i < numRandomValues; i++) {
            randomValues.add(new Tuple<>(randomFrom(enrichValues), randomAlphaOfLength(4)));
        }

        List<Map<String, Object>> valuesConfig = new ArrayList<>(numRandomValues);
        for (Tuple<String, String> tuple : randomValues) {
            Map<String, Object> entry = new HashMap<>();
            entry.put("source", tuple.v1());
            entry.put("target", tuple.v2());
            valuesConfig.add(entry);
        }
        config.put("enrich_values", valuesConfig);

        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(Collections.emptyMap(), "_tag", config));
        assertThat(e.getMessage(), equalTo("[policy_name] required property is missing"));
    }

    public void testUnsupportedPolicy() {
        List<String> enrichValues = Arrays.asList("globalRank", "tldRank", "tld");
        EnrichPolicy policy =
            new EnrichPolicy("unsupported", null, Collections.singletonList("source_index"), "my_key", enrichValues);
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null);
        factory.policies = Collections.singletonMap("majestic", policy);

        Map<String, Object> config = new HashMap<>();
        config.put("policy_name", "majestic");
        config.put("enrich_key", "host");
        boolean keyIgnoreMissing = randomBoolean();
        if (keyIgnoreMissing || randomBoolean()) {
            config.put("ignore_missing", keyIgnoreMissing);
        }

        int numRandomValues = randomIntBetween(1, 8);
        List<Tuple<String, String>> randomValues = new ArrayList<>(numRandomValues);
        for (int i = 0; i < numRandomValues; i++) {
            randomValues.add(new Tuple<>(randomFrom(enrichValues), randomAlphaOfLength(4)));
        }

        List<Map<String, Object>> valuesConfig = new ArrayList<>(numRandomValues);
        for (Tuple<String, String> tuple : randomValues) {
            Map<String, Object> entry = new HashMap<>();
            entry.put("source", tuple.v1());
            entry.put("target", tuple.v2());
            valuesConfig.add(entry);
        }
        config.put("enrich_values", valuesConfig);

        Exception e = expectThrows(IllegalArgumentException.class, () -> factory.create(Collections.emptyMap(), "_tag", config));
        assertThat(e.getMessage(), equalTo("unsupported policy type [unsupported]"));
    }

    public void testNonExistingDecorateField() {
        List<String> enrichValues = Arrays.asList("globalRank", "tldRank", "tld");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, Collections.singletonList("source_index"), "my_key",
            enrichValues);
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null);
        factory.policies = Collections.singletonMap("majestic", policy);

        Map<String, Object> config = new HashMap<>();
        config.put("policy_name", "majestic");
        config.put("enrich_key", "host");

        Map<String, Object> entry = new HashMap<>();
        entry.put("source", "rank");
        entry.put("target", "rank");
        List<Map<String, Object>> valuesConfig = Collections.singletonList(entry);
        config.put("enrich_values", valuesConfig);

        Exception e = expectThrows(IllegalArgumentException.class, () -> factory.create(Collections.emptyMap(), "_tag", config));
        assertThat(e.getMessage(), equalTo("source field [rank] does not exist in policy [majestic]"));
    }

}
