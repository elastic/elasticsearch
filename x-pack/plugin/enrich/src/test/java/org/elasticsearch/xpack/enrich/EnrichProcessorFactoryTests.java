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
        config.put("field", "host");
        config.put("target_field", "entry");
        boolean keyIgnoreMissing = randomBoolean();
        if (keyIgnoreMissing || randomBoolean()) {
            config.put("ignore_missing", keyIgnoreMissing);
        }

        Boolean overrideEnabled = randomBoolean() ? null : randomBoolean();
        if (overrideEnabled != null) {
            config.put("override", overrideEnabled);
        }

        int numRandomValues = randomIntBetween(1, 8);
        List<Tuple<String, String>> randomValues = new ArrayList<>(numRandomValues);
        for (int i = 0; i < numRandomValues; i++) {
            randomValues.add(new Tuple<>(randomFrom(enrichValues), randomAlphaOfLength(4)));
        }

        ExactMatchProcessor result = (ExactMatchProcessor) factory.create(Collections.emptyMap(), "_tag", config);
        assertThat(result, notNullValue());
        assertThat(result.getPolicyName(), equalTo("majestic"));
        assertThat(result.getField(), equalTo("host"));
        assertThat(result.getTargetField(), equalTo("entry"));
        assertThat(result.getMatchField(), equalTo("my_key"));
        assertThat(result.isIgnoreMissing(), is(keyIgnoreMissing));
        if (overrideEnabled != null) {
            assertThat(result.isOverrideEnabled(), is(overrideEnabled));
        } else {
            assertThat(result.isOverrideEnabled(), is(true));
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
        config.put("set_from", valuesConfig);

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
        config.put("set_from", valuesConfig);

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
        config.put("field", "host");
        config.put("target_field", "entry");
        boolean keyIgnoreMissing = randomBoolean();
        if (keyIgnoreMissing || randomBoolean()) {
            config.put("ignore_missing", keyIgnoreMissing);
        }

        Exception e = expectThrows(IllegalArgumentException.class, () -> factory.create(Collections.emptyMap(), "_tag", config));
        assertThat(e.getMessage(), equalTo("unsupported policy type [unsupported]"));
    }

    public void testCompactEnrichValuesFormat() throws Exception {
        List<String> enrichValues = Arrays.asList("globalRank", "tldRank", "tld");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null,
            Collections.singletonList("source_index"), "host", enrichValues);
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null);
        factory.policies = Collections.singletonMap("majestic", policy);

        Map<String, Object> config = new HashMap<>();
        config.put("policy_name", "majestic");
        config.put("field", "host");
        config.put("target_field", "entry");

        ExactMatchProcessor result = (ExactMatchProcessor) factory.create(Collections.emptyMap(), "_tag", config);
        assertThat(result, notNullValue());
        assertThat(result.getPolicyName(), equalTo("majestic"));
        assertThat(result.getField(), equalTo("host"));
        assertThat(result.getTargetField(), equalTo("entry"));
    }

    public void testNoTargetField() throws Exception {
        List<String> enrichValues = Arrays.asList("globalRank", "tldRank", "tld");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null,
            Collections.singletonList("source_index"), "host", enrichValues);
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null);
        factory.policies = Collections.singletonMap("majestic", policy);

        Map<String, Object> config1 = new HashMap<>();
        config1.put("policy_name", "majestic");
        config1.put("field", "host");

        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(Collections.emptyMap(), "_tag", config1));
        assertThat(e.getMessage(), equalTo("[target_field] required property is missing"));
    }

}
