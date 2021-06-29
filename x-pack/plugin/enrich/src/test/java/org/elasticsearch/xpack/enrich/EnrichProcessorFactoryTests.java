/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

public class EnrichProcessorFactoryTests extends ESTestCase {

    private ScriptService scriptService;

    @Before
    public void initializeScriptService() {
        scriptService = mock(ScriptService.class);
    }

    public void testCreateProcessorInstance() throws Exception {
        List<String> enrichValues = List.of("globalRank", "tldRank", "tld");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of("source_index"), "my_key", enrichValues);
        try (Client client = new NoOpClient(this.getClass().getSimpleName() + "TestClient")) {
            EnrichProcessorFactory factory = new EnrichProcessorFactory(client, scriptService);
            factory.metadata = createMetadata("majestic", policy);

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

            Integer maxMatches = null;
            if (randomBoolean()) {
                maxMatches = randomIntBetween(1, 128);
                config.put("max_matches", maxMatches);
            }

            int numRandomValues = randomIntBetween(1, 8);
            List<Tuple<String, String>> randomValues = new ArrayList<>(numRandomValues);
            for (int i = 0; i < numRandomValues; i++) {
                randomValues.add(new Tuple<>(randomFrom(enrichValues), randomAlphaOfLength(4)));
            }

            MatchProcessor result = (MatchProcessor) factory.create(Collections.emptyMap(), "_tag", null, config);
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
            if (maxMatches != null) {
                assertThat(result.getMaxMatches(), equalTo(maxMatches));
            } else {
                assertThat(result.getMaxMatches(), equalTo(1));
            }
        }
    }

    public void testPolicyDoesNotExist() {
        List<String> enrichValues = List.of("globalRank", "tldRank", "tld");
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null, scriptService);
        factory.metadata = Metadata.builder().build();

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
            valuesConfig.add(Map.of("source", tuple.v1(), "target", tuple.v2()));
        }
        config.put("set_from", valuesConfig);

        Exception e = expectThrows(IllegalArgumentException.class, () -> factory.create(Collections.emptyMap(), "_tag", null, config));
        assertThat(e.getMessage(), equalTo("no enrich index exists for policy with name [majestic]"));
    }

    public void testPolicyNameMissing() {
        List<String> enrichValues = List.of("globalRank", "tldRank", "tld");
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null, scriptService);

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
            valuesConfig.add(Map.of("source", tuple.v1(), "target", tuple.v2()));
        }
        config.put("set_from", valuesConfig);

        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(Collections.emptyMap(), "_tag", null, config));
        assertThat(e.getMessage(), equalTo("[policy_name] required property is missing"));
    }

    public void testUnsupportedPolicy() throws Exception {
        List<String> enrichValues = List.of("globalRank", "tldRank", "tld");
        EnrichPolicy policy = new EnrichPolicy("unsupported", null, List.of("source_index"), "my_key", enrichValues);
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null, scriptService);
        factory.metadata = createMetadata("majestic", policy);

        Map<String, Object> config = new HashMap<>();
        config.put("policy_name", "majestic");
        config.put("field", "host");
        config.put("target_field", "entry");
        boolean keyIgnoreMissing = randomBoolean();
        if (keyIgnoreMissing || randomBoolean()) {
            config.put("ignore_missing", keyIgnoreMissing);
        }

        Exception e = expectThrows(IllegalArgumentException.class, () -> factory.create(Collections.emptyMap(), "_tag", null, config));
        assertThat(e.getMessage(), equalTo("unsupported policy type [unsupported]"));
    }

    public void testCompactEnrichValuesFormat() throws Exception {
        List<String> enrichValues = List.of("globalRank", "tldRank", "tld");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of("source_index"), "host", enrichValues);
        try (Client client = new NoOpClient(this.getClass().getSimpleName() + "TestClient")) {
            EnrichProcessorFactory factory = new EnrichProcessorFactory(client, scriptService);
            factory.metadata = createMetadata("majestic", policy);

            Map<String, Object> config = new HashMap<>();
            config.put("policy_name", "majestic");
            config.put("field", "host");
            config.put("target_field", "entry");

            MatchProcessor result = (MatchProcessor) factory.create(Collections.emptyMap(), "_tag", null, config);
            assertThat(result, notNullValue());
            assertThat(result.getPolicyName(), equalTo("majestic"));
            assertThat(result.getField(), equalTo("host"));
            assertThat(result.getTargetField(), equalTo("entry"));
        }
    }

    public void testNoTargetField() throws Exception {
        List<String> enrichValues = List.of("globalRank", "tldRank", "tld");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of("source_index"), "host", enrichValues);
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null, scriptService);
        factory.metadata = createMetadata("majestic", policy);

        Map<String, Object> config1 = new HashMap<>();
        config1.put("policy_name", "majestic");
        config1.put("field", "host");

        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(Collections.emptyMap(), "_tag", null, config1));
        assertThat(e.getMessage(), equalTo("[target_field] required property is missing"));
    }

    public void testIllegalMaxMatches() throws Exception {
        List<String> enrichValues = List.of("globalRank", "tldRank", "tld");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of("source_index"), "my_key", enrichValues);
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null, scriptService);
        factory.metadata = createMetadata("majestic", policy);

        Map<String, Object> config = new HashMap<>();
        config.put("policy_name", "majestic");
        config.put("field", "host");
        config.put("target_field", "entry");
        config.put("max_matches", randomBoolean() ? between(-2048, 0) : between(129, 2048));

        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(Collections.emptyMap(), "_tag", null, config));
        assertThat(e.getMessage(), equalTo("[max_matches] should be between 1 and 128"));
    }

    static Metadata createMetadata(String name, EnrichPolicy policy) throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        IndexMetadata.Builder builder = IndexMetadata.builder(EnrichPolicy.getBaseName(name) + "-1");
        builder.settings(settings);
        builder.putMapping(
            "{\"_meta\": {\"enrich_match_field\": \""
                + policy.getMatchField()
                + "\", \"enrich_policy_type\": \""
                + policy.getType()
                + "\"}}"
        );
        builder.putAlias(AliasMetadata.builder(EnrichPolicy.getBaseName(name)).build());
        return Metadata.builder().put(builder).build();
    }

}
