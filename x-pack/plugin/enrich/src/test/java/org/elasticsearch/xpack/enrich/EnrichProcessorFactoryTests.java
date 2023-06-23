/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.enrich.action.EnrichCoordinatorProxyAction;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class EnrichProcessorFactoryTests extends ESTestCase {

    private ScriptService scriptService;
    private EnrichCache enrichCache = new EnrichCache(0L);

    @Before
    public void initializeScriptService() {
        scriptService = mock(ScriptService.class);
    }

    public void testCreateProcessorInstance() throws Exception {
        List<String> enrichValues = List.of("globalRank", "tldRank", "tld");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of("source_index"), "my_key", enrichValues);
        try (Client client = new NoOpClient(this.getClass().getSimpleName() + "TestClient")) {
            EnrichProcessorFactory factory = new EnrichProcessorFactory(client, scriptService, enrichCache);
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
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null, scriptService, enrichCache);
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
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null, scriptService, enrichCache);

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
        try (Client client = new NoOpClient(this.getClass().getSimpleName() + "TestClient")) {
            EnrichProcessorFactory factory = new EnrichProcessorFactory(client, scriptService, enrichCache);
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
    }

    public void testCompactEnrichValuesFormat() throws Exception {
        List<String> enrichValues = List.of("globalRank", "tldRank", "tld");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of("source_index"), "host", enrichValues);
        try (Client client = new NoOpClient(this.getClass().getSimpleName() + "TestClient")) {
            EnrichProcessorFactory factory = new EnrichProcessorFactory(client, scriptService, enrichCache);
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
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null, scriptService, enrichCache);
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
        EnrichProcessorFactory factory = new EnrichProcessorFactory(null, scriptService, enrichCache);
        factory.metadata = createMetadata("majestic", policy);

        Map<String, Object> config = new HashMap<>();
        config.put("policy_name", "majestic");
        config.put("field", "host");
        config.put("target_field", "entry");
        config.put("max_matches", randomBoolean() ? between(-2048, 0) : between(129, 2048));

        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(Collections.emptyMap(), "_tag", null, config));
        assertThat(e.getMessage(), equalTo("[max_matches] should be between 1 and 128"));
    }

    public void testCaching() throws Exception {
        int[] requestCounter = new int[1];
        enrichCache = new EnrichCache(100L);
        List<String> enrichValues = List.of("globalRank", "tldRank", "tld");
        EnrichPolicy policy = new EnrichPolicy(EnrichPolicy.MATCH_TYPE, null, List.of("source_index"), "host", enrichValues);
        try (Client client = new NoOpClient(this.getClass().getSimpleName() + "testCaching") {

            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                assert EnrichCoordinatorProxyAction.NAME.equals(action.name());
                var emptyResponse = new SearchResponse(
                    new InternalSearchResponse(
                        new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                        InternalAggregations.EMPTY,
                        new Suggest(Collections.emptyList()),
                        new SearchProfileResults(Collections.emptyMap()),
                        false,
                        false,
                        1
                    ),
                    "",
                    1,
                    1,
                    0,
                    0,
                    ShardSearchFailure.EMPTY_ARRAY,
                    SearchResponse.Clusters.EMPTY
                );
                requestCounter[0]++;
                listener.onResponse((Response) emptyResponse);
            }
        }) {
            EnrichProcessorFactory factory = new EnrichProcessorFactory(client, scriptService, enrichCache);
            factory.accept(ClusterState.builder(new ClusterName("_name")).metadata(createMetadata("majestic", policy)).build());

            Map<String, Object> config = new HashMap<>();
            config.put("policy_name", "majestic");
            config.put("field", "domain");
            config.put("target_field", "entry");
            IngestDocument ingestDocument = new IngestDocument(
                "_index",
                "_id",
                1L,
                "_routing",
                VersionType.INTERNAL,
                Map.of("domain", "elastic.co")
            );
            MatchProcessor processor = (MatchProcessor) factory.create(Collections.emptyMap(), "_tag", null, config);

            // A search is performed and that is cached:
            IngestDocument[] result = new IngestDocument[1];
            Exception[] failure = new Exception[1];
            processor.execute(ingestDocument, (r, e) -> {
                result[0] = r;
                failure[0] = e;
            });
            assertThat(failure[0], nullValue());
            assertThat(result[0], notNullValue());
            assertThat(requestCounter[0], equalTo(1));
            assertThat(enrichCache.getStats("_id").getCount(), equalTo(1L));
            assertThat(enrichCache.getStats("_id").getMisses(), equalTo(1L));
            assertThat(enrichCache.getStats("_id").getHits(), equalTo(0L));
            assertThat(enrichCache.getStats("_id").getEvictions(), equalTo(0L));

            // No search is performed, result is read from the cache:
            result[0] = null;
            failure[0] = null;
            processor.execute(ingestDocument, (r, e) -> {
                result[0] = r;
                failure[0] = e;
            });
            assertThat(failure[0], nullValue());
            assertThat(result[0], notNullValue());
            assertThat(requestCounter[0], equalTo(1));
            assertThat(enrichCache.getStats("_id").getCount(), equalTo(1L));
            assertThat(enrichCache.getStats("_id").getMisses(), equalTo(1L));
            assertThat(enrichCache.getStats("_id").getHits(), equalTo(1L));
            assertThat(enrichCache.getStats("_id").getEvictions(), equalTo(0L));
        }
    }

    static Metadata createMetadata(String name, EnrichPolicy policy) {
        IndexMetadata.Builder builder = IndexMetadata.builder(EnrichPolicy.getBaseName(name) + "-1");
        builder.settings(indexSettings(Version.CURRENT, 1, 0));
        builder.putMapping(Strings.format("""
            {"_meta": {"enrich_match_field": "%s", "enrich_policy_type": "%s"}}
            """, policy.getMatchField(), policy.getType()));
        builder.putAlias(AliasMetadata.builder(EnrichPolicy.getBaseName(name)).build());
        return Metadata.builder().put(builder).build();
    }

}
