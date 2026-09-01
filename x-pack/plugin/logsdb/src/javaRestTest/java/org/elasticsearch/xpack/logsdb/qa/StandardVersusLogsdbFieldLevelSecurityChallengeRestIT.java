/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.datageneration.matchers.MatchResult;
import org.elasticsearch.datageneration.matchers.Matcher;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.ClassRule;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This test suite asserts that field-level security produces the SAME _source from a standard index and a logsdb index (synthetic source,
 * with _ignored_source stored in binary doc values), across the randomized mappings and documents the challenge framework generates.
 */
public class StandardVersusLogsdbFieldLevelSecurityChallengeRestIT extends BulkChallengeRestIT {

    /**
     * Leaf types stored in {@code _source} as a nested object (geo_point as {@code {lat,lon}}/GeoJSON, geo_shape/shape as GeoJSON). A
     * standard index filters {@code _source} by exact leaf path, so an {@code except:[field]} rule leaves these intact: the value survives
     * via its ungated {@code field.lat}/{@code field.coordinates} sub-paths. logsdb rebuilds each from a single name-keyed doc-values field
     * that FLS does hide, so it drops the field and the two sources diverge. This is a standard-index FLS leak, not a logsdb bug, so we
     * exclude these types from the denied-field candidates rather than assert an equivalence that cannot hold.
     */
    private static final Set<String> DENY_INCOMPATIBLE_FIELD_TYPES = Set.of("geo_point", "geo_shape", "shape");

    @ClassRule()
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .module("data-streams")
        .module("x-pack-stack")
        .user("test_admin", "x-pack-test-password")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("xpack.security.http.ssl.enabled", "false")
        .setting("xpack.security.transport.ssl.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("cluster.logsdb.enabled", "true")
        .setting("xpack.ml.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public StandardVersusLogsdbFieldLevelSecurityChallengeRestIT() {
        super(new DataGenerationHelper(builder -> builder.withMaxFieldCountPerLevel(30), false));
    }

    public void testFieldLevelSecuritySourceEquivalence() throws IOException {
        final int numberOfDocuments = ESTestCase.randomIntBetween(20, 80);

        final List<XContentBuilder> documents = new ArrayList<>();
        // Static starting point so documents are identical between test runs, mirroring the base class' document generation.
        final Instant startingPoint = ZonedDateTime.of(2024, 1, 1, 10, 0, 0, 0, ZoneId.of("UTC")).toInstant();
        for (int i = 0; i < numberOfDocuments; i++) {
            documents.add(generateDocument(startingPoint.plus(i, ChronoUnit.SECONDS)));
        }
        indexDocuments(() -> documents, () -> documents);

        // Target one field so that, when it lands inside an _ignored_source capture on the logsdb side, FLS must drop entries and re-encode
        // the survivors. Also, randomize the polarity: excluding the field exercises the exclude automaton, while granting only it
        // exercises the include automaton. Both indices filter identically, so the sources must match either way.
        final String targetField = randomDeniedField();
        final boolean grantOnly = randomBoolean();

        final String encoded = createFieldLevelSecurityApiKey(targetField, grantOnly);

        final SearchSourceBuilder search = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(numberOfDocuments);

        final MatchResult matchResult = Matcher.matchSource()
            .mappings(dataGenerationHelper.mapping().lookup(), getContenderMappings(), getBaselineMappings())
            .settings(getContenderSettings(), getBaselineSettings())
            .expected(querySourcesAsApiKey(getBaselineDataStreamName(), search, encoded))
            .ignoringSort(true)
            .isEqualTo(querySourcesAsApiKey(getContenderDataStreamName(), search, encoded));
        final String policy = grantOnly ? "grant-only" : "except";
        assertTrue("target field [" + targetField + "] policy [" + policy + "]: " + matchResult.getMessage(), matchResult.isMatch());
    }

    /**
     * Picks a concrete field to deny. Prefers the mapping template's leaf paths (precise, dotted), skipping types whose {@code _source}
     * form the standard index cannot filter identically to logsdb (see {@link #DENY_INCOMPATIBLE_FIELD_TYPES}), but the challenge
     * framework sometimes uses a fully dynamic mapping with no predefined fields, so it falls back to a top-level field from a document.
     */
    private String randomDeniedField() throws IOException {
        final List<String> templateFields = new ArrayList<>();
        for (final Map.Entry<String, String> field : dataGenerationHelper.getTemplateFieldTypes().entrySet()) {
            if ("@timestamp".equals(field.getKey()) == false && DENY_INCOMPATIBLE_FIELD_TYPES.contains(field.getValue()) == false) {
                templateFields.add(field.getKey());
            }
        }
        if (templateFields.isEmpty() == false) {
            return randomFrom(templateFields);
        }

        final SearchSourceBuilder search = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(100);
        final Request request = new Request("GET", "/" + getBaselineDataStreamName() + "/_search");
        request.setJsonEntity(Strings.toString(search));
        final Response response = client.performRequest(request);
        assertOK(response);
        final List<String> fields = new ArrayList<>();
        for (final Map<String, Object> source : getSources(response)) {
            for (final String key : source.keySet()) {
                if ("@timestamp".equals(key) == false && fields.contains(key) == false) {
                    fields.add(key);
                }
            }
        }
        assertFalse("expected at least one non-timestamp field in the indexed documents", fields.isEmpty());
        return randomFrom(fields);
    }

    private String createFieldLevelSecurityApiKey(final String targetField, final boolean grantOnly) throws IOException {
        // Build via XContentBuilder so randomized field_security, field and index names with control characters are correctly JSON-escaped.
        final XContentBuilder body = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .field("name", "fls-challenge")
            .startObject("role_descriptors")
            .startObject("role")
            .startArray("indices")
            .startObject()
            .array("names", getBaselineDataStreamName(), getContenderDataStreamName())
            .array("privileges", "read")
            .startObject("field_security");
        // grantOnly grants just @timestamp plus the target field (an include filter that drops everything else); otherwise grant all
        // fields except the target (an exclude filter). @timestamp is always granted so the routing/sort field survives both polarities.
        if (grantOnly) {
            body.array("grant", "@timestamp", targetField);
        } else {
            body.array("grant", "*").array("except", targetField);
        }
        body.endObject().endObject().endArray().endObject().endObject().endObject();

        final Request request = new Request("POST", "/_security/api_key");
        request.setJsonEntity(Strings.toString(body));
        final Response response = client.performRequest(request);
        assertOK(response);

        return (String) entityAsMap(response).get("encoded");
    }

    private List<Map<String, Object>> querySourcesAsApiKey(final String dataStream, final SearchSourceBuilder search, final String encoded)
        throws IOException {
        final Request request = new Request("GET", "/" + dataStream + "/_search");
        request.setJsonEntity(Strings.toString(search));
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + encoded));
        final Response response = client.performRequest(request);
        assertOK(response);
        return getSources(response);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> getSources(final Response response) throws IOException {
        final Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), response.getEntity().getContent(), true);
        final Map<String, Object> hits = (Map<String, Object>) map.get("hits");
        final List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        return hitList.stream()
            .sorted(Comparator.comparing(hit -> (String) hit.get("_id")))
            .map(hit -> (Map<String, Object>) hit.get("_source"))
            .toList();
    }
}
