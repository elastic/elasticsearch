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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * This test suite asserts that field-level security produces the SAME _source from a standard index and a logsdb index (synthetic source,
 * with _ignored_source stored in binary doc values), across the randomized mappings and documents the challenge framework generates.
 */
public class StandardVersusLogsdbFieldLevelSecurityChallengeRestIT extends BulkChallengeRestIT {

    public StandardVersusLogsdbFieldLevelSecurityChallengeRestIT() {}

    public void testFieldLevelSecuritySourceEquivalence() throws IOException {
        final int numberOfDocuments = ESTestCase.randomIntBetween(20, 80);

        final List<XContentBuilder> documents = new ArrayList<>();
        // Static starting point so documents are identical between test runs, mirroring the base class' document generation.
        final Instant startingPoint = ZonedDateTime.of(2024, 1, 1, 10, 0, 0, 0, ZoneId.of("UTC")).toInstant();
        for (int i = 0; i < numberOfDocuments; i++) {
            documents.add(generateDocument(startingPoint.plus(i, ChronoUnit.SECONDS)));
        }
        indexDocuments(() -> documents, () -> documents);

        // Deny one field so that, when it lands inside an _ignored_source capture on the logsdb side, FLS must drop that entry and hand
        // back the survivors - the multi-value re-encode path the fix guards. Both indices filter identically, so the sources must match.
        final String deniedField = randomDeniedField();

        final String encoded = createFieldLevelSecurityApiKey(deniedField);

        final SearchSourceBuilder search = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(numberOfDocuments);

        final MatchResult matchResult = Matcher.matchSource()
            .mappings(dataGenerationHelper.mapping().lookup(), getContenderMappings(), getBaselineMappings())
            .settings(getContenderSettings(), getBaselineSettings())
            .expected(querySourcesAsApiKey(getBaselineDataStreamName(), search, encoded))
            .ignoringSort(true)
            .isEqualTo(querySourcesAsApiKey(getContenderDataStreamName(), search, encoded));
        assertTrue("denied field [" + deniedField + "]: " + matchResult.getMessage(), matchResult.isMatch());
    }

    /**
     * Picks a concrete field to deny. Prefers the mapping template's leaf paths (precise, dotted), but the challenge framework sometimes
     * uses a fully dynamic mapping with no predefined fields, so it falls back to a top-level field read from an indexed document.
     */
    private String randomDeniedField() throws IOException {
        final List<String> templateFields = new ArrayList<>(dataGenerationHelper.getTemplateFieldTypes().keySet());
        templateFields.remove("@timestamp");
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

    private String createFieldLevelSecurityApiKey(final String deniedField) throws IOException {
        final Request request = new Request("POST", "/_security/api_key");
        request.setJsonEntity(Strings.format("""
            {
              "name": "fls-challenge",
              "role_descriptors": {
                "role": {
                  "indices": [
                    {
                      "names": [ "%s", "%s" ],
                      "privileges": [ "read" ],
                      "field_security": { "grant": [ "*" ], "except": [ "%s" ] }
                    }
                  ]
                }
              }
            }""", getBaselineDataStreamName(), getContenderDataStreamName(), deniedField));
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
