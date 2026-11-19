/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.xcontent.JsonSchemaTestValidator;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * Validates a real query's production response against the published profile schema.
 *
 * <p>This protects both sides of the contract: profile values produced by execution and the response serialization path
 * exposed to clients. Focused unit tests cover individual schema definitions and registry completeness.
 */
public class EsqlProfileSchemaIT extends AbstractEsqlIntegTestCase {

    private static final String SCHEMA_RESOURCE = "/esql_profile.schema.json";

    public void testProfileResponseMatchesPublishedSchema() throws IOException {
        createTestIndex();
        assertProfileMatchesSchema("FROM test | STATS total = SUM(v), count = COUNT(*) BY host | SORT total DESC | LIMIT 10", true);
    }

    /** A FORK over subqueries has merges, with a driver for each branch, and their exchanges. */
    public void testForkProfileMatchesPublishedSchema() throws IOException {
        createTestIndex();
        assertProfileMatchesSchema("""
            FROM (FROM test | WHERE v <= 3), (FROM (FROM test | WHERE v == 4), (FROM test | WHERE v == 5))
            | FORK (WHERE v <= 2) (WHERE v > 2 AND v < 5) (WHERE v >= 5)
            | KEEP _fork, v
            | SORT _fork, v
            """, false);
    }

    /** A query that runs in several phases labels its drivers and plans with them. */
    public void testMultiPhaseProfileMatchesPublishedSchema() throws IOException {
        createTestIndex();
        assertProfileMatchesSchema("""
            FROM test
            | INLINE STATS c = COUNT(*) BY host
            | WHERE v IN (FROM test | WHERE v < 5 | KEEP v)
            | KEEP v, c
            """, false);
    }

    private void assertProfileMatchesSchema(String query, boolean expectOperatorStatus) throws IOException {
        EsqlQueryRequest request = syncEsqlQueryRequest(query);
        request.profile(true);
        try (EsqlQueryResponse response = run(request)) {
            assertFalse(response.profile().drivers().isEmpty());
            assertFalse(response.profile().plans().isEmpty());
            if (expectOperatorStatus) {
                assertTrue(
                    response.profile()
                        .drivers()
                        .stream()
                        .flatMap(driver -> driver.operators().stream())
                        .anyMatch(operator -> operator.status() != null)
                );
            }

            Map<String, Object> renderedResponse = XContentTestUtils.convertToMap(response);
            Map<String, Object> renderedProfile = object(renderedResponse.get("profile"));
            List<String> errors = JsonSchemaTestValidator.draft7(schema())
                .validate(XContentTestUtils.convertToXContent(renderedProfile, XContentType.JSON));
            assertEquals(
                "The production profile response does not match the published schema. Update its PROFILE_SCHEMA and regenerate "
                    + "the schema with ./gradlew :x-pack:plugin:esql:profile-schema:updateProfileSchema",
                List.of(),
                errors
            );
        }
    }

    private void createTestIndex() {
        assertAcked(client().admin().indices().prepareCreate("test").setMapping("host", "type=keyword", "v", "type=long"));
        for (int i = 0; i < 20; i++) {
            prepareIndex("test").setSource(Map.of("host", "host-" + (i % 4), "v", i)).get();
        }
        client().admin().indices().prepareRefresh("test").get();
    }

    private static Map<String, Object> schema() throws IOException {
        try (InputStream in = EsqlProfileSchemaIT.class.getResourceAsStream(SCHEMA_RESOURCE)) {
            assertNotNull("missing [" + SCHEMA_RESOURCE + "] on the test classpath", in);
            try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, in)) {
                return parser.map();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> object(Object value) {
        assertNotNull("profile was not serialized", value);
        assertTrue("profile must be an object", value instanceof Map<?, ?>);
        return (Map<String, Object>) value;
    }
}
