/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.requestObjectBuilder;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.runEsqlSync;
import static org.hamcrest.Matchers.equalTo;

/**
 * Checks the cap on how many fields {@code SET unmapped_fields="LOAD_ALL"} expands {@code _source} into: past 1000 distinct
 * fields, only the alphabetically first 1000 become columns, with a warning, while a {@code KEEP} pattern still reaches the others.
 * Subclasses run this against the different cluster setups - single node, multiple nodes, mixed versions, and a remote cluster.
 */
public abstract class UnmappedFieldsLoadAllMaxFieldsTestCase extends ESRestTestCase {
    private static final int MAX_EXPANDED_FIELDS = 1000;
    private static final String INDEX = "load_all_max_fields";
    // The Warning header backslash-escapes the quotes in the message.
    private static final String WARNING = "unmapped_fields=\\\"LOAD_ALL\\\" found more than [1000] fields in _source; only the first "
        + "[1000] in alphabetical order are returned. Use KEEP or DROP to select the others.";

    @Before
    public void requireCapability() throws IOException {
        for (RestClient client : clientsRequiringCapability()) {
            assumeTrue(
                "requires the LOAD_ALL field cap",
                RestEsqlTestCase.hasCapabilities(client, List.of(EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_MAX_FIELDS.capabilityName()))
            );
        }
    }

    @After
    public void deleteIndex() throws IOException {
        for (RestClient client : clientsHoldingData()) {
            try {
                client.performRequest(new Request("DELETE", "/" + INDEX));
            } catch (ResponseException e) {
                assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
            }
        }
    }

    /** The clients of the clusters the test index lives in. */
    protected List<RestClient> clientsHoldingData() {
        return List.of(client());
    }

    /** The clients of every cluster the query touches, all of which must support the cap for the test to run. */
    protected List<RestClient> clientsRequiringCapability() {
        return clientsHoldingData();
    }

    /** The index expression {@code FROM} uses to reach the test index. */
    protected String indexPattern(String index) {
        return index;
    }

    /**
     * Two documents with one mapped {@code id} and 1001 distinct unmapped fields between them: {@code f0000..f0599} and
     * {@code f0500..f1000}. The overlap shows a field counts once however many rows carry it; {@code f1000} is the one past the cap.
     */
    private void indexDocs() throws IOException {
        for (RestClient client : clientsHoldingData()) {
            Request create = new Request("PUT", "/" + INDEX);
            create.setJsonEntity("""
                {
                  "mappings": {
                    "dynamic": false,
                    "properties": {
                      "id": { "type": "keyword" }
                    }
                  }
                }""");
            client.performRequest(create);

            Request bulk = new Request("POST", "/" + INDEX + "/_bulk");
            bulk.addParameter("refresh", "true");
            bulk.setJsonEntity(
                "{\"index\":{}}\n" + doc("0", 0, 600) + "\n{\"index\":{}}\n" + doc("1", 500, MAX_EXPANDED_FIELDS + 1) + "\n"
            );
            Map<String, Object> response = entityAsMap(client.performRequest(bulk));
            assertThat(response.get("errors"), equalTo(false));
        }
    }

    public void testOnlyFirstFieldsAreReturned() throws IOException {
        indexDocs();
        String query = String.format(Locale.ROOT, """
            SET unmapped_fields="LOAD_ALL";
            FROM %s
            | KEEP id, *
            | SORT id""", indexPattern(INDEX));
        Map<String, Object> result = runEsqlSync(
            requestObjectBuilder().query(query),
            new AssertWarnings.ExactStrings(List.of(WARNING)),
            null
        );

        List<String> expected = new ArrayList<>();
        expected.add("id");
        for (int i = 0; i < MAX_EXPANDED_FIELDS; i++) {
            expected.add(fieldName(i));
        }
        assertThat(columnNames(result), equalTo(expected));
        List<?> values = (List<?>) result.get("values");
        assertThat(values.size(), equalTo(2));
        // f0000 only in the first document, f0999 only in the second.
        assertThat(((List<?>) values.get(0)).get(1), equalTo("0"));
        assertThat(((List<?>) values.get(0)).get(MAX_EXPANDED_FIELDS), equalTo(null));
        assertThat(((List<?>) values.get(1)).get(1), equalTo(null));
        assertThat(((List<?>) values.get(1)).get(MAX_EXPANDED_FIELDS), equalTo("999"));
    }

    /** The {@code KEEP} pattern applies before the cap, so it can select a field the cap would otherwise cut off. */
    public void testKeepReachesFieldsPastTheCap() throws IOException {
        indexDocs();
        String query = String.format(Locale.ROOT, """
            SET unmapped_fields="LOAD_ALL";
            FROM %s
            | KEEP id, f1*
            | SORT id""", indexPattern(INDEX));
        Map<String, Object> result = runEsqlSync(requestObjectBuilder().query(query), new AssertWarnings.NoWarnings(), null);

        assertThat(columnNames(result), equalTo(List.of("id", fieldName(MAX_EXPANDED_FIELDS))));
        // The first document has no f1000.
        assertThat(result.get("values"), equalTo(List.of(Arrays.asList("0", null), List.of("1", String.valueOf(MAX_EXPANDED_FIELDS)))));
    }

    /** One document's JSON: the mapped {@code id}, then fields {@code from} (inclusive) to {@code to} (exclusive), valued by number. */
    private static String doc(String id, int from, int to) {
        StringBuilder doc = new StringBuilder("{\"id\":\"").append(id).append('"');
        for (int i = from; i < to; i++) {
            doc.append(",\"").append(fieldName(i)).append("\":").append(i);
        }
        return doc.append('}').toString();
    }

    private static String fieldName(int i) {
        return String.format(Locale.ROOT, "f%04d", i);
    }

    private static List<String> columnNames(Map<String, Object> result) {
        List<String> names = new ArrayList<>();
        for (Object column : (List<?>) result.get("columns")) {
            names.add((String) ((Map<?, ?>) column).get("name"));
        }
        return names;
    }
}
