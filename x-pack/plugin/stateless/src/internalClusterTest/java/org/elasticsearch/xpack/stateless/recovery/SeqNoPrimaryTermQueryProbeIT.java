/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Checks that the diagnostic query proposed during the 2026-09 incident is actually valid: a range over {@code _seq_no},
 * sorted on {@code _seq_no}, returning the sequence number and primary term of each hit without any document source.
 */
public class SeqNoPrimaryTermQueryProbeIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // we want a real HTTP endpoint to POST the literal query body to
    }

    public void testTheProposedDiagnosticQueryIsValid() throws Exception {
        startMasterOnlyNode();
        startIndexNode();
        startSearchNode();

        final String indexName = "probe";
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        final AtomicInteger ids = new AtomicInteger();
        indexDocs(indexName, 20, () -> "doc-" + ids.getAndIncrement());
        refresh(indexName);

        // the literal body from the Slack message, only the index name and the floor differ
        final Request request = new Request("GET", "/" + indexName + "/_search");
        request.setJsonEntity("""
            { "query": { "range": { "_seq_no": { "gt": 5 } } },
              "sort": [ { "_seq_no": "asc" } ],
              "_source": false, "seq_no_primary_term": true, "size": 50 }
            """);

        final Response response = getRestClient().performRequest(request);
        final String body = EntityUtils.toString(response.getEntity());

        logger.info("================ QUERY RESPONSE ================");
        logger.info(body);
        logger.info("===============================================");

        assertEquals(200, response.getStatusLine().getStatusCode());
        // the three things the message depends on
        assertTrue("response must carry _seq_no per hit", body.contains("\"_seq_no\""));
        assertTrue("response must carry _primary_term per hit", body.contains("\"_primary_term\""));
        assertFalse("no document source should be returned", body.contains("\"_source\""));
    }

    /**
     * The same question asked entirely through the URL, for a proxy that will only pass a GET without a body through.
     */
    public void testTheQueryAlsoWorksWithNoRequestBody() throws Exception {
        startMasterOnlyNode();
        startIndexNode();
        startSearchNode();

        final String indexName = "probe";
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        final AtomicInteger ids = new AtomicInteger();
        indexDocs(indexName, 20, () -> "doc-" + ids.getAndIncrement());
        refresh(indexName);

        final Request request = new Request("GET", "/" + indexName + "/_search");
        request.addParameter("q", "_seq_no:>5");
        request.addParameter("sort", "_seq_no:asc");
        request.addParameter("size", "50");
        request.addParameter("seq_no_primary_term", "true");
        request.addParameter("_source", "false");

        final Response response = getRestClient().performRequest(request);
        final String body = EntityUtils.toString(response.getEntity());

        logger.info("================ URL-ONLY QUERY RESPONSE ================");
        logger.info(body);
        logger.info("=========================================================");

        assertEquals(200, response.getStatusLine().getStatusCode());
        assertTrue("response must carry _seq_no per hit", body.contains("\"_seq_no\""));
        assertTrue("response must carry _primary_term per hit", body.contains("\"_primary_term\""));
        assertFalse("no document source should be returned", body.contains("\"_source\""));
    }
}
