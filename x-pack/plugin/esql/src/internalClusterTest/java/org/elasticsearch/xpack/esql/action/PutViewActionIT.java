/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.xpack.esql.view.PutViewAction;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

public class PutViewActionIT extends AbstractEsqlIntegTestCase {

    public void testIndicesAreNotValidateUponCreation() {
        assertAcked(createView("my_view", "FROM not-validated"));
    }

    public void testInvalidSyntaxQueryIsRejected() {
        expectThrows(ElasticsearchException.class, containsString("mismatched input"), () -> createView("my_view", "NOT VALID ESQL $$$$"));
    }

    public void testSetIsRejected() {
        expectThrows(
            ElasticsearchException.class,
            containsString("SET statements are not allowed in views"),
            () -> createView("my_view", "SET time_zone=\"Europe/Berlin\"; FROM index")
        );
    }

    private AcknowledgedResponse createView(String viewName, String query) {
        return client().execute(
            PutViewAction.INSTANCE,
            new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(viewName, query))
        ).actionGet(30, TimeUnit.SECONDS);
    }
}
