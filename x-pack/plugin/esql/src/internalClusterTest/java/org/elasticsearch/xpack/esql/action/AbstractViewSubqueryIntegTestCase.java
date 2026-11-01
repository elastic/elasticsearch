/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public abstract class AbstractViewSubqueryIntegTestCase extends AbstractEsqlIntegTestCase {

    private final List<String> createdViews = new ArrayList<>();

    @Before
    public void createLanguagesIndex() {
        if (indexExists("languages")) {
            return;
        }
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("languages")
                .setMapping("language_code", "type=integer", "language_name", "type=keyword")
                .get()
        );
        BulkRequestBuilder bulk = client().prepareBulk();
        bulk.add(prepareIndex("languages").setSource("language_code", 1, "language_name", "English"));
        bulk.add(prepareIndex("languages").setSource("language_code", 2, "language_name", "French"));
        bulk.add(prepareIndex("languages").setSource("language_code", 3, "language_name", "Spanish"));
        bulk.add(prepareIndex("languages").setSource("language_code", 4, "language_name", "German"));
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        ensureGreen("languages");
    }

    protected void createView(String name, String query) {
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(name, query))
            )
        );
        createdViews.add(name);
    }

    @After
    public void deleteViews() {
        for (var name : createdViews) {
            try {
                client().execute(
                    DeleteViewAction.INSTANCE,
                    new DeleteViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new String[] { name })
                ).actionGet();
            } catch (ResourceNotFoundException ignored) {} catch (Exception e) {
                logger.warn("view cleanup [{}] failed", name, e);
            }
        }
        createdViews.clear();
    }

    protected List<List<Object>> countsByClassAndName(String from) {
        try (var response = run(from + " | STATS n = COUNT(*) BY _class, _name | SORT _class, _name")) {
            return getValuesList(response);
        }
    }

    protected static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }
}
