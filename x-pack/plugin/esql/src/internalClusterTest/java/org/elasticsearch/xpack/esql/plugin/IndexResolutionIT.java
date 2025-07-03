/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class IndexResolutionIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), DataStreamsPlugin.class);
    }

    public void testResolvesConcreteIndex() {
        assertAcked(client().admin().indices().prepareCreate("index-1"));
        indexRandom(true, "index-1", 10);

        try (var response = run(syncEsqlQueryRequest().query("FROM index-1"))) {
            assertOk(response);
        }
    }

    public void testResolvesAlias() {
        assertAcked(client().admin().indices().prepareCreate("index-1"));
        indexRandom(true, "index-1", 10);
        assertAcked(client().admin().indices().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("index-1", "alias-1"));

        try (var response = run(syncEsqlQueryRequest().query("FROM alias-1"))) {
            assertOk(response);
        }
    }

    public void testResolvesDataStream() {
        assertAcked(
            client().execute(
                TransportPutComposableIndexTemplateAction.TYPE,
                new TransportPutComposableIndexTemplateAction.Request("data-stream-1-template").indexTemplate(
                    ComposableIndexTemplate.builder()
                        .indexPatterns(List.of("data-stream-1*"))
                        .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                        .build()
                )
            )
        );
        assertAcked(
            client().execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "data-stream-1")
            )
        );

        try (var response = run(syncEsqlQueryRequest().query("FROM data-stream-1"))) {
            assertOk(response);
        }
    }

    public void testResolvesPattern() {
        assertAcked(client().admin().indices().prepareCreate("index-1"));
        indexRandom(true, "index-1", 10);
        assertAcked(client().admin().indices().prepareCreate("index-2"));
        indexRandom(true, "index-2", 10);

        try (var response = run(syncEsqlQueryRequest().query("FROM index-*"))) {
            assertOk(response);
        }
    }

    public void testDoesNotResolveMissingIndex() {
        expectThrows(
            VerificationException.class,
            containsString("Unknown index [no-such-index]"),
            () -> run(syncEsqlQueryRequest().query("FROM no-such-index"))
        );
    }

    public void testDoesNotResolveEmptyPattern() {
        expectThrows(
            VerificationException.class,
            containsString("Unknown index [index-*]"),
            () -> run(syncEsqlQueryRequest().query("FROM index-*"))
        );
    }

    public void testDoesNotResolveClosedIndex() {
        assertAcked(client().admin().indices().prepareCreate("index-1"));
        indexRandom(true, "index-1", 10);
        assertAcked(client().admin().indices().prepareClose("index-1"));

        expectThrows(
            ClusterBlockException.class,
            containsString("index [index-1] blocked by: [FORBIDDEN/4/index closed]"),
            () -> run(syncEsqlQueryRequest().query("FROM index-1"))
        );
    }

    public void testPartialResolution() {
        assertAcked(client().admin().indices().prepareCreate("index-1"));
        assertAcked(client().admin().indices().prepareCreate("index-2"));
        indexRandom(true, "index-2", 10);

        try (var response = run(syncEsqlQueryRequest().query("FROM index-1,nonexisting"))) {
            assertOk(response);
        }
        expectThrows(
            IndexNotFoundException.class,
            equalTo("no such index [nonexisting]"),
            () -> run(syncEsqlQueryRequest().query("FROM index-2,nonexisting"))
        );
    }

    private static void assertOk(EsqlQueryResponse response) {
        assertThat(response.isPartial(), equalTo(false));
    }
}
