/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.view.PutViewAction;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ViewIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), DataStreamsPlugin.class);
    }

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

    public void testCannotCreateAliasToView() {
        assertAcked(createView("my-view", "FROM not-validated"));
        assertAcked(indicesAdmin().prepareCreate("source-index"));

        expectThrows(
            IndexNotFoundException.class,
            containsString("no such index [my-view]"),
            () -> indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(IndicesAliasesRequest.AliasActions.add().index("my-view").alias("some-alias"))
                .get()
        );
    }

    public void testViewOverAlias() {
        assertAcked(indicesAdmin().prepareCreate("source-index").setMapping("f1", "type=integer"));
        prepareIndex("source-index").setSource("f1", 42).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(IndicesAliasesRequest.AliasActions.add().index("source-index").alias("source-alias"))
                .get()
        );

        assertAcked(createView("alias-view", "FROM source-alias | KEEP f1"));

        try (EsqlQueryResponse response = run("FROM alias-view")) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(42))));
        }
    }

    public void testViewOverDataStream() throws IOException {
        assertAcked(
            client().execute(
                TransportPutComposableIndexTemplateAction.TYPE,
                new TransportPutComposableIndexTemplateAction.Request("ds-view-template").indexTemplate(
                    ComposableIndexTemplate.builder()
                        .indexPatterns(List.of("logs-view-test*"))
                        .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                        .template(Template.builder().mappings(new CompressedXContent("""
                            {
                              "properties": {
                                "@timestamp": { "type": "date" },
                                "f1": { "type": "integer" }
                              }
                            }""")))
                        .build()
                )
            )
        );

        String time = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
        prepareIndex("logs-view-test").setOpType(DocWriteRequest.OpType.CREATE)
            .setSource("@timestamp", time, "f1", 42)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        assertAcked(createView("ds-view", "FROM logs-view-test | KEEP f1"));

        try (EsqlQueryResponse response = run("FROM ds-view")) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(42))));
        }
    }

    private AcknowledgedResponse createView(String viewName, String query) {
        return client().execute(
            PutViewAction.INSTANCE,
            new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(viewName, query))
        ).actionGet(30, TimeUnit.SECONDS);
    }
}
