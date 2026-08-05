/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.Build;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class ViewRestTests extends AbstractViewTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        // Add data-streams so the test can create the co-resident data stream that triggers the resolver throw.
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(DataStreamsPlugin.class);
        return plugins;
    }

    public void testRestWorksInBothSnapshotAndRelease() {
        GetViewAction.Request request = new GetViewAction.Request(TimeValue.timeValueMinutes(1));
        TestResponseCapture<GetViewAction.Response> responseCapture = new TestResponseCapture<>();
        client().admin().cluster().execute(GetViewAction.INSTANCE, request, responseCapture);
        if (responseCapture.error.get() != null) {
            String build = Build.current().isSnapshot() ? "SNAPSHOT" : "RELEASE";
            fail(responseCapture.error.get(), "Failed to get views in " + build + " build");
        }
        if (responseCapture.response == null) {
            fail("Response is null");
        }
    }

    public void testGetViewByCoresidentDataStreamNameIsEmptyNotError() {
        // Unlike datasets, the view resolver is already lenient for a co-resident data stream: GET by that explicit
        // name resolves to no views and returns an empty response, never an error. (Documents that views do NOT share
        // the dataset data-stream leak.)
        assertAcked(
            client().execute(
                TransportPutComposableIndexTemplateAction.TYPE,
                new TransportPutComposableIndexTemplateAction.Request("entities-updates-template").indexTemplate(
                    ComposableIndexTemplate.builder()
                        .indexPatterns(List.of("entities-updates-*"))
                        .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                        .build()
                )
            )
        );
        assertAcked(
            client().execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "entities-updates-default")
            )
        );

        GetViewAction.Request request = new GetViewAction.Request(TimeValue.timeValueMinutes(1));
        request.indices("entities-updates-default");
        TestResponseCapture<GetViewAction.Response> capture = new TestResponseCapture<>();
        client().admin().cluster().execute(GetViewAction.INSTANCE, request, capture);

        assertNull("view GET on a co-resident data stream must not error", capture.error.get());
        assertNotNull(capture.response);
    }

    public void testGetViewByMissingNameReturnsCleanNotFound() {
        // GET a view by a name that does not exist. The view resolver throws IndexNotFoundException; the transport must
        // translate it to a clean view-shaped not-found, never leak the raw index_not_found_exception. Mirrors the
        // dataset get/delete and view delete transports.
        final String missing = "no-such-view-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        GetViewAction.Request request = new GetViewAction.Request(TimeValue.timeValueMinutes(1));
        request.indices(missing);
        TestResponseCapture<GetViewAction.Response> capture = new TestResponseCapture<>();
        client().admin().cluster().execute(GetViewAction.INSTANCE, request, capture);

        Exception error = capture.error.get();
        assertThat(error, instanceOf(ResourceNotFoundException.class));
        assertThat(error.getMessage(), containsString("view [" + missing + "] not found"));
        // IndexNotFoundException is itself a ResourceNotFoundException subtype, so guard against the raw leak.
        assertThat("GET must not leak the raw index resolution error", error, not(instanceOf(IndexNotFoundException.class)));
    }
}
