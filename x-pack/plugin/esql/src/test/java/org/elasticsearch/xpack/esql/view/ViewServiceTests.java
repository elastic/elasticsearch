/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ViewServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private ViewService viewService;

    @Before
    public void setUpService() {
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(
            threadPool,
            ProjectId.DEFAULT,
            Settings.EMPTY,
            Set.of(ViewService.MAX_VIEWS_COUNT_SETTING, ViewService.MAX_VIEW_LENGTH_SETTING)
        );
        // The default test publisher never acknowledges, so acked tasks would not complete their listeners.
        var publisher = ClusterServiceUtils.createClusterStatePublisher(clusterService.getClusterApplierService());
        clusterService.getMasterService().setClusterStatePublisher((event, publishListener, ackListener) -> {
            // Acknowledge only after the state is applied, so the listener never completes before the change is visible.
            publisher.publish(event, ActionListener.runAfter(publishListener, () -> {
                ackListener.onCommit(TimeValue.ZERO);
                event.getNewState().nodes().forEach(node -> ackListener.onNodeAck(node, null));
            }), ackListener);
        });
        viewService = new ViewService(clusterService, TEST_PARSER);
    }

    @After
    public void tearDownService() {
        clusterService.close();
        terminate(threadPool);
    }

    public void testCreatesMissingView() {
        var future = ensureReservedViewExists("reserved-view", "FROM some-index", "description");
        publish(UnaryOperator.identity());

        assertThat(future.actionGet(10, TimeUnit.SECONDS).isAcknowledged(), is(true));
        assertThat(
            viewService.get(ProjectId.DEFAULT, "reserved-view"),
            equalTo(new View("reserved-view", "FROM some-index", "description", true))
        );
    }

    public void testDoesNotChangeUpToDateView() {
        putView(new View("reserved-view", "FROM some-index", "description", true));
        var before = viewService.get(ProjectId.DEFAULT, "reserved-view");

        var future = ensureReservedViewExists("reserved-view", "FROM some-index", "description");
        publish(UnaryOperator.identity());

        assertThat(future.actionGet(10, TimeUnit.SECONDS).isAcknowledged(), is(true));
        assertThat(viewService.get(ProjectId.DEFAULT, "reserved-view"), sameInstance(before));
    }

    public void testUpdatesQuery() {
        putView(new View("reserved-view", "FROM some-index", "description", true));

        var future = ensureReservedViewExists("reserved-view", "FROM other-index", "description");
        publish(UnaryOperator.identity());

        assertThat(future.actionGet(10, TimeUnit.SECONDS).isAcknowledged(), is(true));
        assertThat(
            viewService.get(ProjectId.DEFAULT, "reserved-view"),
            equalTo(new View("reserved-view", "FROM other-index", "description", true))
        );
    }

    public void testUpdatesDescription() {
        putView(new View("reserved-view", "FROM some-index", "description", true));

        var future = ensureReservedViewExists("reserved-view", "FROM some-index", "other description");
        publish(UnaryOperator.identity());

        assertThat(future.actionGet(10, TimeUnit.SECONDS).isAcknowledged(), is(true));
        assertThat(
            viewService.get(ProjectId.DEFAULT, "reserved-view"),
            equalTo(new View("reserved-view", "FROM some-index", "other description", true))
        );
    }

    public void testFailsWhenViewWithSameNameExists() {
        putView(new View("reserved-view", "FROM user-index", null, false));
        var before = viewService.get(ProjectId.DEFAULT, "reserved-view");

        var future = ensureReservedViewExists("reserved-view", "FROM some-index", null);
        publish(UnaryOperator.identity());

        expectThrows(
            ResourceAlreadyExistsException.class,
            containsString("view [reserved-view] already exists"),
            () -> future.actionGet(10, TimeUnit.SECONDS)
        );
        assertThat(viewService.get(ProjectId.DEFAULT, "reserved-view"), sameInstance(before));
    }

    public void testFailsWhenIndexWithSameNameExists() {
        publish(
            state -> state.putProjectMetadata(
                ProjectMetadata.builder(clusterService.state().metadata().getProject(ProjectId.DEFAULT))
                    .put(IndexMetadata.builder("reserved-view").settings(indexSettings(IndexVersion.current(), 1, 0)))
            )
        );

        var future = ensureReservedViewExists("reserved-view", "FROM some-index", null);
        publish(UnaryOperator.identity());

        expectThrows(
            ResourceAlreadyExistsException.class,
            containsString("view [reserved-view] cannot be created"),
            () -> future.actionGet(10, TimeUnit.SECONDS)
        );
        assertThat(viewService.get(ProjectId.DEFAULT, "reserved-view"), nullValue());
    }

    public void testFailsWhenMaxViewsCountIsReached() {
        clusterService.getClusterSettings().applySettings(Settings.builder().put(ViewService.MAX_VIEWS_COUNT_SETTING.getKey(), 0).build());

        var future = ensureReservedViewExists("reserved-view", "FROM some-index", null);
        publish(UnaryOperator.identity());

        expectThrows(
            IllegalArgumentException.class,
            containsString("the maximum number of views is reached"),
            () -> future.actionGet(10, TimeUnit.SECONDS)
        );
        assertThat(viewService.get(ProjectId.DEFAULT, "reserved-view"), nullValue());
    }

    public void testWaitsForAllNodesToSupportReservedViews() {
        publish(
            state -> state.putCompatibilityVersions(
                "node",
                TransportVersionUtils.randomVersionNotSupporting(View.VIEW_RESERVED_VERSION),
                Map.of()
            )
        );

        var future = ensureReservedViewExists("reserved-view", "FROM some-index", null);

        publish(UnaryOperator.identity());
        assertThat(future.isDone(), is(false));
        assertThat(viewService.get(ProjectId.DEFAULT, "reserved-view"), nullValue());

        publish(state -> state.putCompatibilityVersions("node", TransportVersion.current(), Map.of()));
        assertThat(future.actionGet(10, TimeUnit.SECONDS).isAcknowledged(), is(true));
        assertThat(viewService.get(ProjectId.DEFAULT, "reserved-view"), notNullValue());
    }

    private PlainActionFuture<AcknowledgedResponse> ensureReservedViewExists(String name, String query, String description) {
        var future = new PlainActionFuture<AcknowledgedResponse>();
        viewService.ensureReservedViewExists(ProjectId.DEFAULT, name, query, description, future);
        return future;
    }

    private void putView(View view) {
        var future = new PlainActionFuture<AcknowledgedResponse>();
        viewService.putView(ProjectId.DEFAULT, new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, view), future);
        future.actionGet(10, TimeUnit.SECONDS);
    }

    /**
     * Applies the change to the current cluster state and publishes it, which notifies cluster state listeners.
     */
    private void publish(UnaryOperator<ClusterState.Builder> change) {
        ClusterServiceUtils.setState(clusterService, change.apply(ClusterState.builder(clusterService.state())));
    }
}
