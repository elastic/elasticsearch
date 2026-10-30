/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.junit.After;
import org.junit.Before;

import java.util.List;
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
            Set.of(ViewService.MAX_VIEWS_COUNT_SETTING, ViewService.MAX_VIEW_LENGTH_SETTING, EsqlFlags.ESQL_MAX_COMMAND_COUNT)
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
        viewService = new ViewService(clusterService, TEST_PARSER, (statement, viewName, listener) -> listener.onResponse(null));
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

    public void testWaitsForCreationConditionBeforeCreatingView() {
        var future = new PlainActionFuture<AcknowledgedResponse>();
        viewService.ensureReservedViewExists(
            ProjectId.DEFAULT,
            new View("reserved-view", "FROM some-index", null, true),
            project -> project.hasIndex("some-index"),
            future
        );

        publish(UnaryOperator.identity());
        assertThat(future.isDone(), is(false));
        assertThat(viewService.get(ProjectId.DEFAULT, "reserved-view"), nullValue());

        publish(
            state -> state.putProjectMetadata(
                ProjectMetadata.builder(clusterService.state().metadata().getProject(ProjectId.DEFAULT))
                    .put(IndexMetadata.builder("some-index").settings(indexSettings(IndexVersion.current(), 1, 0)))
            )
        );
        assertThat(future.actionGet(10, TimeUnit.SECONDS).isAcknowledged(), is(true));
        assertThat(viewService.get(ProjectId.DEFAULT, "reserved-view"), notNullValue());
    }

    public void testUpdatesExistingViewEvenIfCreationConditionIsFalse() {
        putView(new View("reserved-view", "FROM some-index", null, true));

        var future = new PlainActionFuture<AcknowledgedResponse>();
        viewService.ensureReservedViewExists(
            ProjectId.DEFAULT,
            new View("reserved-view", "FROM other-index", null, true),
            project -> false,
            future
        );
        publish(UnaryOperator.identity());

        assertThat(future.actionGet(10, TimeUnit.SECONDS).isAcknowledged(), is(true));
        assertThat(viewService.get(ProjectId.DEFAULT, "reserved-view").query(), equalTo("FROM other-index"));
    }

    public void testAcceptsTsWithStats() {
        validatePutView("ts-view", "TS metrics | STATS max(rate(requests)) BY host");
        validatePutView("ts-view", "TS metrics | WHERE cpu > 1 | STATS max(cpu) | KEEP `max(cpu)`");
    }

    public void testAcceptsViewWithoutTs() {
        validatePutView("from-view", "FROM metrics | KEEP cpu");
        validatePutView("from-view", "FROM metrics | STATS avg(cpu)");
    }

    public void testRejectsTsWithoutStats() {
        expectThrows(
            IllegalArgumentException.class,
            containsString("view [ts-view] uses TS, which must be followed by a STATS command"),
            () -> validatePutView("ts-view", "TS metrics | WHERE cpu > 1 | KEEP cpu")
        );
    }

    public void testAcceptsPromql() {
        validatePutView("promql-view", "PROMQL index=metrics step=1m cpu_avg=(avg(cpu))");
        validatePutView("promql-view", "PROMQL index=metrics step=1m cpu_avg=(avg(cpu)) | WHERE cpu_avg > 1 | KEEP cpu_avg");
        validatePutView("promql-view", "PROMQL index=metrics step=1m cpu_avg=(avg(cpu)) | STATS max(cpu_avg)");
    }

    public void testIgnoresTsInSubquery() {
        validatePutView("subquery-view", "FROM (TS metrics | KEEP cpu), (FROM other)");
    }

    private void validatePutView(String name, String query) {
        viewService.validatePutView(clusterService.state().metadata().getProject(ProjectId.DEFAULT), new View(name, query, null, false));
    }

    /**
     * A PUT request carries no pragmas, so the cluster setting is the only source of the command limit when a view is validated.
     */
    public void testPutViewValidatesAgainstConfiguredCommandLimit() {
        int defaultLimit = EsqlFlags.ESQL_MAX_COMMAND_COUNT.getDefault(Settings.EMPTY);
        // Each pipeline stays below the per-pipeline depth limit, only their sum exceeds the command limit.
        String pipes = " | WHERE a > 0".repeat(defaultLimit / 2 + 1);
        View view = new View("big-view", "FROM some-index, (FROM foo" + pipes + "), (FROM bar" + pipes + ")", null);
        var project = clusterService.state().metadata().getProject(ProjectId.DEFAULT);

        expectThrows(
            ParsingException.class,
            containsString(
                "ES|QL statement exceeded the maximum of [" + defaultLimit + "] commands allowed, including the commands in its subqueries"
            ),
            () -> viewService.validatePutView(project, view)
        );

        clusterService.getClusterSettings()
            .applySettings(Settings.builder().put(EsqlFlags.ESQL_MAX_COMMAND_COUNT.getKey(), defaultLimit + 100).build());
        viewService.validatePutView(project, view);
    }

    public void testValidateViewWarnsWhenQueryIsNotValid() {
        var service = viewServiceFailingAnalysisWith(new VerificationException("Found 1 problem\nline 1:6: Unknown index [missing]"));

        validateView(service, new View("v", "FROM missing", null, false));

        assertWarnings("view [v] query is not valid: Found 1 problem line 1:6: Unknown index [missing]");
        assertThat(service.get(ProjectId.DEFAULT, "v"), nullValue());
    }

    /**
     * These failures say nothing about the query, so they must not look like a problem with it.
     */
    public void testValidateViewDoesNotWarnWhenAnalysisFailsForOtherReasons() {
        for (Exception failure : List.<Exception>of(
            new EsRejectedExecutionException("rejected"),
            new TaskCancelledException("Cancelled"),
            new CircuitBreakingException("too big", CircuitBreaker.Durability.TRANSIENT),
            new ElasticsearchSecurityException("not allowed", RestStatus.FORBIDDEN),
            new ElasticsearchStatusException("something broke", RestStatus.INTERNAL_SERVER_ERROR)
        )) {
            validateView(viewServiceFailingAnalysisWith(failure), new View("v", "FROM missing", null, false));
            ensureNoWarnings();
        }
    }

    public void testValidateViewDoesNotWarnWhenQueryIsValid() {
        validateView(viewService, new View("v", "FROM some-index", null, false));
        ensureNoWarnings();
    }

    public void testPutViewDoesNotAnalyze() {
        var service = new ViewService(clusterService, TEST_PARSER, (statement, viewName, listener) -> fail("must not analyze"));

        putView(service, new View("v", "FROM missing", null, false));

        assertThat(service.get(ProjectId.DEFAULT, "v"), notNullValue());
    }

    private ViewService viewServiceFailingAnalysisWith(Exception failure) {
        return new ViewService(clusterService, TEST_PARSER, (statement, viewName, listener) -> listener.onFailure(failure));
    }

    private void validateView(ViewService service, View view) {
        var future = new PlainActionFuture<Void>();
        service.validateView(ProjectId.DEFAULT, view, future);
        future.actionGet(10, TimeUnit.SECONDS);
    }

    private PlainActionFuture<AcknowledgedResponse> ensureReservedViewExists(String name, String query, String description) {
        var future = new PlainActionFuture<AcknowledgedResponse>();
        viewService.ensureReservedViewExists(ProjectId.DEFAULT, new View(name, query, description, true), future);
        return future;
    }

    private void putView(View view) {
        putView(viewService, view);
    }

    private void putView(ViewService service, View view) {
        var future = new PlainActionFuture<AcknowledgedResponse>();
        service.putView(ProjectId.DEFAULT, new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, view), future);
        future.actionGet(10, TimeUnit.SECONDS);
    }

    /**
     * Applies the change to the current cluster state and publishes it, which notifies cluster state listeners.
     */
    private void publish(UnaryOperator<ClusterState.Builder> change) {
        ClusterServiceUtils.setState(clusterService, change.apply(ClusterState.builder(clusterService.state())));
    }
}
