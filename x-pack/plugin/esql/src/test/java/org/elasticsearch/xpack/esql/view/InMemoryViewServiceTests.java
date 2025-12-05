/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.parser.AbstractStatementParserTests;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class InMemoryViewServiceTests extends AbstractStatementParserTests {
    static InMemoryViewService viewService;

    @BeforeClass
    public static void setup() {
        viewService = new InMemoryViewService();
    }

    @AfterClass
    public static void afterTearDown() {
        viewService.close();
    }

    PlanTelemetry telemetry = new PlanTelemetry(new EsqlFunctionRegistry());
    ProjectId projectId = ProjectId.fromId("1");

    public void testPutGet() {
        addView("view1", "from emp");
        addView("view2", "from view1");
        addView("view3", "from view2");
        assertThat(viewService.get(projectId, "view1").query(), equalTo("from emp"));
        assertThat(viewService.get(projectId, "view2").query(), equalTo("from view1"));
        assertThat(viewService.get(projectId, "view3").query(), equalTo("from view2"));
    }

    public void testReplaceView() {
        addView("view1", "from emp");
        addView("view2", "from view1");
        addView("view3", "from view2");
        LogicalPlan plan = statement("from view3");
        LogicalPlan rewritten = viewService.replaceViews(plan, telemetry);
        assertThat(rewritten, equalTo(statement("from emp")));
    }

    public void testViewDepthExceeded() {
        addView("view1", "from emp");
        addView("view2", "from view1");
        addView("view3", "from view2");
        addView("view4", "from view3");
        addView("view5", "from view4");
        addView("view6", "from view5");
        addView("view7", "from view6");
        addView("view8", "from view7");
        addView("view9", "from view8");
        addView("view10", "from view9");
        addView("view11", "from view10");

        // FROM view11 should fail
        Exception e = expectThrows(VerificationException.class, () -> viewService.replaceViews(statement("from view11"), telemetry));
        assertThat(e.getMessage(), startsWith("The maximum allowed view depth of 10 has been exceeded"));

        // But FROM view10 should work
        LogicalPlan rewritten = viewService.replaceViews(statement("from view10"), telemetry);
        assertThat(rewritten, equalTo(statement("from emp")));
    }

    public void testModifiedViewDepth() {
        try (
            InMemoryViewService customViewService = viewService.withSettings(
                Settings.builder().put(ViewService.MAX_VIEW_DEPTH_SETTING.getKey(), 1).build()
            )
        ) {
            addView("view1", "from emp", customViewService);
            addView("view2", "from view1", customViewService);
            addView("view3", "from view2", customViewService);

            // FROM view2 should fail
            Exception e = expectThrows(
                VerificationException.class,
                () -> customViewService.replaceViews(statement("from view2"), telemetry)
            );
            assertThat(e.getMessage(), startsWith("The maximum allowed view depth of 1 has been exceeded"));

            // But FROM view1 should work
            LogicalPlan rewritten = customViewService.replaceViews(statement("from view1"), telemetry);
            assertThat(rewritten, equalTo(statement("from emp")));
        } catch (Exception e) {
            throw new AssertionError("unexpected exception", e);
        }
    }

    public void testViewCountExceeded() {
        for (int i = 0; i < ViewService.MAX_VIEWS_COUNT_SETTING.getDefault(Settings.EMPTY); i++) {
            addView("view" + i, "from emp");
        }

        // FROM view11 should fail
        Exception e = expectThrows(Exception.class, () -> addView("viewx", "from emp"));
        assertThat(e.getMessage(), containsString("cannot add view, the maximum number of views is reached: 100"));
    }

    public void testModifiedViewCount() {
        try (
            InMemoryViewService customViewService = new InMemoryViewService(
                Settings.builder().put(ViewService.MAX_VIEWS_COUNT_SETTING.getKey(), 1).build()
            )
        ) {
            addView("view1", "from emp", customViewService);

            // View2 should fail
            Exception e = expectThrows(Exception.class, () -> addView("view2", "from emp", customViewService));
            assertThat(e.getMessage(), containsString("cannot add view, the maximum number of views is reached: 1"));
        } catch (Exception e) {
            throw new AssertionError("unexpected exception", e);
        }
    }

    public void testViewLengthExceeded() {
        addView("view1", "from short");

        // Long view definition should fail
        Exception e = expectThrows(
            Exception.class,
            () -> addView("viewx", "from " + "a".repeat(Math.max(0, ViewService.MAX_VIEW_LENGTH_SETTING.getDefault(Settings.EMPTY))))
        );
        assertThat(e.getMessage(), containsString("view query is too large: 10005 characters, the maximum allowed is 10000"));
    }

    public void testModifiedViewLength() {
        try (
            InMemoryViewService customViewService = viewService.withSettings(
                Settings.builder().put(ViewService.MAX_VIEW_LENGTH_SETTING.getKey(), 6).build()
            )
        ) {
            addView("view1", "from a", customViewService);

            // Just one character longer should fail
            Exception e = expectThrows(Exception.class, () -> addView("view2", "from aa", customViewService));
            assertThat(e.getMessage(), containsString("view query is too large: 7 characters, the maximum allowed is 6"));
        } catch (Exception e) {
            throw new AssertionError("unexpected exception", e);
        }
    }

    public void testInvalidViewNames() {
        try (InMemoryViewService customViewService = new InMemoryViewService()) {
            for (var name : Map.of(
                "viewX",
                "invalid view name [viewX], must be lowercase",
                ".",
                "invalid view name [.], must not be '.' or '..'",
                "..",
                "invalid view name [..], must not be '.' or '..'",
                "invalid name",
                "invalid view name [invalid name], must not contain the following characters",
                "invalid*name",
                "invalid view name [invalid*name], must not contain the following characters"
            ).entrySet()) {
                expectThrows(
                    "Expected '" + name.getKey() + "' to be an invalid name, but it was not",
                    Exception.class,
                    containsString(name.getValue()),
                    () -> addView(name.getKey(), "from aa", customViewService)
                );
            }
        }
    }

    private void addView(String name, String query) {
        addView(name, query, viewService);
    }

    private void addView(String name, String query, ViewService viewService) {
        PutViewAction.Request request = new PutViewAction.Request(TimeValue.ONE_MINUTE, TimeValue.ONE_MINUTE, new View(name, query));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> err = new AtomicReference<>(null);
        viewService.putView(projectId, request, ActionListener.wrap(r -> latch.countDown(), e -> {
            err.set(e);
            latch.countDown();
        }));
        try {
            // In-memory puts are synchronous, so we should never wait here
            assert latch.await(1, TimeUnit.MILLISECONDS) : "should never timeout";
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (err.get() != null) {
            throw new RuntimeException(err.get());
        }
    }

}
