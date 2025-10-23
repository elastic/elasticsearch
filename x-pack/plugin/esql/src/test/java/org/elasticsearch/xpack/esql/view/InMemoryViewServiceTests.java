/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.parser.AbstractStatementParserTests;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class InMemoryViewServiceTests extends AbstractStatementParserTests {
    EsqlFunctionRegistry functionRegistry = new EsqlFunctionRegistry();
    InMemoryViewService viewService = new InMemoryViewService(functionRegistry);
    PlanTelemetry telemetry = new PlanTelemetry(functionRegistry);

    public void testPutGet() throws Exception {
        addView("view1", "from emp");
        addView("view2", "from view1");
        addView("view3", "from view2");
        assertThat(viewService.get("view1").query(), equalTo("from emp"));
        assertThat(viewService.get("view2").query(), equalTo("from view1"));
        assertThat(viewService.get("view3").query(), equalTo("from view2"));
    }

    public void testReplaceView() throws Exception {
        addView("view1", "from emp");
        addView("view2", "from view1");
        addView("view3", "from view2");
        LogicalPlan plan = statement("from view3");
        LogicalPlan rewritten = viewService.replaceViews(plan, telemetry);
        assertThat(rewritten, equalTo(statement("from emp")));
    }

    public void testViewDepthExceeded() throws Exception {
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
        var config = new ViewService.ViewServiceConfig(100, 10_000, 1);
        InMemoryViewService customViewService = new InMemoryViewService(functionRegistry, config);
        try {
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

    public void testViewCountExceeded() throws Exception {
        for (int i = 0; i < ViewService.ViewServiceConfig.DEFAULT.maxViews(); i++) {
            addView("view" + i, "from emp");
        }

        // FROM view11 should fail
        Exception e = expectThrows(IllegalArgumentException.class, () -> addView("viewX", "from emp"));
        assertThat(e.getMessage(), startsWith("cannot add view, the maximum number of views is reached: 100"));
    }

    public void testModifiedViewCount() {
        var config = new ViewService.ViewServiceConfig(1, 10_000, 10);
        InMemoryViewService customViewService = new InMemoryViewService(functionRegistry, config);
        try {
            addView("view1", "from emp", customViewService);

            // View2 should fail
            Exception e = expectThrows(IllegalArgumentException.class, () -> addView("view2", "from emp", customViewService));
            assertThat(e.getMessage(), startsWith("cannot add view, the maximum number of views is reached: 1"));
        } catch (Exception e) {
            throw new AssertionError("unexpected exception", e);
        }
    }

    public void testViewLengthExceeded() throws Exception {
        addView("view1", "from short");

        // Long view definition should fail
        StringBuilder longView = new StringBuilder("from ");
        for (int i = 0; i < ViewService.ViewServiceConfig.DEFAULT.maxViewSize(); i++) {
            longView.append("a");
        }
        Exception e = expectThrows(IllegalArgumentException.class, () -> addView("viewX", longView.toString()));
        assertThat(e.getMessage(), startsWith("view query is too large: 10005 characters, the maximum allowed is 10000"));
    }

    public void testModifiedViewLength() {
        var config = new ViewService.ViewServiceConfig(100, 6, 10);
        InMemoryViewService customViewService = new InMemoryViewService(functionRegistry, config);
        try {
            addView("view1", "from a", customViewService);

            // Just one character longer should fail
            Exception e = expectThrows(IllegalArgumentException.class, () -> addView("view2", "from aa", customViewService));
            assertThat(e.getMessage(), startsWith("view query is too large: 7 characters, the maximum allowed is 6"));
        } catch (Exception e) {
            throw new AssertionError("unexpected exception", e);
        }
    }

    private void addView(String name, String query) {
        addView(name, query, viewService);
    }

    private void addView(String name, String query, ViewService viewService) {
        viewService.put(name, new View(query), ActionListener.noop());
    }

}
