/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
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
        LogicalPlan rewritten = viewService.replaceViews(plan, telemetry, EsqlTestUtils.TEST_CFG);
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
        Exception e = expectThrows(
            VerificationException.class,
            () -> viewService.replaceViews(statement("from view11"), telemetry, EsqlTestUtils.TEST_CFG)
        );
        assertThat(e.getMessage(), startsWith("The maximum allowed view depth of 10 has been exceeded"));

        // But FROM view10 should work
        LogicalPlan rewritten = viewService.replaceViews(statement("from view10"), telemetry, EsqlTestUtils.TEST_CFG);
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
                () -> customViewService.replaceViews(statement("from view2"), telemetry, EsqlTestUtils.TEST_CFG)
            );
            assertThat(e.getMessage(), startsWith("The maximum allowed view depth of 1 has been exceeded"));

            // But FROM view1 should work
            LogicalPlan rewritten = customViewService.replaceViews(statement("from view1"), telemetry, EsqlTestUtils.TEST_CFG);
            assertThat(rewritten, equalTo(statement("from emp")));
        } catch (Exception e) {
            throw new AssertionError("unexpected exception", e);
        }
    }

    private void addView(String name, String query) throws Exception {
        addView(name, query, viewService);
    }

    private void addView(String name, String query, ViewService viewService) throws Exception {
        viewService.put(name, new View(query), ActionListener.noop(), EsqlTestUtils.TEST_CFG);
    }

}
