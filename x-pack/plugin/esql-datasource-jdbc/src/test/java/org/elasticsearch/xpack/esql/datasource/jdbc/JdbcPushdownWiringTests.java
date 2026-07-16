/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.optimizer.ExternalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushFiltersToSource;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.equalsOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Wiring guard for JDBC WHERE pushdown.
 * <p>
 * The optimizer rule {@link PushFiltersToSource} is the ONLY path that turns an ES|QL {@code FilterExec} sitting on a
 * JDBC {@link ExternalSourceExec} into a pushed-down predicate. Previously the rule could reach a connector's
 * {@link org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport} only through the file-based
 * {@code FormatReaderRegistry}; a JDBC source (which is not a file format) therefore silently resolved to a full scan.
 * The wiring threads the connector {@code sourceFactories} map (keyed by the compound scheme, e.g. {@code jdbc:postgresql})
 * into {@link ExternalOptimizerContext} so the rule can look the connector up by {@code sourceType} and consult its
 * {@link org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory#filterPushdownSupport()}.
 * <p>
 * These tests drive the REAL {@link PushFiltersToSource} rule against the REAL {@link JdbcConnectorFactory} (over an
 * H2 driver registry, no database connection is opened — {@code filterPushdownSupport()} is a pure lookup) and assert:
 * <ul>
 *   <li>a bare-column numeric filter is resolved and pushed (the {@link ExternalSourceExec#pushedFilter()} becomes a
 *       {@link JdbcPushedQuery}), so a regression back to "no connector pushdown path" fails loudly here;</li>
 *   <li>a keyword equality is pushed but RECHECK — the engine-side {@link FilterExec} is retained for byte-exact
 *       correctness while the DB still gets the predicate;</li>
 *   <li>with NO {@code sourceFactories} map (the earlier shape) the same filter is NOT pushed — pinning that the map is
 *       exactly what re-enables connector resolution;</li>
 *   <li>with pushdown disabled on the factory (the {@code esql.jdbc.pushdown.enabled} kill switch reporting no
 *       support) the filter is NOT pushed even though the map is present.</li>
 * </ul>
 */
public class JdbcPushdownWiringTests extends ESTestCase {

    private static final String JDBC_URL = "jdbc:postgresql://db.example.com:5432/warehouse";
    private static final String SOURCE_TYPE = "jdbc:postgresql";

    private static final Attribute ID = referenceAttribute("id", DataType.INTEGER);
    private static final Attribute NAME = referenceAttribute("name", DataType.KEYWORD);

    private JdbcDriverRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = JdbcDriverRegistry.fromClassLoader(getClass().getClassLoader());
    }

    @Override
    public void tearDown() throws Exception {
        registry.close();
        super.tearDown();
    }

    public void testBareColumnNumericFilterIsResolvedAndPushed() throws Exception {
        // id > 5 on an INTEGER column: YES pushdown (no RECHECK), so the FilterExec is pruned entirely and the pushed
        // filter lands on the ExternalSourceExec as a JdbcPushedQuery. This is the core proof: the rule reached the
        // JDBC connector's filterPushdownSupport through the sourceFactories map.
        try (JdbcConnectorFactory factory = new JdbcConnectorFactory(registry)) {
            Expression condition = greaterThanOf(ID, of(5));
            PhysicalPlan result = applyRule(condition, sourceFactories(factory));

            ExternalSourceExec external = findExternalSource(result);
            assertThat(
                "regression: PushFiltersToSource did not resolve the JDBC connector's filterPushdownSupport",
                external.pushedFilter(),
                instanceOf(JdbcPushedQuery.class)
            );
            // YES pushdown: the FilterExec is gone, the source stands alone.
            assertThat("a non-RECHECK bare-column filter must prune the FilterExec", result, instanceOf(ExternalSourceExec.class));
        }
    }

    public void testKeywordEqualityIsPushedButRechecked() throws Exception {
        // name == "acme" on a KEYWORD column: pushed for row-skipping, but kept in the engine-side FilterExec because
        // the vendor collation may not match ES|QL's byte-exact equality (RECHECK). Both halves must be present.
        try (JdbcConnectorFactory factory = new JdbcConnectorFactory(registry)) {
            Expression condition = equalsOf(NAME, of("acme"));
            PhysicalPlan result = applyRule(condition, sourceFactories(factory));

            assertThat("RECHECK keyword equality must retain the engine-side FilterExec", result, instanceOf(FilterExec.class));
            ExternalSourceExec external = findExternalSource(result);
            assertThat(
                "the keyword equality must still be pushed to the DB for row-skipping",
                external.pushedFilter(),
                instanceOf(JdbcPushedQuery.class)
            );
        }
    }

    public void testNotPushedWithoutSourceFactoriesMap() throws Exception {
        // Earlier shape: the ExternalOptimizerContext carries only a (null) FormatReaderRegistry and no connector map.
        // A JDBC source is not a file format, so resolution must find nothing and leave the filter in place — proving
        // the sourceFactories map is exactly what re-enables connector pushdown.
        Expression condition = greaterThanOf(ID, of(5));
        PhysicalPlan result = applyRule(condition, new ExternalOptimizerContext(null));

        assertThat("without the connector map the filter must not be pushed", result, instanceOf(FilterExec.class));
        assertNull("no pushdown means no pushed filter on the source", findExternalSource(result).pushedFilter());
    }

    public void testNotPushedWhenFactoryReportsNoSupport() throws Exception {
        // Kill switch: the factory reports NO filter-pushdown support (esql.jdbc.pushdown.enabled=false). The rule
        // resolves the connector via the map but gets a null support and must leave every filter in the engine.
        AtomicBoolean pushdownEnabled = new AtomicBoolean(false);
        try (
            JdbcConnectorFactory disabled = new JdbcConnectorFactory(
                registry,
                DialectRegistry.defaultRegistry(),
                SsrfGuard::defaultGuard,
                () -> true,
                pushdownEnabled::get
            )
        ) {
            Expression condition = greaterThanOf(ID, of(5));
            PhysicalPlan result = applyRule(condition, sourceFactories(disabled));

            assertThat("with pushdown disabled the filter must stay in the engine", result, instanceOf(FilterExec.class));
            assertNull("disabled pushdown means no pushed filter on the source", findExternalSource(result).pushedFilter());
        }
    }

    // -- helpers --

    private static ExternalOptimizerContext sourceFactories(ExternalSourceFactory factory) {
        // Keyed on the compound scheme, exactly as DataSourceModule registers the LazyConnectorFactory and as
        // JdbcConnectorFactory.resolveMetadata stamps sourceType.
        return new ExternalOptimizerContext(null, Map.of(SOURCE_TYPE, factory));
    }

    private static PhysicalPlan applyRule(Expression condition, ExternalOptimizerContext external) {
        ExternalSourceExec source = new ExternalSourceExec(
            Source.EMPTY,
            JDBC_URL,
            SOURCE_TYPE,
            List.of(ID, NAME),
            Map.of("table", "orders"),
            Map.of(),
            null
        );
        FilterExec filter = new FilterExec(Source.EMPTY, source, condition);
        LocalPhysicalOptimizerContext ctx = new LocalPhysicalOptimizerContext(null, null, null, null, null, external);
        return new PushFiltersToSource().apply(filter, ctx);
    }

    /** Finds the single {@link ExternalSourceExec} in the (small) result tree. */
    private static ExternalSourceExec findExternalSource(PhysicalPlan plan) {
        AtomicReference<ExternalSourceExec> found = new AtomicReference<>();
        plan.forEachDown(ExternalSourceExec.class, found::set);
        ExternalSourceExec external = found.get();
        assertNotNull("expected an ExternalSourceExec in the result plan: " + plan, external);
        return external;
    }
}
