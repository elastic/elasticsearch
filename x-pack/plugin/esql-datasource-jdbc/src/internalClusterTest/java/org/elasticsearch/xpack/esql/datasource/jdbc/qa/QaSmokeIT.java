/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc.qa;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.datasource.jdbc.JdbcDataSourcePlugin;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;

/**
 * Smoke test for the JDBC QA harness skeleton. It proves three things without any Docker or real database:
 * <ol>
 *   <li>the harness boots an {@link AbstractEsqlIntegTestCase} node and the ES|QL engine answers a trivial query;</li>
 *   <li>{@link JdbcDataSourcePlugin} is actually loaded on that node (reported via the node-info plugins list);</li>
 *   <li>the {@link JdbcDatabaseFixture} base wiring (URL + start/stop + DDL/DML load seam) compiles and executes,
 *       exercised against an in-memory H2 fixture defined locally here.</li>
 * </ol>
 * The reusable {@code H2Fixture}/{@code H2JdbcIT} end-to-end {@code FROM jdbc:...} path is exercised separately;
 * this class only smoke-checks that the skeleton stands up.
 * <p>
 * Single-node, SUITE scope — same rationale as {@code JdbcDatasetIT}: keeps the boot fast and avoids the unrelated
 * multi-node dataset-publication assertion.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class QaSmokeIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(JdbcDataSourcePlugin.class);
        return plugins;
    }

    public void testHarnessClusterRunsEsql() {
        try (EsqlQueryResponse response = run("ROW n = 1 | EVAL doubled = n * 2")) {
            assertColumnNames(response.columns(), List.of("n", "doubled"));
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo(1));
            assertThat(rows.get(0).get(1), equalTo(2));
        }
    }

    public void testJdbcDataSourcePluginLoaded() {
        NodesInfoResponse nodesInfo = clusterAdmin().prepareNodesInfo().clear().setPlugins(true).get();
        for (NodeInfo nodeInfo : nodesInfo.getNodes()) {
            List<String> pluginClassNames = nodeInfo.getInfo(PluginsAndModules.class)
                .getPluginInfos()
                .stream()
                .map(p -> p.descriptor().getClassname())
                .toList();
            assertThat(
                "JdbcDataSourcePlugin not reported loaded on node [" + nodeInfo.getNode().getName() + "]: " + pluginClassNames,
                pluginClassNames,
                hasItem(JdbcDataSourcePlugin.class.getName())
            );
        }
    }

    public void testJdbcDatabaseFixtureDdlDmlSeam() throws Exception {
        try (H2SmokeFixture fixture = new H2SmokeFixture()) {
            fixture.start();
            fixture.load("CREATE TABLE t (id INTEGER, name VARCHAR(16))", "INSERT INTO t VALUES (1, 'alpha'), (2, 'beta')");
            try (
                Connection connection = fixture.newConnection();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM t")
            ) {
                assertTrue("expected a COUNT(*) row", rs.next());
                assertThat(rs.getInt(1), equalTo(2));
            }
            assertThat(fixture.esqlJdbcUrl(), startsWith("jdbc:h2:mem:"));
        }
    }

    /**
     * In-memory H2 fixture local to this smoke test. Deliberately not the reusable {@code H2Fixture} — this only
     * proves the {@link JdbcDatabaseFixture} base seams work. {@code DB_CLOSE_DELAY=-1}
     * keeps the in-mem database alive for the fixture's lifetime; {@code DATABASE_TO_UPPER=false} preserves the
     * lower-case identifiers the fixtures use.
     */
    private static final class H2SmokeFixture extends JdbcDatabaseFixture {
        private final String url = "jdbc:h2:mem:qa_smoke;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false";

        @Override
        public String esqlJdbcUrl() {
            return url;
        }

        @Override
        protected String driverClassName() {
            return "org.h2.Driver";
        }
    }
}
