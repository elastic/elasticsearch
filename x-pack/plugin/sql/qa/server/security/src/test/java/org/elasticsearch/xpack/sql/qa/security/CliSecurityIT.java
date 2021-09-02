/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.security;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.xpack.sql.qa.cli.EmbeddedCli;
import org.elasticsearch.xpack.sql.qa.cli.EmbeddedCli.SecurityConfig;
import org.elasticsearch.xpack.sql.qa.cli.ErrorsTestCase;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.sql.qa.cli.CliIntegrationTestCase.elasticsearchAddress;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

public class CliSecurityIT extends SqlSecurityTestCase {

    @Override
    public void testDescribeWorksAsFullAccess() {}

    @Override
    public void testQuerySingleFieldGranted() {}

    @Override
    public void testScrollWithSingleFieldExcepted() {}

    @Override
    public void testQueryWorksAsAdmin() {}

    static SecurityConfig adminSecurityConfig() {
        String keystoreLocation;
        String keystorePassword;
        if (RestSqlIT.SSL_ENABLED) {
            Path keyStore;
            try {
                keyStore = PathUtils.get(RestSqlIT.class.getResource("/test-node.jks").toURI());
            } catch (URISyntaxException e) {
                throw new RuntimeException("exception while reading the store", e);
            }
            if (Files.exists(keyStore) == false) {
                throw new IllegalStateException("Keystore file [" + keyStore + "] does not exist.");
            }
            keystoreLocation = keyStore.toAbsolutePath().toString();
            keystorePassword = "keypass";
        } else {
            keystoreLocation = null;
            keystorePassword = null;
        }
        return new SecurityConfig(RestSqlIT.SSL_ENABLED, "test_admin", "x-pack-test-password", keystoreLocation, keystorePassword);
    }

    /**
     * Perform security test actions using the CLI.
     */
    private static class CliActions implements Actions {
        @Override
        public String minimalPermissionsForAllActions() {
            return "cli_or_drivers_minimal";
        }

        private SecurityConfig userSecurity(String user) {
            SecurityConfig admin = adminSecurityConfig();
            if (user == null) {
                return admin;
            }
            return new SecurityConfig(
                RestSqlIT.SSL_ENABLED,
                user,
                "test-user-password",
                admin.keystoreLocation(),
                admin.keystorePassword()
            );
        }

        @Override
        public void queryWorksAsAdmin() throws Exception {
            try (EmbeddedCli cli = new EmbeddedCli(elasticsearchAddress(), true, adminSecurityConfig())) {
                assertThat(cli.command("SELECT * FROM test ORDER BY a"), containsString("a       |       b       |       c"));
                assertEquals("---------------+---------------+---------------", cli.readLine());
                assertThat(cli.readLine(), containsString("1              |2              |3"));
                assertThat(cli.readLine(), containsString("4              |5              |6"));
                assertEquals("", cli.readLine());
            }
        }

        @Override
        public void expectMatchesAdmin(String adminSql, String user, String userSql) throws Exception {
            expectMatchesAdmin(adminSql, user, userSql, cli -> {});
        }

        @Override
        public void expectScrollMatchesAdmin(String adminSql, String user, String userSql) throws Exception {
            expectMatchesAdmin(adminSql, user, userSql, cli -> {
                assertEquals("[?1l>[?1000l[?2004lfetch size set to [90m1[0m", cli.command("fetch size = 1"));
                assertEquals(
                    "[?1l>[?1000l[?2004lfetch separator set to \"[90m -- fetch sep -- [0m\"",
                    cli.command("fetch separator = \" -- fetch sep -- \"")
                );
            });
        }

        public void expectMatchesAdmin(String adminSql, String user, String userSql, CheckedConsumer<EmbeddedCli, Exception> customizer)
            throws Exception {
            List<String> adminResult = new ArrayList<>();
            try (EmbeddedCli cli = new EmbeddedCli(elasticsearchAddress(), true, adminSecurityConfig())) {
                customizer.accept(cli);
                adminResult.add(cli.command(adminSql));
                String line;
                do {
                    line = cli.readLine();
                    adminResult.add(line);
                } while (false == (line.equals("[0m") || line.equals("")));
                adminResult.add(line);
            }

            Iterator<String> expected = adminResult.iterator();
            try (EmbeddedCli cli = new EmbeddedCli(elasticsearchAddress(), true, userSecurity(user))) {
                customizer.accept(cli);
                assertTrue(expected.hasNext());
                assertEquals(expected.next(), cli.command(userSql));
                String line;
                do {
                    line = cli.readLine();
                    assertTrue(expected.hasNext());
                    assertEquals(expected.next(), line);
                } while (false == (line.equals("[0m") || line.equals("")));
                assertTrue(expected.hasNext());
                assertEquals(expected.next(), line);
                assertFalse(expected.hasNext());
            }
        }

        @Override
        public void expectDescribe(Map<String, List<String>> columns, String user) throws Exception {
            try (EmbeddedCli cli = new EmbeddedCli(elasticsearchAddress(), true, userSecurity(user))) {
                String output = cli.command("DESCRIBE test");
                assertThat(output, containsString("column"));
                assertThat(output, containsString("type"));
                assertThat(output, containsString("mapping"));
                assertThat(cli.readLine(), containsString("-+---------------+---------------"));
                for (Map.Entry<String, List<String>> column : columns.entrySet()) {
                    String line = cli.readLine();
                    assertThat(line, startsWith(column.getKey()));
                    for (String value : column.getValue()) {
                        assertThat(line, containsString(value));
                    }
                }
                assertEquals("", cli.readLine());
            }
        }

        @Override
        public void expectShowTables(List<String> tables, String user) throws Exception {
            try (EmbeddedCli cli = new EmbeddedCli(elasticsearchAddress(), true, userSecurity(user))) {
                String tablesOutput = cli.command("SHOW TABLES");
                assertThat(tablesOutput, containsString("name"));
                assertThat(tablesOutput, containsString("type"));
                assertThat(tablesOutput, containsString("kind"));
                assertEquals("---------------+---------------+---------------", cli.readLine());
                for (String table : tables) {
                    String line = null;
                    /*
                     * Security automatically creates either a `.security` or a
                     * `.security6` index but it might not have created the index
                     * by the time the test runs.
                     */
                    while (line == null || line.startsWith(".security")) {
                        line = cli.readLine();
                    }
                    assertThat(line, containsString(table));
                }
                assertEquals("", cli.readLine());
            }
        }

        @Override
        public void expectUnknownIndex(String user, String sql) throws Exception {
            try (EmbeddedCli cli = new EmbeddedCli(elasticsearchAddress(), true, userSecurity(user))) {
                ErrorsTestCase.assertFoundOneProblem(cli.command(sql));
                assertThat(cli.readLine(), containsString("Unknown index"));
            }
        }

        @Override
        public void expectForbidden(String user, String sql) throws Exception {
            /*
             * Cause the CLI to skip its connection test on startup so we
             * can get a forbidden exception when we run the query.
             */
            try (EmbeddedCli cli = new EmbeddedCli(elasticsearchAddress(), false, userSecurity(user))) {
                assertThat(cli.command(sql), containsString("is unauthorized for user [" + user + "]"));
            }
        }

        @Override
        public void expectUnknownColumn(String user, String sql, String column) throws Exception {
            try (EmbeddedCli cli = new EmbeddedCli(elasticsearchAddress(), true, userSecurity(user))) {
                ErrorsTestCase.assertFoundOneProblem(cli.command(sql));
                assertThat(cli.readLine(), containsString("Unknown column [" + column + "]" + ErrorsTestCase.END));
            }
        }

        @Override
        public void checkNoMonitorMain(String user) throws Exception {
            // Building the cli will attempt the connection and run the assertion
            @SuppressWarnings("resource")  // forceClose will close it
            EmbeddedCli cli = new EmbeddedCli(elasticsearchAddress(), true, userSecurity(user)) {
                @Override
                protected void assertConnectionTest() throws IOException {
                    assertThat(readLine(), containsString("action [cluster:monitor/main] is unauthorized for user [" + user + "]"));
                }
            };
            cli.forceClose();
        }
    }

    public CliSecurityIT() {
        super(new CliActions());
    }
}
