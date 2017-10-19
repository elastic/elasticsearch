/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.security;

import org.elasticsearch.xpack.qa.sql.cli.RemoteCli;

import static org.elasticsearch.xpack.qa.sql.cli.CliIntegrationTestCase.elasticsearchAddress;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CliSecurityIT extends SqlSecurityTestCase {
    static String adminEsUrlPrefix() {
        return "test_admin:x-pack-test-password@";
    }

    /**
     * Perform security test actions using the CLI.
     */
    private static class CliActions implements Actions {
        @Override
        public void queryWorksAsAdmin() throws Exception {
            try (RemoteCli cli = new RemoteCli(adminEsUrlPrefix() + elasticsearchAddress())) {
                assertThat(cli.command("SELECT * FROM test ORDER BY a"), containsString("a       |       b       |       c"));
                assertEquals("---------------+---------------+---------------", cli.readLine());
                assertThat(cli.readLine(), containsString("1              |2              |3"));
                assertThat(cli.readLine(), containsString("4              |5              |6"));
                assertEquals("[0m", cli.readLine());        
            }
        }

        @Override
        public void expectMatchesAdmin(String adminSql, String user, String userSql) throws Exception {
            List<String> adminResult = new ArrayList<>();
            try (RemoteCli cli = new RemoteCli(adminEsUrlPrefix() + elasticsearchAddress())) {
                adminResult.add(cli.command(adminSql));
                String line;
                do {
                    line = cli.readLine();
                    adminResult.add(line);
                } while (false == line.equals("[0m"));
                adminResult.add(line);
            }

            Iterator<String> expected = adminResult.iterator();
            try (RemoteCli cli = new RemoteCli(userPrefix(user) + elasticsearchAddress())) {
                assertTrue(expected.hasNext());
                assertEquals(expected.next(), cli.command(userSql));
                String line;
                do {
                    line = cli.readLine();
                    assertTrue(expected.hasNext());
                    assertEquals(expected.next(), line);
                } while (false == line.equals("[0m"));
                assertTrue(expected.hasNext());
                assertEquals(expected.next(), line);
                assertFalse(expected.hasNext());
            }
        }

        @Override
        public void expectDescribe(Map<String, String> columns, String user) throws Exception {
            try (RemoteCli cli = new RemoteCli(userPrefix(user) + elasticsearchAddress())) {
                assertThat(cli.command("DESCRIBE test"), containsString("column     |     type"));
                assertEquals("---------------+---------------", cli.readLine());
                for (Map.Entry<String, String> column : columns.entrySet()) {
                    assertThat(cli.readLine(), both(startsWith(column.getKey())).and(containsString("|" + column.getValue())));
                }
                assertEquals("[0m", cli.readLine());
            }
        }

        @Override
        public void expectShowTables(List<String> tables, String user) throws Exception {
            try (RemoteCli cli = new RemoteCli(userPrefix(user) + elasticsearchAddress())) {
                assertThat(cli.command("SHOW TABLES"), containsString("table"));
                assertEquals("---------------", cli.readLine());
                for (String table : tables) {
                    assertThat(cli.readLine(), containsString(table));
                }
                assertEquals("[0m", cli.readLine());
            }
        }

        @Override
        public void expectForbidden(String user, String sql) throws Exception {
            try (RemoteCli cli = new RemoteCli(userPrefix(user) + elasticsearchAddress())) {
                assertThat(cli.command(sql), containsString("is unauthorized for user [" + user + "]"));
            }
        }

        @Override
        public void expectUnknownColumn(String user, String sql, String column) throws Exception {
            try (RemoteCli cli = new RemoteCli(userPrefix(user) + elasticsearchAddress())) {
                assertThat(cli.command(sql), containsString("[1;31mBad request"));
                assertThat(cli.readLine(), containsString("Unknown column [" + column + "][1;23;31m][0m"));
            }
        }

        private String userPrefix(String user) {
            if (user == null) {
                return adminEsUrlPrefix();
            }
            return user + ":testpass@";
        }
    }

    public CliSecurityIT() {
        super(new CliActions());
    }
}