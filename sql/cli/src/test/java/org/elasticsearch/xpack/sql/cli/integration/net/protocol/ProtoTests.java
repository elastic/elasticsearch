/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.integration.net.protocol;

import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.sql.cli.CliConfiguration;
import org.elasticsearch.xpack.sql.cli.CliHttpServer;
import org.elasticsearch.xpack.sql.cli.net.client.CliHttpClient;
import org.elasticsearch.xpack.sql.cli.net.protocol.CommandResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.ExceptionResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.Response;
import org.elasticsearch.xpack.sql.net.client.SuppressForbidden;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

@AwaitsFix(bugUrl = "being ported")
@SuppressForbidden(reason = "being ported")
public class ProtoTests extends ESTestCase {
    // NOCOMMIT port remaining demos

    private static Client esClient;
    private static CliHttpServer server;
    private static CliHttpClient client;

    @BeforeClass
    public static void setupServer() throws Exception {
        if (esClient == null) {
            esClient = new PreBuiltTransportClient(Settings.EMPTY);
        }
        if (server == null) {
            server = new CliHttpServer(esClient);
            server.start(0);
        }

        if (client == null) {
            CliConfiguration ci = new CliConfiguration(server.url(), new Properties());
            client = new CliHttpClient(ci);
        }
    }

    @AfterClass
    public static void shutdownServer() {
        if (server != null) {
            server.stop();
            server = null;
        }

        if (client != null) {
            client.close();
            client = null;
        }

        if (esClient != null) {
            esClient.close();
            esClient = null;
        }
    }

    public void testInfoAction() throws Exception {
        // NOCOMMIT port to CliIntegrationTestCase
        InfoResponse esInfo = (InfoResponse) client.serverInfo();
        assertThat(esInfo, notNullValue());
        assertThat(esInfo.cluster, is("elasticsearch"));
        assertThat(esInfo.node, not(isEmptyOrNullString()));
        assertThat(esInfo.versionHash, not(isEmptyOrNullString()));
        assertThat(esInfo.versionString, startsWith("5."));
        assertThat(esInfo.majorVersion, is(5));
        //assertThat(esInfo.minorVersion(), is(0));
    }

    public void testDemo() throws Exception {
        // add math functions

        // add statistical function + explanation on optimization

        List<String> commands = Arrays.asList(
                "DESCRIBE emp.emp",
                "SELECT * FROM emp.emp",
                "SELECT * FROM emp.emp LIMIT 5",
                "SELECT last_name l, first_name f FROM emp.emp WHERE gender = 'F' LIMIT 5",
                "SELECT last_name l, first_name f FROM emp.emp WHERE tenure > 30 ORDER BY salary LIMIT 5",
                "SELECT * FROM emp.emp WHERE MATCH(first_name, 'Mary')",
                "SELECT * FROM emp.emp WHERE MATCH('first_name,last_name', 'Morton', 'type=best_fields;default_operator=OR')",
                "SELECT * FROM emp.emp WHERE QUERY('Elvis Alain')",
                "SELECT LOG(salary) FROM emp.emp",
                "SELECT salary s, LOG(salary) m FROM emp.emp LIMIT 5",
                "SELECT salary s, EXP(LOG(salary)) m FROM emp.emp LIMIT 5",
                "SELECT salary s, ROUND(EXP(LOG(salary))) m FROM emp.emp LIMIT 5",
                "SELECT salary s, ROUND(EXP(LOG(salary))) m FROM emp.emp ORDER BY ROUND(LOG(emp_no)) LIMIT 5",
                "SELECT year(birth_date) year, last_name l, first_name f "
                        + "FROM emp.emp WHERE year(birth_date) <=1960 AND tenure < 25 ORDER BY year LIMIT 5",
                "SELECT COUNT(*) FROM emp.emp",
                "SELECT COUNT(*) FROM emp.emp WHERE emp_no >= 10010",
                "SELECT tenure, COUNT(*) count, MIN(salary) min, AVG(salary) avg, MAX(salary) max FROM emp.emp GROUP BY tenure",
                "SELECT YEAR(birth_date) born, COUNT(*) count, MIN(salary) min, AVG(salary) avg, MAX(salary) max "
                        + "FROM emp.emp GROUP BY born",
                "SELECT tenure, gender, COUNT(tenure) count, AVG(salary) avg FROM emp.emp GROUP BY tenure, gender HAVING avg > 50000",
                "SELECT gender, tenure, AVG(salary) avg FROM emp.emp GROUP BY gender, tenure HAVING avg > 50000 ORDER BY tenure DESC",
                // nested docs
                "DESCRIBE emp.emp", 
                "SELECT dep FROM emp.emp",
                "SELECT dep.dept_name, first_name, last_name FROM emp.emp WHERE emp_no = 10020",
                "SELECT first_name f, last_name l, dep.from_date FROM emp.emp WHERE dep.dept_name = 'Production' ORDER BY dep.from_date",
                "SELECT first_name f, last_name l, YEAR(dep.from_date) start "
                        + "FROM emp.emp WHERE dep.dept_name = 'Production' AND tenure > 30 ORDER BY start"
                );
        
        for (String c : commands) {
            Response command = client.command(c, null);
            String msg = "";
            if (command instanceof ExceptionResponse) {
                msg = ((ExceptionResponse) command).message;
            }
            if (command instanceof CommandResponse) {
                msg = ((CommandResponse) command).data.toString();
            }
            System.out.println(msg);
        }
    }
}