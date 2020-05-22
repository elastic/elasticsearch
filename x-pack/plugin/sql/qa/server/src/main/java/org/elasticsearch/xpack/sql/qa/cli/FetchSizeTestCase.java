/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.cli;

import org.elasticsearch.client.Request;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

/**
 * Test for setting the fetch size.
 */
public abstract class FetchSizeTestCase extends CliIntegrationTestCase {
    public void testSelect() throws IOException {
        Request request = new Request("PUT", "/test/_bulk");
        request.addParameter("refresh", "true");
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"test_field\":" + i + "}\n");
        }
        request.setJsonEntity(bulk.toString());
        client().performRequest(request);

        assertEquals("[?1l>[?1000l[?2004lfetch size set to [90m4[0m", command("fetch size = 4"));
        assertEquals(
            "[?1l>[?1000l[?2004lfetch separator set to \"[90m -- fetch sep -- [0m\"",
            command("fetch separator = \" -- fetch sep -- \"")
        );
        assertThat(command("SELECT * FROM test ORDER BY test_field ASC"), containsString("test_field"));
        assertThat(readLine(), containsString("----------"));
        int i = 0;
        while (i < 20) {
            assertThat(readLine(), containsString(Integer.toString(i++)));
            assertThat(readLine(), containsString(Integer.toString(i++)));
            assertThat(readLine(), containsString(Integer.toString(i++)));
            assertThat(readLine(), containsString(Integer.toString(i++)));
            assertThat(readLine(), containsString(" -- fetch sep -- "));
        }
        assertEquals("", readLine());
    }

    public void testInvalidFetchSize() throws IOException {
        assertEquals(ErrorsTestCase.START + "Invalid fetch size [[3;33;22mcat" + ErrorsTestCase.END, command("fetch size = cat"));
        assertEquals(ErrorsTestCase.START + "Invalid fetch size [[3;33;22m0[23;31;1m]. Must be > 0.[0m", command("fetch size = 0"));
        assertEquals(ErrorsTestCase.START + "Invalid fetch size [[3;33;22m-1231[23;31;1m]. Must be > 0.[0m", command("fetch size = -1231"));
        assertEquals(
            ErrorsTestCase.START + "Invalid fetch size [[3;33;22m" + Long.MAX_VALUE + ErrorsTestCase.END,
            command("fetch size = " + Long.MAX_VALUE)
        );
    }

    // Test for issue: https://github.com/elastic/elasticsearch/issues/42851
    // Even though fetch size and limit are smaller than the noRows, all buckets
    // should be processed to achieve the global ordering of the aggregate function.
    public void testOrderingOnAggregate() throws IOException {
        Request request = new Request("PUT", "/test/_bulk");
        request.addParameter("refresh", "true");
        StringBuilder bulk = new StringBuilder();
        for (int i = 1; i <= 100; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"a\":").append(i).append(", \"b\" : ").append(i).append("}\n");
        }
        request.setJsonEntity(bulk.toString());
        client().performRequest(request);

        assertEquals("[?1l>[?1000l[?2004lfetch size set to [90m4[0m", command("fetch size = 4"));
        assertEquals(
            "[?1l>[?1000l[?2004lfetch separator set to \"[90m -- fetch sep -- [0m\"",
            command("fetch separator = \" -- fetch sep -- \"")
        );
        assertThat(command("SELECT max(b) FROM test GROUP BY a ORDER BY max(b) DESC LIMIT 20"), containsString("max(b)"));
        assertThat(readLine(), containsString("----------"));
        for (int i = 100; i > 80; i--) {
            if (i < 100 && i % 4 == 0) {
                assertThat(readLine(), containsString(" -- fetch sep -- "));
            }
            assertThat(readLine(), containsString(Integer.toString(i)));
        }
        assertEquals("", readLine());
    }
}
