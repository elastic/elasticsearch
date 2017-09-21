/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.cli;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import java.io.IOException;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;

/**
 * Test for setting the fetch size.
 */
public abstract class FetchSizeTestCase extends CliIntegrationTestCase {
    public void testSelect() throws IOException {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"test_field\":" + i + "}\n");
        }
        client().performRequest("PUT", "/test/doc/_bulk", singletonMap("refresh", "true"),
                new StringEntity(bulk.toString(), ContentType.APPLICATION_JSON));
        assertEquals("fetch size set to [90m4[0m", command("fetch size = 4"));
        assertEquals("fetch separator set to \"[90m -- fetch sep -- [0m\"", command("fetch separator = \" -- fetch sep -- \""));
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
        assertEquals("[1;31mInvalid fetch size [[22;3;33mcat[1;23;31m][0m", command("fetch size = cat"));
        assertEquals("[1;31mInvalid fetch size [[22;3;33m0[1;23;31m]. Must be > 0.[0m", command("fetch size = 0"));
        assertEquals("[1;31mInvalid fetch size [[22;3;33m-1231[1;23;31m]. Must be > 0.[0m", command("fetch size = -1231"));
        assertEquals("[1;31mInvalid fetch size [[22;3;33m" + Long.MAX_VALUE + "[1;23;31m][0m", command("fetch size = " + Long.MAX_VALUE));
    }
}
