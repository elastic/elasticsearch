/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import java.io.IOException;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;

/**
 * Test for setting the fetch size.
 */
public class FetchSizeIT extends CliIntegrationTestCase {
    public void testSelect() throws IOException {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            bulk.append("{\"index\":{}}\n");
            bulk.append("{\"test_field\":" + i + "}\n");
        }
        client().performRequest("PUT", "/test/doc/_bulk", singletonMap("refresh", "true"),
                new StringEntity(bulk.toString(), ContentType.APPLICATION_JSON));
        command("fetch size = 4");
        assertEquals("fetch size set to 4", in.readLine());
        command("fetch separator = \" -- fetch sep -- \"");
        assertEquals("fetch separator set to \" -- fetch sep -- \"", in.readLine());
        command("SELECT * FROM test ORDER BY test_field ASC");
        assertThat(in.readLine(), containsString("test_field"));
        assertThat(in.readLine(), containsString("----------"));
        int i = 0;
        while (i < 20) {
            assertThat(in.readLine(), containsString(Integer.toString(i++)));
            assertThat(in.readLine(), containsString(Integer.toString(i++)));
            assertThat(in.readLine(), containsString(Integer.toString(i++)));
            assertThat(in.readLine(), containsString(Integer.toString(i++)));
            assertEquals(" -- fetch sep -- ", in.readLine());
        }
        assertEquals("", in.readLine());
    }

    public void testInvalidFetchSize() throws IOException {
        command("fetch size = cat");
        assertEquals("Invalid fetch size [cat]", in.readLine());
        command("fetch size = 0");
        assertEquals("Invalid fetch size [0]. Must be > 0.", in.readLine());
        command("fetch size = -1231");
        assertEquals("Invalid fetch size [-1231]. Must be > 0.", in.readLine());
        command("fetch size = " + Long.MAX_VALUE);
        assertEquals("Invalid fetch size [" + Long.MAX_VALUE + "]", in.readLine());
    }
}
