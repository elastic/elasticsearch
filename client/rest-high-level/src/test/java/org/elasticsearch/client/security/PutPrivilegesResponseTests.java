/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class PutPrivilegesResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        final String json = "{\n" +
                "  \"app02\": {\n" +
                "    \"all\": {\n" +
                "      \"created\": true\n" +
                "    }\n" +
                "  },\n" +
                "  \"app01\": {\n" +
                "    \"read\": {\n" +
                "      \"created\": false\n" +
                "    },\n" +
                "    \"write\": {\n" +
                "      \"created\": true\n" +
                "    }\n" +
                "  }\n" +
                "}";

        final PutPrivilegesResponse putPrivilegesResponse = PutPrivilegesResponse
                .fromXContent(createParser(XContentType.JSON.xContent(), json));

        assertThat(putPrivilegesResponse.wasCreated("app02", "all"), is(true));
        assertThat(putPrivilegesResponse.wasCreated("app01", "read"), is(false));
        assertThat(putPrivilegesResponse.wasCreated("app01", "write"), is(true));
        expectThrows(IllegalArgumentException.class, () -> putPrivilegesResponse.wasCreated("unknown-app", "unknown-priv"));
        expectThrows(IllegalArgumentException.class, () -> putPrivilegesResponse.wasCreated("app01", "unknown-priv"));
    }

    public void testGetStatusFailsForUnknownApplicationOrPrivilegeName() {
        final PutPrivilegesResponse putPrivilegesResponse = new PutPrivilegesResponse(
                Collections.singletonMap("app-1", Collections.singletonMap("priv", true)));

        final boolean invalidAppName = randomBoolean();
        final String applicationName = (invalidAppName) ? randomAlphaOfLength(4) : "app-1";
        final String privilegeName = randomAlphaOfLength(4);

        final IllegalArgumentException ile = expectThrows(IllegalArgumentException.class,
                () -> putPrivilegesResponse.wasCreated(applicationName, privilegeName));
        assertThat(ile.getMessage(), equalTo("application name or privilege name not found in the response"));
    }

    public void testGetStatusFailsForNullOrEmptyApplicationOrPrivilegeName() {
        final PutPrivilegesResponse putPrivilegesResponse = new PutPrivilegesResponse(
                Collections.singletonMap("app-1", Collections.singletonMap("priv", true)));

        final boolean nullOrEmptyAppName = randomBoolean();
        final String applicationName = (nullOrEmptyAppName) ? randomFrom(Arrays.asList("", "    ", null)) : "app-1";
        final String privilegeName = randomFrom(Arrays.asList("", "    ", null));
        final IllegalArgumentException ile = expectThrows(IllegalArgumentException.class,
                () -> putPrivilegesResponse.wasCreated(applicationName, privilegeName));
        assertThat(ile.getMessage(),
                (nullOrEmptyAppName ? equalTo("application name is required") : equalTo("privilege name is required")));
    }
}
