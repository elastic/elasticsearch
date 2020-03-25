/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
