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

import org.elasticsearch.client.security.PutPrivilegesResponse.Status;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
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

        final PutPrivilegesResponse putPrivilegesResponse = PutPrivilegesResponse.fromXContent(
                XContentType.JSON.xContent().createParser(new NamedXContentRegistry(Collections.emptyList()), new DeprecationHandler() {
                    @Override
                    public void usedDeprecatedName(String usedName, String modernName) {
                    }

                    @Override
                    public void usedDeprecatedField(String usedName, String replacedWith) {
                    }
                }, json));

        assertThat(putPrivilegesResponse.status("app02", "all"), is(Status.CREATED));
        assertThat(putPrivilegesResponse.status("app01", "read"), is(Status.UPDATED));
        assertThat(putPrivilegesResponse.status("app01", "write"), is(Status.CREATED));
        assertThat(putPrivilegesResponse.status("unknown-app", "unknown-priv"), is(Status.UNKNOWN));
        assertThat(putPrivilegesResponse.status("app01", "unknown-priv"), is(Status.UNKNOWN));
    }

    public void testGetStatusFailsForInvalidApplicationOrPrivilegeName() {
        final PutPrivilegesResponse putPrivilegesResponse = new PutPrivilegesResponse(
                Collections.singletonMap("app-1", Collections.singletonMap("priv", Status.CREATED)));
        final boolean nullOrEmptyAppName = randomBoolean();
        final String applicationName = nullOrEmptyAppName ? randomFrom(Arrays.asList("", "    ", null)) : randomAlphaOfLength(4);
        final String privilegeName = nullOrEmptyAppName ? randomAlphaOfLength(4) : randomFrom(Arrays.asList("", "    ", null));
        IllegalArgumentException ile = expectThrows(IllegalArgumentException.class,
                () -> putPrivilegesResponse.status(applicationName, privilegeName));
        assertThat(ile.getMessage(),
                (nullOrEmptyAppName ? equalTo("application name is required") : equalTo("privilege name is required")));
    }
}
