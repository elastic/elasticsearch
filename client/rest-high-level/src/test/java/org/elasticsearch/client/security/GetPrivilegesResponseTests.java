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

import org.elasticsearch.client.security.user.privileges.ApplicationPrivilege;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class GetPrivilegesResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        final String json = "{" +
            "  \"testapp\": {" +
            "    \"read\": {" +
            "      \"application\": \"testapp\"," +
            "      \"name\": \"read\"," +
            "      \"actions\": [ \"action:login\", \"data:read/*\" ]" +
            "    }," +
            "    \"write\": {" +
            "      \"application\": \"testapp\"," +
            "      \"name\": \"write\"," +
            "      \"actions\": [ \"action:login\", \"data:write/*\" ]," +
            "      \"metadata\": { \"key1\": \"value1\" }" +
            "    }," +
            "    \"all\": {" +
            "      \"application\": \"testapp\"," +
            "      \"name\": \"all\"," +
            "      \"actions\": [ \"action:login\", \"data:write/*\" , \"manage:*\"]" +
            "    }" +
            "  }," +
            "  \"testapp2\": {" +
            "    \"read\": {" +
            "      \"application\": \"testapp2\"," +
            "      \"name\": \"read\"," +
            "      \"actions\": [ \"action:login\", \"data:read/*\" ]," +
            "      \"metadata\": { \"key2\": \"value2\" }" +
            "    }," +
            "    \"write\": {" +
            "      \"application\": \"testapp2\"," +
            "      \"name\": \"write\"," +
            "      \"actions\": [ \"action:login\", \"data:write/*\" ]" +
            "    }," +
            "    \"all\": {" +
            "      \"application\": \"testapp2\"," +
            "      \"name\": \"all\"," +
            "      \"actions\": [ \"action:login\", \"data:write/*\" , \"manage:*\"]" +
            "    }" +
            "  }" +
            "}";

        final GetPrivilegesResponse response = GetPrivilegesResponse.fromXContent(XContentType.JSON.xContent().createParser(
            new NamedXContentRegistry(Collections.emptyList()), DeprecationHandler.IGNORE_DEPRECATIONS, json));

        final ApplicationPrivilege readTestappPrivilege =
            new ApplicationPrivilege("testapp", "read", Arrays.asList("action:login", "data:read/*"), null);
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        final ApplicationPrivilege writeTestappPrivilege =
            new ApplicationPrivilege("testapp", "write", Arrays.asList("action:login", "data:write/*"), metadata);
        final ApplicationPrivilege allTestappPrivilege =
            new ApplicationPrivilege("testapp", "all", Arrays.asList("action:login", "data:write/*", "manage:*"), null);
        final Map<String, Object> metadata2 = new HashMap<>();
        metadata2.put("key2", "value2");
        final ApplicationPrivilege readTestapp2Privilege =
            new ApplicationPrivilege("testapp2", "read", Arrays.asList("action:login", "data:read/*"), metadata2);
        final ApplicationPrivilege writeTestapp2Privilege =
            new ApplicationPrivilege("testapp2", "write", Arrays.asList("action:login", "data:write/*"), null);
        final ApplicationPrivilege allTestapp2Privilege =
            new ApplicationPrivilege("testapp2", "all", Arrays.asList("action:login", "data:write/*", "manage:*"), null);
        final GetPrivilegesResponse exptectedResponse =
            new GetPrivilegesResponse(Arrays.asList(readTestappPrivilege, writeTestappPrivilege, allTestappPrivilege,
                readTestapp2Privilege, writeTestapp2Privilege, allTestapp2Privilege));
        assertThat(response, equalTo(exptectedResponse));
    }

    public void testEqualsHashCode() {
        final List<ApplicationPrivilege> privileges = new ArrayList<>();
        final List<ApplicationPrivilege> privileges2 = new ArrayList<>();
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        final ApplicationPrivilege writePrivilege =
            new ApplicationPrivilege("testapp", "write", Arrays.asList("action:login", "data:write/*"),
                metadata);
        final ApplicationPrivilege readPrivilege =
            new ApplicationPrivilege("testapp", "read", Arrays.asList("data:read/*", "action:login"),
                metadata);
        privileges.add(readPrivilege);
        privileges.add(writePrivilege);
        privileges2.add(writePrivilege);
        privileges2.add(readPrivilege);
        final GetPrivilegesResponse response = new GetPrivilegesResponse(privileges);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(response, (original) -> {
            return new GetPrivilegesResponse(original.getPrivileges());
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(response, (original) -> {
            return new GetPrivilegesResponse(original.getPrivileges());
        }, GetPrivilegesResponseTests::mutateTestItem);
    }

    private static GetPrivilegesResponse mutateTestItem(GetPrivilegesResponse original) {
        if (randomBoolean()) {
            Set<ApplicationPrivilege> originalPrivileges = original.getPrivileges();
            Set<ApplicationPrivilege> privileges = new HashSet<>();
            privileges.addAll(originalPrivileges);
            privileges.add(new ApplicationPrivilege("testapp", "all", Arrays.asList("action:login", "data:read/*", "manage:*"), null));
            return new GetPrivilegesResponse(privileges);
        } else {
            final List<ApplicationPrivilege> privileges = new ArrayList<>();
            final ApplicationPrivilege privilege =
                new ApplicationPrivilege("testapp", "all", Arrays.asList("action:login", "data:write/*", "manage:*"), null);
            privileges.add(privilege);
            return new GetPrivilegesResponse(privileges);
        }
    }
}
