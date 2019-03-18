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

import org.elasticsearch.client.security.user.privileges.IndicesPrivileges;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.client.security.user.privileges.Role.ClusterPrivilegeName;
import org.elasticsearch.client.security.user.privileges.Role.IndexPrivilegeName;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class CreateApiKeyRequestTests extends ESTestCase {

    public void test() throws IOException {
        List<Role> roles = new ArrayList<>();
        roles.add(Role.builder().name("r1").clusterPrivileges(ClusterPrivilegeName.ALL)
                .indicesPrivileges(IndicesPrivileges.builder().indices("ind-x").privileges(IndexPrivilegeName.ALL).build()).build());
        roles.add(Role.builder().name("r2").clusterPrivileges(ClusterPrivilegeName.ALL)
                .indicesPrivileges(IndicesPrivileges.builder().indices("ind-y").privileges(IndexPrivilegeName.ALL).build()).build());

        CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest("api-key", roles, null, null);
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        createApiKeyRequest.toXContent(builder, ToXContent.EMPTY_PARAMS);
        final String output = Strings.toString(builder);
        assertThat(output, equalTo(
                "{\"name\":\"api-key\",\"role_descriptors\":{\"r1\":{\"applications\":[],\"cluster\":[\"all\"],\"indices\":[{\"names\":"
                        + "[\"ind-x\"],\"privileges\":[\"all\"],\"allow_restricted_indices\":false}],\"metadata\":{},\"run_as\":[]},"
                        + "\"r2\":{\"applications\":[],\"cluster\":"
                        + "[\"all\"],\"indices\":[{\"names\":[\"ind-y\"],\"privileges\":[\"all\"],\"allow_restricted_indices\":false}],"
                        + "\"metadata\":{},\"run_as\":[]}}}"));
    }

    public void testEqualsHashCode() {
        final String name = randomAlphaOfLength(5);
        List<Role> roles = Collections.singletonList(Role.builder().name("r1").clusterPrivileges(ClusterPrivilegeName.ALL)
                .indicesPrivileges(IndicesPrivileges.builder().indices("ind-x").privileges(IndexPrivilegeName.ALL).build()).build());
        final TimeValue expiration = null;
        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());

        CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(name, roles, expiration, refreshPolicy);

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createApiKeyRequest, (original) -> {
            return new CreateApiKeyRequest(original.getName(), original.getRoles(), original.getExpiration(), original.getRefreshPolicy());
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createApiKeyRequest, (original) -> {
            return new CreateApiKeyRequest(original.getName(), original.getRoles(), original.getExpiration(), original.getRefreshPolicy());
        }, CreateApiKeyRequestTests::mutateTestItem);
    }

    private static CreateApiKeyRequest mutateTestItem(CreateApiKeyRequest original) {
        switch (randomIntBetween(0, 3)) {
        case 0:
            return new CreateApiKeyRequest(randomAlphaOfLength(5), original.getRoles(), original.getExpiration(),
                    original.getRefreshPolicy());
        case 1:
            return new CreateApiKeyRequest(original.getName(),
                    Collections.singletonList(Role.builder().name(randomAlphaOfLength(6)).clusterPrivileges(ClusterPrivilegeName.ALL)
                            .indicesPrivileges(
                                    IndicesPrivileges.builder().indices(randomAlphaOfLength(4)).privileges(IndexPrivilegeName.ALL).build())
                            .build()),
                    original.getExpiration(), original.getRefreshPolicy());
        case 2:
            return new CreateApiKeyRequest(original.getName(), original.getRoles(), TimeValue.timeValueSeconds(10000),
                    original.getRefreshPolicy());
        case 3:
            List<RefreshPolicy> values = Arrays.stream(RefreshPolicy.values()).filter(rp -> rp != original.getRefreshPolicy())
                    .collect(Collectors.toList());
            return new CreateApiKeyRequest(original.getName(), original.getRoles(), original.getExpiration(), randomFrom(values));
        default:
            return new CreateApiKeyRequest(randomAlphaOfLength(5), original.getRoles(), original.getExpiration(),
                    original.getRefreshPolicy());
        }
    }
}
