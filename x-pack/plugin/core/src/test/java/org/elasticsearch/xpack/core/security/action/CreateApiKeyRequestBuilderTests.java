/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class CreateApiKeyRequestBuilderTests extends ESTestCase {

    public void testParserAndCreateApiRequestBuilder() throws IOException {
        boolean withExpiration = randomBoolean();
        final String json = "{ \"name\" : \"my-api-key\", "
                + ((withExpiration) ? " \"expiration\": \"1d\", " : "")
                +" \"role_descriptors\": { \"role-a\": {\"cluster\":[\"a-1\", \"a-2\"],"
                + " \"index\": [{\"names\": [\"indx-a\"], \"privileges\": [\"read\"] }] }, "
                + " \"role-b\": {\"cluster\":[\"b\"],"
                + " \"index\": [{\"names\": [\"indx-b\"], \"privileges\": [\"read\"] }] } "
                + "} }";
        final BytesArray source = new BytesArray(json);
        final NodeClient mockClient = mock(NodeClient.class);
        final CreateApiKeyRequest request = new CreateApiKeyRequestBuilder(mockClient).source(source, XContentType.JSON).request();
        final List<RoleDescriptor> actualRoleDescriptors = request.getRoleDescriptors();
        assertThat(request.getName(), equalTo("my-api-key"));
        assertThat(actualRoleDescriptors.size(), is(2));
        for (RoleDescriptor rd : actualRoleDescriptors) {
            String[] clusters = null;
            IndicesPrivileges indicesPrivileges = null;
            if (rd.getName().equals("role-a")) {
                clusters = new String[] { "a-1", "a-2" };
                indicesPrivileges = RoleDescriptor.IndicesPrivileges.builder().indices("indx-a").privileges("read").build();
            } else if (rd.getName().equals("role-b")){
                clusters = new String[] { "b" };
                indicesPrivileges = RoleDescriptor.IndicesPrivileges.builder().indices("indx-b").privileges("read").build();
            } else {
                fail("unexpected role name");
            }
            assertThat(rd.getClusterPrivileges(), arrayContainingInAnyOrder(clusters));
            assertThat(rd.getIndicesPrivileges(),
                    arrayContainingInAnyOrder(indicesPrivileges));
        }
        if (withExpiration) {
            assertThat(request.getExpiration(), equalTo(TimeValue.parseTimeValue("1d", "expiration")));
        }
    }

    public void testParserAndCreateApiRequestBuilderWithNullOrEmptyRoleDescriptors() throws IOException {
        boolean withExpiration = randomBoolean();
        boolean noRoleDescriptorsField = randomBoolean();
        final String json = "{ \"name\" : \"my-api-key\""
                + ((withExpiration) ? ", \"expiration\": \"1d\"" : "")
                + ((noRoleDescriptorsField) ? "" : ", \"role_descriptors\": {}")
                + "}";
        final BytesArray source = new BytesArray(json);
        final NodeClient mockClient = mock(NodeClient.class);
        final CreateApiKeyRequest request = new CreateApiKeyRequestBuilder(mockClient).source(source, XContentType.JSON).request();
        final List<RoleDescriptor> actualRoleDescriptors = request.getRoleDescriptors();
        assertThat(request.getName(), equalTo("my-api-key"));
        assertThat(actualRoleDescriptors.size(), is(0));
        if (withExpiration) {
            assertThat(request.getExpiration(), equalTo(TimeValue.parseTimeValue("1d", "expiration")));
        }
    }
}
