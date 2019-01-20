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

import org.elasticsearch.client.security.user.privileges.GlobalOperationPrivilege;
import org.elasticsearch.client.security.user.privileges.IndicesPrivileges;
import org.elasticsearch.client.security.user.privileges.UserIndicesPrivileges;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.nullValue;

public class GetUserPrivilegesResponseTests extends ESTestCase {

    public void testParse() throws Exception {
        String json = "{" +
            "\"cluster\":[\"manage\",\"manage_security\",\"monitor\"]," +
            "\"global\":[" +
            " {\"application\":{\"manage\":{\"applications\":[\"test-*\"]}}}," +
            " {\"application\":{\"manage\":{\"applications\":[\"apps-*\"]}}}" +
            "]," +
            "\"indices\":[" +
            " {\"names\":[\"test-1-*\"],\"privileges\":[\"read\"],\"allow_restricted_indices\": false}," +
            " {\"names\":[\"test-4-*\"],\"privileges\":[\"read\"],\"allow_restricted_indices\": true," +
            "  \"field_security\":[{\"grant\":[\"*\"],\"except\":[\"private-*\"]}]}," +
            " {\"names\":[\"test-6-*\",\"test-7-*\"],\"privileges\":[\"read\"],\"allow_restricted_indices\": true," +
            "  \"query\":[\"{\\\"term\\\":{\\\"test\\\":true}}\"]}," +
            " {\"names\":[\"test-2-*\"],\"privileges\":[\"read\"],\"allow_restricted_indices\": false," +
            "  \"field_security\":[{\"grant\":[\"*\"],\"except\":[\"secret-*\",\"private-*\"]},{\"grant\":[\"apps-*\"]}]," +
            "  \"query\":[\"{\\\"term\\\":{\\\"test\\\":true}}\",\"{\\\"term\\\":{\\\"apps\\\":true}}\"]}," +
            " {\"names\":[\"test-3-*\",\"test-6-*\"],\"privileges\":[\"read\",\"write\"],\"allow_restricted_indices\": true}," +
            " {\"names\":[\"test-3-*\",\"test-4-*\",\"test-5-*\"],\"privileges\":[\"read\"],\"allow_restricted_indices\": false," +
            "  \"field_security\":[{\"grant\":[\"test-*\"]}]}," +
            " {\"names\":[\"test-1-*\",\"test-9-*\"],\"privileges\":[\"all\"],\"allow_restricted_indices\": true}" +
            "]," +
            "\"applications\":[" +
            " {\"application\":\"app-dne\",\"privileges\":[\"all\"],\"resources\":[\"*\"]}," +
            " {\"application\":\"test-app\",\"privileges\":[\"read\"],\"resources\":[\"object/1\",\"object/2\"]}," +
            " {\"application\":\"test-app\",\"privileges\":[\"user\",\"dne\"],\"resources\":[\"*\"]}" +
            "]," +
            "\"run_as\":[\"app-*\",\"test-*\"]}";
        final XContentParser parser = createParser(XContentType.JSON.xContent(), json);
        final GetUserPrivilegesResponse response = GetUserPrivilegesResponse.fromXContent(parser);

        assertThat(response.getClusterPrivileges(), contains("manage", "manage_security", "monitor"));

        assertThat(response.getGlobalPrivileges().size(), equalTo(2));
        assertThat(Iterables.get(response.getGlobalPrivileges(), 0).getPrivileges().size(), equalTo(1));
        assertManageApplicationsPrivilege(Iterables.get(response.getGlobalPrivileges(), 0).getPrivileges().iterator().next(), "test-*");
        assertThat(Iterables.get(response.getGlobalPrivileges(), 1).getPrivileges().size(), equalTo(1));
        assertManageApplicationsPrivilege(Iterables.get(response.getGlobalPrivileges(), 1).getPrivileges().iterator().next(), "apps-*");

        assertThat(response.getIndicesPrivileges().size(), equalTo(7));
        assertThat(Iterables.get(response.getIndicesPrivileges(), 0).getIndices(), contains("test-1-*"));
        assertThat(Iterables.get(response.getIndicesPrivileges(), 0).getPrivileges(), contains("read"));
        assertThat(Iterables.get(response.getIndicesPrivileges(), 0).allowRestrictedIndices(), equalTo(false));
        assertThat(Iterables.get(response.getIndicesPrivileges(), 0).getFieldSecurity(), emptyIterable());
        assertThat(Iterables.get(response.getIndicesPrivileges(), 0).getQueries(), emptyIterable());

        final UserIndicesPrivileges test4Privilege = Iterables.get(response.getIndicesPrivileges(), 1);
        assertThat(test4Privilege.getIndices(), contains("test-4-*"));
        assertThat(test4Privilege.getPrivileges(), contains("read"));
        assertThat(test4Privilege.allowRestrictedIndices(), equalTo(true));
        assertThat(test4Privilege.getFieldSecurity(), iterableWithSize(1));
        final IndicesPrivileges.FieldSecurity test4FLS = test4Privilege.getFieldSecurity().iterator().next();
        assertThat(test4FLS.getGrantedFields(), contains("*"));
        assertThat(test4FLS.getDeniedFields(), contains("private-*"));
        assertThat(test4Privilege.getQueries(), emptyIterable());

        final UserIndicesPrivileges test2Privilege = Iterables.get(response.getIndicesPrivileges(), 3);
        assertThat(test2Privilege.getIndices(), contains("test-2-*"));
        assertThat(test2Privilege.getPrivileges(), contains("read"));
        assertThat(test2Privilege.allowRestrictedIndices(), equalTo(false));
        assertThat(test2Privilege.getFieldSecurity(), iterableWithSize(2));
        final Iterator<IndicesPrivileges.FieldSecurity> test2FLSIter = test2Privilege.getFieldSecurity().iterator();
        final IndicesPrivileges.FieldSecurity test2FLS1 = test2FLSIter.next();
        final IndicesPrivileges.FieldSecurity test2FLS2 = test2FLSIter.next();
        assertThat(test2FLS1.getGrantedFields(), contains("*"));
        assertThat(test2FLS1.getDeniedFields(), containsInAnyOrder("secret-*", "private-*"));
        assertThat(test2FLS2.getGrantedFields(), contains("apps-*"));
        assertThat(test2FLS2.getDeniedFields(), nullValue());
        assertThat(test2Privilege.getQueries(), iterableWithSize(2));
        final Iterator<String> test2QueryIter = test2Privilege.getQueries().iterator();
        assertThat(test2QueryIter.next(), equalTo("{\"term\":{\"test\":true}}"));
        assertThat(test2QueryIter.next(), equalTo("{\"term\":{\"apps\":true}}"));

        assertThat(Iterables.get(response.getIndicesPrivileges(), 6).getIndices(), contains("test-1-*", "test-9-*"));
        assertThat(Iterables.get(response.getIndicesPrivileges(), 6).getPrivileges(), contains("all"));
        assertThat(Iterables.get(response.getIndicesPrivileges(), 6).allowRestrictedIndices(), equalTo(true));
        assertThat(Iterables.get(response.getIndicesPrivileges(), 6).getFieldSecurity(), emptyIterable());
        assertThat(Iterables.get(response.getIndicesPrivileges(), 6).getQueries(), emptyIterable());

        assertThat(response.getApplicationPrivileges().size(), equalTo(3));
        assertThat(Iterables.get(response.getApplicationPrivileges(), 1).getApplication(), equalTo("test-app"));
        assertThat(Iterables.get(response.getApplicationPrivileges(), 1).getPrivileges(), contains("read"));
        assertThat(Iterables.get(response.getApplicationPrivileges(), 1).getResources(), containsInAnyOrder("object/1", "object/2"));

        assertThat(response.getRunAsPrivilege(), contains("app-*", "test-*"));
    }

    private void assertManageApplicationsPrivilege(GlobalOperationPrivilege privilege, String... applications) {
        assertThat(privilege.getCategory(), equalTo("application"));
        assertThat(privilege.getOperation(), equalTo("manage"));
        assertThat(privilege.getRaw().keySet(), contains("applications"));
        assertThat(privilege.getRaw().get("applications"), instanceOf(List.class));
        assertThat((List<?>) privilege.getRaw().get("applications"), contains((Object[]) applications));
    }
}
