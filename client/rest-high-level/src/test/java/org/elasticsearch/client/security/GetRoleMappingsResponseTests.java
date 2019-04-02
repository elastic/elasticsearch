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

import org.elasticsearch.client.security.support.expressiondsl.fields.FieldRoleMapperExpression;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class GetRoleMappingsResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        final String json = "{\n" + 
                " \"kerberosmapping\" : {\n" + 
                "   \"enabled\" : true,\n" + 
                "   \"roles\" : [\n" + 
                "     \"superuser\"\n" + 
                "   ],\n" + 
                "   \"rules\" : {\n" + 
                "     \"field\" : {\n" + 
                "       \"realm.name\" : \"kerb1\"\n" + 
                "     }\n" + 
                "   },\n" + 
                "   \"metadata\" : { }\n" + 
                " },\n" + 
                " \"ldapmapping\" : {\n" + 
                "   \"enabled\" : false,\n" + 
                "   \"roles\" : [\n" + 
                "     \"monitoring\"\n" + 
                "   ],\n" + 
                "   \"rules\" : {\n" + 
                "     \"field\" : {\n" + 
                "       \"groups\" : \"cn=ipausers,cn=groups,cn=accounts,dc=ipademo,dc=local\"\n" + 
                "     }\n" + 
                "   },\n" + 
                "   \"metadata\" : { }\n" + 
                " }\n" + 
                "}";
        final GetRoleMappingsResponse response = GetRoleMappingsResponse.fromXContent(XContentType.JSON.xContent().createParser(
                new NamedXContentRegistry(Collections.emptyList()), new DeprecationHandler() {
                    @Override
                    public void usedDeprecatedName(String usedName, String modernName) {
                    }

                    @Override
                    public void usedDeprecatedField(String usedName, String replacedWith) {
                    }
                }, json));
        final List<ExpressionRoleMapping> expectedRoleMappingsList = new ArrayList<>();
        expectedRoleMappingsList.add(new ExpressionRoleMapping("kerberosmapping", FieldRoleMapperExpression.ofKeyValues("realm.name",
                "kerb1"), Collections.singletonList("superuser"), Collections.emptyList(), null, true));
        expectedRoleMappingsList.add(new ExpressionRoleMapping("ldapmapping", FieldRoleMapperExpression.ofGroups(
                "cn=ipausers,cn=groups,cn=accounts,dc=ipademo,dc=local"), Collections.singletonList("monitoring"), Collections.emptyList(),
            null, false));
        final GetRoleMappingsResponse expectedResponse = new GetRoleMappingsResponse(expectedRoleMappingsList);
        assertThat(response, equalTo(expectedResponse));
    }

    public void testEqualsHashCode() {
        final List<ExpressionRoleMapping> roleMappingsList = new ArrayList<>();
        roleMappingsList.add(new ExpressionRoleMapping("kerberosmapping", FieldRoleMapperExpression.ofKeyValues("realm.name",
                "kerb1"), Collections.singletonList("superuser"), Collections.emptyList(), null, true));
        final GetRoleMappingsResponse response = new GetRoleMappingsResponse(roleMappingsList);
        assertNotNull(response);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(response, (original) -> {
            return new GetRoleMappingsResponse(original.getMappings());
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(response, (original) -> {
            return new GetRoleMappingsResponse(original.getMappings());
        }, GetRoleMappingsResponseTests::mutateTestItem);
    }

    private static GetRoleMappingsResponse mutateTestItem(GetRoleMappingsResponse original) {
        GetRoleMappingsResponse mutated = null;
        switch(randomIntBetween(0, 1)) {
        case 0:
            final List<ExpressionRoleMapping> roleMappingsList1 = new ArrayList<>();
            roleMappingsList1.add(new ExpressionRoleMapping("ldapmapping", FieldRoleMapperExpression.ofGroups(
                "cn=ipausers,cn=groups,cn=accounts,dc=ipademo,dc=local"), Collections.singletonList("monitoring"), Collections.emptyList(),
                null, false));
            mutated = new GetRoleMappingsResponse(roleMappingsList1);
            break;
        case 1:
            final List<ExpressionRoleMapping> roleMappingsList2 = new ArrayList<>();
            ExpressionRoleMapping originalRoleMapping = original.getMappings().get(0);
            roleMappingsList2.add(new ExpressionRoleMapping(originalRoleMapping.getName(),
                FieldRoleMapperExpression.ofGroups("cn=ipausers,cn=groups,cn=accounts,dc=ipademo,dc=local"), originalRoleMapping.getRoles(),
                Collections.emptyList(), originalRoleMapping.getMetadata(), !originalRoleMapping.isEnabled()));
            mutated = new GetRoleMappingsResponse(roleMappingsList2);
            break;
        }
        return mutated;
    }
}
