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

package org.elasticsearch.client.security.support.expressiondsl;

import org.elasticsearch.client.security.support.expressiondsl.expressions.AllRoleMapperExpression;
import org.elasticsearch.client.security.support.expressiondsl.expressions.AnyRoleMapperExpression;
import org.elasticsearch.client.security.support.expressiondsl.expressions.ExceptRoleMapperExpression;
import org.elasticsearch.client.security.support.expressiondsl.fields.FieldRoleMapperExpression;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Date;

import static org.hamcrest.Matchers.equalTo;

public class RoleMapperExpressionDslTests extends ESTestCase {

    public void testRoleMapperExpressionToXContentType() throws IOException {

        final RoleMapperExpression allExpression = AllRoleMapperExpression.builder()
                .addExpression(AnyRoleMapperExpression.builder()
                        .addExpression(FieldRoleMapperExpression.ofDN("*,ou=admin,dc=example,dc=com"))
                        .addExpression(FieldRoleMapperExpression.ofUsername("es-admin", "es-system"))
                        .build())
                .addExpression(FieldRoleMapperExpression.ofGroups("cn=people,dc=example,dc=com"))
                .addExpression(new ExceptRoleMapperExpression(FieldRoleMapperExpression.ofMetadata("metadata.terminated_date", new Date(
                        1537145401027L))))
                .build();

        final XContentBuilder builder = XContentFactory.jsonBuilder();
        allExpression.toXContent(builder, ToXContent.EMPTY_PARAMS);
        final String output = Strings.toString(builder);
        final String expected =
             "{"+
               "\"all\":["+
                 "{"+
                   "\"any\":["+
                     "{"+
                       "\"field\":{"+
                         "\"dn\":[\"*,ou=admin,dc=example,dc=com\"]"+
                       "}"+
                     "},"+
                     "{"+
                       "\"field\":{"+
                         "\"username\":["+
                           "\"es-admin\","+
                           "\"es-system\""+
                         "]"+
                       "}"+
                     "}"+
                   "]"+
                 "},"+
                 "{"+
                   "\"field\":{"+
                     "\"groups\":[\"cn=people,dc=example,dc=com\"]"+
                   "}"+
                 "},"+
                 "{"+
                   "\"except\":{"+
                     "\"field\":{"+
                       "\"metadata.terminated_date\":[\"2018-09-17T00:50:01.027Z\"]"+
                     "}"+
                   "}"+
                 "}"+
               "]"+
             "}";

        assertThat(output, equalTo(expected));
    }

    public void testFieldRoleMapperExpressionThrowsExceptionForMissingMetadataPrefix() {
        final IllegalArgumentException ile = expectThrows(IllegalArgumentException.class, () -> FieldRoleMapperExpression.ofMetadata(
                "terminated_date", new Date(1537145401027L)));
        assertThat(ile.getMessage(), equalTo("metadata key must have prefix 'metadata.'"));
    }
}
