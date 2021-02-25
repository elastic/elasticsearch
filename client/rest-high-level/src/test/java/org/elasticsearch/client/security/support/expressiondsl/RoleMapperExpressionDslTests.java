/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
