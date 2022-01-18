/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.security.support.expressiondsl.fields.FieldRoleMapperExpression;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;

public class ExpressionRoleMappingTests extends ESTestCase {

    public void testExpressionRoleMappingParser() throws IOException {
        final String json = """
            {
               "enabled" : true,
               "roles" : [
                 "superuser"
               ],
               "rules" : {
                 "field" : {
                   "realm.name" : "kerb1"
                 }
               },
               "metadata" : { }
             }""";
        final ExpressionRoleMapping expressionRoleMapping = ExpressionRoleMapping.PARSER.parse(
            XContentType.JSON.xContent()
                .createParser(new NamedXContentRegistry(Collections.emptyList()), DeprecationHandler.IGNORE_DEPRECATIONS, json),
            "example-role-mapping"
        );
        final ExpressionRoleMapping expectedRoleMapping = new ExpressionRoleMapping(
            "example-role-mapping",
            FieldRoleMapperExpression.ofKeyValues("realm.name", "kerb1"),
            singletonList("superuser"),
            Collections.emptyList(),
            null,
            true
        );
        assertThat(expressionRoleMapping, equalTo(expectedRoleMapping));
    }

    public void testEqualsHashCode() {
        final ExpressionRoleMapping expressionRoleMapping = new ExpressionRoleMapping(
            "kerberosmapping",
            FieldRoleMapperExpression.ofKeyValues("realm.name", "kerb1"),
            singletonList("superuser"),
            Collections.emptyList(),
            null,
            true
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            expressionRoleMapping,
            original -> new ExpressionRoleMapping(
                original.getName(),
                original.getExpression(),
                original.getRoles(),
                original.getRoleTemplates(),
                original.getMetadata(),
                original.isEnabled()
            ),
            ExpressionRoleMappingTests::mutateTestItem
        );
    }

    private static ExpressionRoleMapping mutateTestItem(ExpressionRoleMapping original) throws IOException {
        return switch (randomIntBetween(0, 5)) {
            case 0 -> new ExpressionRoleMapping(
                "namechanged",
                FieldRoleMapperExpression.ofKeyValues("realm.name", "kerb1"),
                singletonList("superuser"),
                Collections.emptyList(),
                null,
                true
            );
            case 1 -> new ExpressionRoleMapping(
                "kerberosmapping",
                FieldRoleMapperExpression.ofKeyValues("changed", "changed"),
                singletonList("superuser"),
                Collections.emptyList(),
                null,
                true
            );
            case 2 -> new ExpressionRoleMapping(
                "kerberosmapping",
                FieldRoleMapperExpression.ofKeyValues("realm.name", "kerb1"),
                singletonList("changed"),
                Collections.emptyList(),
                null,
                true
            );
            case 3 -> new ExpressionRoleMapping(
                "kerberosmapping",
                FieldRoleMapperExpression.ofKeyValues("realm.name", "kerb1"),
                singletonList("superuser"),
                Collections.emptyList(),
                Map.of("a", "b"),
                true
            );
            case 4 -> new ExpressionRoleMapping(
                "kerberosmapping",
                FieldRoleMapperExpression.ofKeyValues("realm.name", "kerb1"),
                Collections.emptyList(),
                singletonList(new TemplateRoleName(Collections.singletonMap("source", "superuser"), TemplateRoleName.Format.STRING)),
                null,
                true
            );
            case 5 -> new ExpressionRoleMapping(
                "kerberosmapping",
                FieldRoleMapperExpression.ofKeyValues("realm.name", "kerb1"),
                singletonList("superuser"),
                Collections.emptyList(),
                null,
                false
            );
            default -> throw new UnsupportedOperationException();
        };
    }
}
