/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.security.support.expressiondsl.fields.FieldRoleMapperExpression;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;

public class ExpressionRoleMappingTests extends ESTestCase {

    public void testExpressionRoleMappingParser() throws IOException {
        final String json =
                "{\n" +
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
                " }";
        final ExpressionRoleMapping expressionRoleMapping = ExpressionRoleMapping.PARSER.parse(XContentType.JSON.xContent().createParser(
                new NamedXContentRegistry(Collections.emptyList()), DeprecationHandler.IGNORE_DEPRECATIONS, json), "example-role-mapping");
        final ExpressionRoleMapping expectedRoleMapping = new ExpressionRoleMapping("example-role-mapping",
            FieldRoleMapperExpression.ofKeyValues("realm.name", "kerb1"),
            singletonList("superuser"), Collections.emptyList(),
            null, true);
        assertThat(expressionRoleMapping, equalTo(expectedRoleMapping));
    }

    public void testEqualsHashCode() {
        final ExpressionRoleMapping expressionRoleMapping = new ExpressionRoleMapping("kerberosmapping",
            FieldRoleMapperExpression.ofKeyValues("realm.name", "kerb1"),
            singletonList("superuser"), Collections.emptyList(),
            null, true);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(expressionRoleMapping, original ->
            new ExpressionRoleMapping(original.getName(), original.getExpression(), original.getRoles(), original.getRoleTemplates(),
                original.getMetadata(), original.isEnabled()), ExpressionRoleMappingTests::mutateTestItem);
    }

    private static ExpressionRoleMapping mutateTestItem(ExpressionRoleMapping original) throws IOException {
        ExpressionRoleMapping mutated = null;
        switch (randomIntBetween(0, 5)) {
        case 0:
            mutated = new ExpressionRoleMapping("namechanged", FieldRoleMapperExpression.ofKeyValues("realm.name", "kerb1"),
                singletonList("superuser"), Collections.emptyList(), null, true);
            break;
        case 1:
            mutated = new ExpressionRoleMapping("kerberosmapping", FieldRoleMapperExpression.ofKeyValues("changed", "changed"),
                singletonList("superuser"), Collections.emptyList(), null, true);
            break;
        case 2:
            mutated = new ExpressionRoleMapping("kerberosmapping", FieldRoleMapperExpression.ofKeyValues("realm.name", "kerb1"),
                singletonList("changed"), Collections.emptyList(), null, true);
            break;
        case 3:
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("a", "b");
            mutated = new ExpressionRoleMapping("kerberosmapping", FieldRoleMapperExpression.ofKeyValues("realm.name", "kerb1"),
                singletonList("superuser"), Collections.emptyList(), metadata, true);
            break;
        case 4:
            mutated = new ExpressionRoleMapping("kerberosmapping", FieldRoleMapperExpression.ofKeyValues("realm.name", "kerb1"),
                Collections.emptyList(),
                singletonList(new TemplateRoleName(Collections.singletonMap("source", "superuser"), TemplateRoleName.Format.STRING)),
                null, true);
            break;
        case 5:
            mutated = new ExpressionRoleMapping("kerberosmapping", FieldRoleMapperExpression.ofKeyValues("realm.name", "kerb1"),
                singletonList("superuser"), Collections.emptyList(), null, false);
            break;
        }
        return mutated;
    }
}
