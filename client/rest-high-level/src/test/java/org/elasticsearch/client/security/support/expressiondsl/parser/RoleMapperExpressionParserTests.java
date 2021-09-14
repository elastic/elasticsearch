/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security.support.expressiondsl.parser;

import org.elasticsearch.client.security.support.expressiondsl.RoleMapperExpression;
import org.elasticsearch.client.security.support.expressiondsl.expressions.CompositeRoleMapperExpression;
import org.elasticsearch.client.security.support.expressiondsl.fields.FieldRoleMapperExpression;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.iterableWithSize;

public class RoleMapperExpressionParserTests extends ESTestCase {

    public void testParseSimpleFieldExpression() throws Exception {
        String json = "{ \"field\": { \"username\" : [\"*@shield.gov\"] } }";
        FieldRoleMapperExpression field = checkExpressionType(parse(json), FieldRoleMapperExpression.class);
        assertThat(field.getField(), equalTo("username"));
        assertThat(field.getValues(), iterableWithSize(1));
        assertThat(field.getValues().get(0), equalTo("*@shield.gov"));

        assertThat(toJson(field), equalTo(json.replaceAll("\\s", "")));
    }

    public void testParseComplexExpression() throws Exception {
        String json = "{ \"any\": [" +
                "   { \"field\": { \"username\" : \"*@shield.gov\" } }, " +
                "   { \"all\": [" +
                "     { \"field\": { \"username\" : \"/.*\\\\@avengers\\\\.(net|org)/\" } }, " +
                "     { \"field\": { \"groups\" : [ \"admin\", \"operators\" ] } }, " +
                "     { \"except\":" +
                "       { \"field\": { \"groups\" : \"disavowed\" } }" +
                "     }" +
                "   ] }" +
                "] }";
        final RoleMapperExpression expr = parse(json);

        assertThat(expr, instanceOf(CompositeRoleMapperExpression.class));
        CompositeRoleMapperExpression any = (CompositeRoleMapperExpression) expr;

        assertThat(any.getElements(), iterableWithSize(2));

        final FieldRoleMapperExpression fieldShield = checkExpressionType(any.getElements().get(0),
                FieldRoleMapperExpression.class);
        assertThat(fieldShield.getField(), equalTo("username"));
        assertThat(fieldShield.getValues(), iterableWithSize(1));
        assertThat(fieldShield.getValues().get(0), equalTo("*@shield.gov"));

        final CompositeRoleMapperExpression all = checkExpressionType(any.getElements().get(1),
                CompositeRoleMapperExpression.class);
        assertThat(all.getElements(), iterableWithSize(3));

        final FieldRoleMapperExpression fieldAvengers = checkExpressionType(all.getElements().get(0),
                FieldRoleMapperExpression.class);
        assertThat(fieldAvengers.getField(), equalTo("username"));
        assertThat(fieldAvengers.getValues(), iterableWithSize(1));
        assertThat(fieldAvengers.getValues().get(0), equalTo("/.*\\@avengers\\.(net|org)/"));

        final FieldRoleMapperExpression fieldGroupsAdmin = checkExpressionType(all.getElements().get(1),
                FieldRoleMapperExpression.class);
        assertThat(fieldGroupsAdmin.getField(), equalTo("groups"));
        assertThat(fieldGroupsAdmin.getValues(), iterableWithSize(2));
        assertThat(fieldGroupsAdmin.getValues().get(0), equalTo("admin"));
        assertThat(fieldGroupsAdmin.getValues().get(1), equalTo("operators"));

        final CompositeRoleMapperExpression except = checkExpressionType(all.getElements().get(2),
                CompositeRoleMapperExpression.class);
        final FieldRoleMapperExpression fieldDisavowed = checkExpressionType(except.getElements().get(0),
                FieldRoleMapperExpression.class);
        assertThat(fieldDisavowed.getField(), equalTo("groups"));
        assertThat(fieldDisavowed.getValues(), iterableWithSize(1));
        assertThat(fieldDisavowed.getValues().get(0), equalTo("disavowed"));

    }

    private String toJson(final RoleMapperExpression expr) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        expr.toXContent(builder, ToXContent.EMPTY_PARAMS);
        final String output = Strings.toString(builder);
        return output;
    }

    private <T> T checkExpressionType(RoleMapperExpression expr, Class<T> type) {
        assertThat(expr, instanceOf(type));
        return type.cast(expr);
    }

    private RoleMapperExpression parse(String json) throws IOException {
        return new RoleMapperExpressionParser().parse("rules", XContentType.JSON.xContent().createParser(new NamedXContentRegistry(
                Collections.emptyList()), DeprecationHandler.IGNORE_DEPRECATIONS, json));
    }

}
