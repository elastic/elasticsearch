/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.mapper.expressiondsl;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import com.carrotsearch.randomizedtesting.WriterOutputStream;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.watcher.support.xcontent.XContentSource;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.iterableWithSize;

public class ExpressionParserTests extends ESTestCase {

    public void testParseSimpleFieldExpression() throws Exception {
        String json = "{ \"field\": { \"username\" : \"*@shield.gov\" } }";
        FieldExpression field = checkExpressionType(parse(json), FieldExpression.class);
        assertThat(field.getField(), equalTo("username"));
        assertThat(field.getValues(), iterableWithSize(1));
        final Predicate<Object> predicate = field.getValues().get(0);
        assertThat(predicate.test("bob@shield.gov"), equalTo(true));
        assertThat(predicate.test("bob@example.net"), equalTo(false));
        assertThat(json(field), equalTo(json.replaceAll("\\s", "")));
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

        assertThat(expr, instanceOf(AnyExpression.class));
        AnyExpression any = (AnyExpression) expr;

        assertThat(any.getElements(), iterableWithSize(2));

        final FieldExpression fieldShield = checkExpressionType(any.getElements().get(0),
                FieldExpression.class);
        assertThat(fieldShield.getField(), equalTo("username"));
        assertThat(fieldShield.getValues(), iterableWithSize(1));
        final Predicate<Object> predicateShield = fieldShield.getValues().get(0);
        assertThat(predicateShield.test("fury@shield.gov"), equalTo(true));
        assertThat(predicateShield.test("fury@shield.net"), equalTo(false));

        final AllExpression all = checkExpressionType(any.getElements().get(1),
                AllExpression.class);
        assertThat(all.getElements(), iterableWithSize(3));

        final FieldExpression fieldAvengers = checkExpressionType(all.getElements().get(0),
                FieldExpression.class);
        assertThat(fieldAvengers.getField(), equalTo("username"));
        assertThat(fieldAvengers.getValues(), iterableWithSize(1));
        final Predicate<Object> predicateAvengers = fieldAvengers.getValues().get(0);
        assertThat(predicateAvengers.test("stark@avengers.net"), equalTo(true));
        assertThat(predicateAvengers.test("romanov@avengers.org"), equalTo(true));
        assertThat(predicateAvengers.test("fury@shield.gov"), equalTo(false));

        final FieldExpression fieldGroupsAdmin = checkExpressionType(all.getElements().get(1),
                FieldExpression.class);
        assertThat(fieldGroupsAdmin.getField(), equalTo("groups"));
        assertThat(fieldGroupsAdmin.getValues(), iterableWithSize(2));
        assertThat(fieldGroupsAdmin.getValues().get(0).test("admin"), equalTo(true));
        assertThat(fieldGroupsAdmin.getValues().get(0).test("foo"), equalTo(false));
        assertThat(fieldGroupsAdmin.getValues().get(1).test("operators"), equalTo(true));
        assertThat(fieldGroupsAdmin.getValues().get(1).test("foo"), equalTo(false));

        final ExceptExpression except = checkExpressionType(all.getElements().get(2),
                ExceptExpression.class);
        final FieldExpression fieldDisavowed = checkExpressionType(except.getInnerExpression(),
                FieldExpression.class);
        assertThat(fieldDisavowed.getField(), equalTo("groups"));
        assertThat(fieldDisavowed.getValues(), iterableWithSize(1));
        assertThat(fieldDisavowed.getValues().get(0).test("disavowed"), equalTo(true));
        assertThat(fieldDisavowed.getValues().get(0).test("_disavowed_"), equalTo(false));

        Map<String, Object> hawkeye = new HashMap<>();
        hawkeye.put("username", "hawkeye@avengers.org");
        hawkeye.put("groups", Arrays.asList("operators"));
        assertThat(expr.match(hawkeye), equalTo(true));

        Map<String, Object> captain = new HashMap<>();
        captain.put("username", "america@avengers.net");
        assertThat(expr.match(captain), equalTo(false));

        Map<String, Object> warmachine = new HashMap<>();
        warmachine.put("username", "warmachine@avengers.net");
        warmachine.put("groups", Arrays.asList("admin", "disavowed"));
        assertThat(expr.match(warmachine), equalTo(false));

        Map<String, Object> fury = new HashMap<>();
        fury.put("username", "fury@shield.gov");
        fury.put("groups", Arrays.asList("classified", "directors"));
        assertThat(expr.asPredicate().test(fury), equalTo(true));

        assertThat(json(expr), equalTo(json.replaceAll("\\s", "")));
    }

    public void testWriteAndReadFromStream() throws IOException {
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
        final RoleMapperExpression exprSource = parse(json);
        final BytesStreamOutput out = new BytesStreamOutput();
        ExpressionParser.writeExpression(exprSource, out);

        final NamedWriteableRegistry registry = new NamedWriteableRegistry(Security.getNamedWriteables());
        final NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        final RoleMapperExpression exprResult = ExpressionParser.readExpression(input);
        assertThat(json(exprResult), equalTo(json.replaceAll("\\s", "")));
    }

    private <T> T checkExpressionType(RoleMapperExpression expr, Class<T> type) {
        assertThat(expr, instanceOf(type));
        return type.cast(expr);
    }

    private RoleMapperExpression parse(String json) throws IOException {
        return new ExpressionParser().parse("rules", new XContentSource(new BytesArray(json),
                XContentType.JSON));
    }

    private String json(RoleMapperExpression node) throws IOException {
        final StringWriter writer = new StringWriter();
        try (XContentBuilder builder = XContentFactory.jsonBuilder(new WriterOutputStream(writer))) {
            node.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        return writer.toString();
    }
}