/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl;

import com.carrotsearch.randomizedtesting.WriterOutputStream;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression.FieldValue;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;

public class ExpressionParserTests extends ESTestCase {

    public void testParseSimpleFieldExpression() throws Exception {
        String json = "{ \"field\": { \"username\" : \"*@shield.gov\" } }";
        FieldExpression field = checkExpressionType(parse(json), FieldExpression.class);
        assertThat(field.getField(), equalTo("username"));
        assertThat(field.getValues(), iterableWithSize(1));
        final FieldValue value = field.getValues().get(0);
        assertThat(value.getValue(), equalTo("*@shield.gov"));
        assertThat(value.getAutomaton(), notNullValue());
        assertThat(value.getAutomaton().run("bob@shield.gov"), equalTo(true));
        assertThat(value.getAutomaton().run("bob@example.net"), equalTo(false));
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
        final FieldValue valueShield = fieldShield.getValues().get(0);
        assertThat(valueShield.getValue(), equalTo("*@shield.gov"));
        assertThat(valueShield.getAutomaton(), notNullValue());
        assertThat(valueShield.getAutomaton().run("fury@shield.gov"), equalTo(true));
        assertThat(valueShield.getAutomaton().run("fury@shield.net"), equalTo(false));

        final AllExpression all = checkExpressionType(any.getElements().get(1),
                AllExpression.class);
        assertThat(all.getElements(), iterableWithSize(3));

        final FieldExpression fieldAvengers = checkExpressionType(all.getElements().get(0),
                FieldExpression.class);
        assertThat(fieldAvengers.getField(), equalTo("username"));
        assertThat(fieldAvengers.getValues(), iterableWithSize(1));
        final FieldValue valueAvengers = fieldAvengers.getValues().get(0);
        assertThat(valueAvengers.getAutomaton().run("stark@avengers.net"), equalTo(true));
        assertThat(valueAvengers.getAutomaton().run("romanov@avengers.org"), equalTo(true));
        assertThat(valueAvengers.getAutomaton().run("fury@shield.gov"), equalTo(false));

        final FieldExpression fieldGroupsAdmin = checkExpressionType(all.getElements().get(1),
                FieldExpression.class);
        assertThat(fieldGroupsAdmin.getField(), equalTo("groups"));
        assertThat(fieldGroupsAdmin.getValues(), iterableWithSize(2));
        assertThat(fieldGroupsAdmin.getValues().get(0).getValue(), equalTo("admin"));
        assertThat(fieldGroupsAdmin.getValues().get(1).getValue(), equalTo("operators"));

        final ExceptExpression except = checkExpressionType(all.getElements().get(2),
                ExceptExpression.class);
        final FieldExpression fieldDisavowed = checkExpressionType(except.getInnerExpression(),
                FieldExpression.class);
        assertThat(fieldDisavowed.getField(), equalTo("groups"));
        assertThat(fieldDisavowed.getValues(), iterableWithSize(1));
        assertThat(fieldDisavowed.getValues().get(0).getValue(), equalTo("disavowed"));

        ExpressionModel hawkeye = new ExpressionModel();
        hawkeye.defineField("username", "hawkeye@avengers.org");
        hawkeye.defineField("groups", Arrays.asList("operators"));
        assertThat(expr.match(hawkeye), equalTo(true));

        ExpressionModel captain = new ExpressionModel();
        captain.defineField("username", "america@avengers.net");
        assertThat(expr.match(captain), equalTo(false));

        ExpressionModel warmachine = new ExpressionModel();
        warmachine.defineField("username", "warmachine@avengers.net");
        warmachine.defineField("groups", Arrays.asList("admin", "disavowed"));
        assertThat(expr.match(warmachine), equalTo(false));

        ExpressionModel fury = new ExpressionModel();
        fury.defineField("username", "fury@shield.gov");
        fury.defineField("groups", Arrays.asList("classified", "directors"));
        assertThat(expr.asPredicate().test(fury), equalTo(true));

        assertThat(json(expr), equalTo(json.replaceAll("\\s", "")));
    }

    public void testWriteAndReadFromStream() throws Exception {
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

        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        final NamedWriteableRegistry registry = new NamedWriteableRegistry(new XPackClientPlugin(settings).getNamedWriteables());
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
