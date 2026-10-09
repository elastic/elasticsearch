/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ToTextSerializationTests extends AbstractExpressionSerializationTests<ToText> {

    @Override
    protected ToText createTestInstance() {
        return new ToText(randomSource(), randomChild(), randomOptions());
    }

    @Override
    protected ToText mutateInstance(ToText instance) throws IOException {
        Expression field = instance.field();
        Expression options = instance.options();
        if (randomBoolean()) {
            field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        } else {
            options = randomValueOtherThan(options, ToTextSerializationTests::randomOptions);
        }
        return new ToText(instance.source(), field, options);
    }

    private static Expression randomOptions() {
        return randomBoolean() ? null : options(randomFrom("standard", "whitespace", "keyword", "stop"));
    }

    private static MapExpression options(String analyzer) {
        return new MapExpression(Source.EMPTY, List.of(Literal.keyword(Source.EMPTY, "analyzer"), Literal.keyword(Source.EMPTY, analyzer)));
    }

    // TO_TEXT declaring different values analyzers must never be considered equal.
    public void testOptionsAffectEqualsAndHashCode() {
        Source source = randomSource();
        Expression field = randomChild();
        ToText bare = new ToText(source, field, null);
        ToText whitespace = new ToText(source, field, options("whitespace"));
        ToText stop = new ToText(source, field, options("stop"));
        assertThat(whitespace, not(equalTo(bare)));
        assertThat(bare, not(equalTo(whitespace)));
        assertThat(whitespace, not(equalTo(stop)));
        assertThat(whitespace, equalTo(new ToText(source, field, options("whitespace"))));
        assertThat(whitespace.hashCode(), equalTo(new ToText(source, field, options("whitespace")).hashCode()));
    }

    // Analyzer.java and UnionTypeEsField call replaceChildren(singletonList(otherField)) on convert functions;
    // that must not drop the declared analyzer.
    public void testReplaceChildrenPreservesOptions() {
        ToText totext = new ToText(randomSource(), randomChild(), options("whitespace"));
        Expression newField = randomValueOtherThan(totext.field(), AbstractExpressionSerializationTests::randomChild);
        ToText replaced = (ToText) totext.replaceChildren(List.of(newField));
        assertThat(replaced.field(), equalTo(newField));
        assertThat(replaced.options(), equalTo(totext.options()));
    }

    public void testSerializationPreservesOptions() throws IOException {
        ToText totext = new ToText(randomSource(), randomChild(), options("whitespace"));
        ToText copy = copyInstance(totext);
        assertThat(copy.options(), not(nullValue()));
        assertThat(copy, equalTo(totext));
    }

    public void testBackcompatSerializationWithoutOptions() throws IOException {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(ToText.ESQL_TO_TEXT_VALUES_ANALYZER);
        ToText totext = new ToText(randomSource(), randomChildSupportedOn(oldVersion), null);
        ToText copy = copyInstance(totext, oldVersion);
        assertThat(copy.options(), nullValue());
        assertThat(copy, equalTo(totext));
    }

    public void testBackcompatSerializationRejectsOptions() {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(ToText.ESQL_TO_TEXT_VALUES_ANALYZER);
        ToText totext = new ToText(randomSource(), randomChildSupportedOn(oldVersion), options("whitespace"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> copyInstance(totext, oldVersion));
        assertThat(
            e.getMessage(),
            containsString(
                "with options is not supported in peer node's version ["
                    + oldVersion
                    + "]. Upgrade to version ["
                    + ToText.ESQL_TO_TEXT_VALUES_ANALYZER
                    + "] or newer."
            )
        );
    }
}
