/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class StringContainsFunctionPipe extends Pipe {

    private final Pipe string, substring;
    private final boolean caseInsensitive;

    public StringContainsFunctionPipe(Source source, Expression expression, Pipe string, Pipe substring, boolean caseInsensitive) {
        super(source, expression, Arrays.asList(string, substring));
        this.string = string;
        this.substring = substring;
        this.caseInsensitive = caseInsensitive;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        return replaceChildren(newChildren.get(0), newChildren.get(1));
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newString = string.resolveAttributes(resolver);
        Pipe newSubstring = substring.resolveAttributes(resolver);
        if (newString == string && newSubstring == substring) {
            return this;
        }
        return replaceChildren(newString, newSubstring);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return string.supportedByAggsOnlyQuery() && substring.supportedByAggsOnlyQuery();
    }

    @Override
    public boolean resolved() {
        return string.resolved() && substring.resolved();
    }

    protected StringContainsFunctionPipe replaceChildren(Pipe string, Pipe substring) {
        return new StringContainsFunctionPipe(source(), expression(), string, substring, caseInsensitive);
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        string.collectFields(sourceBuilder);
        substring.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<StringContainsFunctionPipe> info() {
        return NodeInfo.create(this, StringContainsFunctionPipe::new, expression(), string, substring, caseInsensitive);
    }

    @Override
    public StringContainsFunctionProcessor asProcessor() {
        return new StringContainsFunctionProcessor(string.asProcessor(), substring.asProcessor(), caseInsensitive);
    }

    public Pipe string() {
        return string;
    }

    public Pipe substring() {
        return substring;
    }

    protected boolean isCaseInsensitive() {
        return caseInsensitive;
    }

    @Override
    public int hashCode() {
        return Objects.hash(string(), substring());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        StringContainsFunctionPipe other = (StringContainsFunctionPipe) obj;
        return Objects.equals(string(), other.string())
                && Objects.equals(substring(), other.substring());
    }
}
