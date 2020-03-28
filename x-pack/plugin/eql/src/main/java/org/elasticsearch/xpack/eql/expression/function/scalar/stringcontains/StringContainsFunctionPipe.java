/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.stringcontains;

import org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class StringContainsFunctionPipe extends Pipe {

    private final Pipe haystack, needle, caseSensitive;

    public StringContainsFunctionPipe(Source source, Expression expression, Pipe haystack, Pipe needle, Pipe caseSensitive) {
        super(source, expression, Arrays.asList(haystack, needle));
        this.haystack = haystack;
        this.needle = needle;
        this.caseSensitive = caseSensitive;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        if (newChildren.size() != 3) {
            throw new IllegalArgumentException("expected [3] children but received [" + newChildren.size() + "]");
        }
        return replaceChildren(newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newHaystack = haystack.resolveAttributes(resolver);
        Pipe newNeedle = needle.resolveAttributes(resolver);
        Pipe newCaseSensitive = caseSensitive.resolveAttributes(resolver);
        if (newHaystack == haystack && newNeedle == needle && newCaseSensitive == caseSensitive) {
            return this;
        }
        return replaceChildren(newHaystack, newNeedle, newCaseSensitive);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return haystack.supportedByAggsOnlyQuery() && needle.supportedByAggsOnlyQuery() && caseSensitive.supportedByAggsOnlyQuery();
    }

    @Override
    public boolean resolved() {
        return haystack.resolved() && needle.resolved() && caseSensitive.resolved();
    }

    protected Pipe replaceChildren(Pipe haystack, Pipe needle, Pipe caseSensitive) {
        return new StringContainsFunctionPipe(source(), expression(), haystack, needle, caseSensitive);
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        haystack.collectFields(sourceBuilder);
        needle.collectFields(sourceBuilder);
        caseSensitive.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<StringContainsFunctionPipe> info() {
        return NodeInfo.create(this, StringContainsFunctionPipe::new, expression(), haystack, needle, caseSensitive);
    }

    @Override
    public StringContainsFunctionProcessor asProcessor() {
        return new StringContainsFunctionProcessor(haystack.asProcessor(), needle.asProcessor(), caseSensitive.asProcessor());
    }

    public Pipe haystack() {
        return haystack;
    }

    public Pipe needle() {
        return needle;
    }

    public Pipe caseSensitive() {
        return caseSensitive;
    }

    @Override
    public int hashCode() {
        return Objects.hash(source(), haystack(), needle(), caseSensitive());
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
        return Objects.equals(source(), other.source())
                && Objects.equals(haystack(), other.haystack())
                && Objects.equals(needle(), other.needle())
                && Objects.equals(caseSensitive(), other.caseSensitive());
    }
}
