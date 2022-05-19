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
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class CIDRMatchFunctionPipe extends Pipe {

    private final Pipe input;
    private final List<Pipe> addresses;

    public CIDRMatchFunctionPipe(Source source, Expression expression, Pipe input, List<Pipe> addresses) {
        super(source, expression, CollectionUtils.combine(Collections.singletonList(input), addresses));
        this.input = input;
        this.addresses = addresses;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        return replaceChildren(newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newInput = input.resolveAttributes(resolver);
        boolean same = (newInput == input);

        ArrayList<Pipe> newAddresses = new ArrayList<Pipe>(addresses.size());
        for (Pipe address : addresses) {
            Pipe newAddress = address.resolveAttributes(resolver);
            newAddresses.add(newAddress);
            same = same && (address == newAddress);
        }
        return same ? this : replaceChildren(newInput, newAddresses);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        if (input.supportedByAggsOnlyQuery() == false) {
            return false;
        }
        for (Pipe address : addresses) {
            if (address.supportedByAggsOnlyQuery() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean resolved() {
        if (input.resolved() == false) {
            return false;
        }
        for (Pipe address : addresses) {
            if (address.resolved() == false) {
                return false;
            }
        }
        return true;
    }

    protected CIDRMatchFunctionPipe replaceChildren(Pipe newInput, List<Pipe> newAddresses) {
        return new CIDRMatchFunctionPipe(source(), expression(), newInput, newAddresses);
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        input.collectFields(sourceBuilder);
        for (Pipe address : addresses) {
            address.collectFields(sourceBuilder);
        }
    }

    @Override
    protected NodeInfo<CIDRMatchFunctionPipe> info() {
        return NodeInfo.create(this, CIDRMatchFunctionPipe::new, expression(), input, addresses);
    }

    @Override
    public CIDRMatchFunctionProcessor asProcessor() {
        ArrayList<Processor> processors = new ArrayList<>(addresses.size());
        for (Pipe address : addresses) {
            processors.add(address.asProcessor());
        }
        return new CIDRMatchFunctionProcessor(input.asProcessor(), processors);
    }

    public Pipe input() {
        return input;
    }

    public List<Pipe> addresses() {
        return addresses;
    }

    @Override
    public int hashCode() {
        return Objects.hash(input(), addresses());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        CIDRMatchFunctionPipe other = (CIDRMatchFunctionPipe) obj;
        return Objects.equals(input(), other.input()) && Objects.equals(addresses(), other.addresses());
    }
}
