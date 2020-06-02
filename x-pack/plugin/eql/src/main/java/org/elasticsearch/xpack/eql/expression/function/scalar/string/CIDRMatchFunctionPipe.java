/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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

    private final Pipe source;
    private final List<Pipe> addresses;

    public CIDRMatchFunctionPipe(Source source, Expression expression, Pipe src, List<Pipe> addresses) {
        super(source, expression, CollectionUtils.combine(Collections.singletonList(src), addresses));
        this.source = src;
        this.addresses = addresses;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        if (newChildren.size() < 2) {
            throw new IllegalArgumentException("expected at least [2] children but received [" + newChildren.size() + "]");
        }
        return replaceChildren(newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newSource = source.resolveAttributes(resolver);
        boolean same = (newSource == source);

        ArrayList<Pipe> newAddresses = new ArrayList<Pipe>(addresses.size());
        for (Pipe address : addresses) {
            Pipe newAddress = address.resolveAttributes(resolver);
            newAddresses.add(newAddress);
            same = same && (address == newAddress);
        }
        if (same) {
            return this;
        }
        return replaceChildren(newSource, newAddresses);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        if (source.supportedByAggsOnlyQuery() == false) {
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
        if (source.resolved() == false) {
            return false;
        }
        for (Pipe address : addresses) {
            if (address.resolved() == false) {
                return false;
            }
        }
        return true;
    }

    protected Pipe replaceChildren(Pipe newSource, List<Pipe> newAddresses) {
        return new CIDRMatchFunctionPipe(source(), expression(), newSource, newAddresses);
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        source.collectFields(sourceBuilder);
        for (Pipe address : addresses) {
            address.collectFields(sourceBuilder);
        }
    }

    @Override
    protected NodeInfo<CIDRMatchFunctionPipe> info() {
        return NodeInfo.create(this, CIDRMatchFunctionPipe::new, expression(), source, addresses);
    }

    @Override
    public CIDRMatchFunctionProcessor asProcessor() {
        ArrayList<Processor> processors = new ArrayList<>(addresses.size());
        for (Pipe address: addresses) {
            processors.add(address.asProcessor());
        }
        return new CIDRMatchFunctionProcessor(source.asProcessor(), processors);
    }

    public Pipe src() {
        return source;
    }

    public List<Pipe> addresses() {
        return addresses;
    }

    @Override
    public int hashCode() {
        return Objects.hash(source(), addresses());
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
        return Objects.equals(source(), other.source())
                && Objects.equals(addresses(), other.addresses());
    }
}
