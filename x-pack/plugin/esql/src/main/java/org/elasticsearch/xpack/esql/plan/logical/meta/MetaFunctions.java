/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.meta;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;

public class MetaFunctions extends LeafPlan {

    private final List<Attribute> attributes;

    public MetaFunctions(Source source) {
        super(source);

        attributes = new ArrayList<>();
        for (var name : List.of("name", "synopsis", "argNames", "argTypes", "argDescriptions", "returnType", "description")) {
            attributes.add(new ReferenceAttribute(Source.EMPTY, name, KEYWORD));
        }
        for (var name : List.of("optionalArgs", "variadic", "isAggregation")) {
            attributes.add(new ReferenceAttribute(Source.EMPTY, name, BOOLEAN));
        }
    }

    @Override
    public List<Attribute> output() {
        return attributes;
    }

    public List<List<Object>> values(FunctionRegistry functionRegistry) {
        List<List<Object>> rows = new ArrayList<>();
        for (var def : functionRegistry.listFunctions(null)) {
            EsqlFunctionRegistry.FunctionDescription signature = EsqlFunctionRegistry.description(def);
            List<Object> row = new ArrayList<>();
            row.add(asBytesRefOrNull(signature.name()));
            row.add(new BytesRef(signature.fullSignature()));
            row.add(collect(signature, EsqlFunctionRegistry.ArgSignature::name));
            row.add(collect(signature, EsqlFunctionRegistry.ArgSignature::type));
            row.add(collect(signature, EsqlFunctionRegistry.ArgSignature::description));
            row.add(withPipes(signature.returnType()));
            row.add(signature.description());
            row.add(collect(signature, EsqlFunctionRegistry.ArgSignature::optional));
            row.add(signature.variadic());
            row.add(signature.isAggregation());
            rows.add(row);
        }
        rows.sort(Comparator.comparing(x -> ((BytesRef) x.get(0))));
        return rows;
    }

    private Object collect(EsqlFunctionRegistry.FunctionDescription signature, Function<EsqlFunctionRegistry.ArgSignature, ?> x) {
        if (signature.args().size() == 0) {
            return null;
        }
        if (signature.args().size() == 1) {
            Object result = x.apply(signature.args().get(0));
            if (result instanceof String[] r) {
                return withPipes(r);
            }
            return result;
        }

        List<EsqlFunctionRegistry.ArgSignature> args = signature.args();
        List<?> result = signature.args().stream().map(x).collect(Collectors.toList());
        boolean withPipes = result.get(0) instanceof String[];
        if (result.isEmpty() == false) {
            List<Object> newResult = new ArrayList<>();
            for (int i = 0; i < result.size(); i++) {
                if (signature.variadic() && args.get(i).optional()) {
                    continue;
                }
                newResult.add(withPipes ? withPipes((String[]) result.get(i)) : result.get(i));
            }
            return newResult;
        }
        return result;
    }

    public static String withPipes(String[] items) {
        return Arrays.stream(items).collect(Collectors.joining("|"));
    }

    private static BytesRef asBytesRefOrNull(String string) {
        return Strings.hasText(string) ? new BytesRef(string) : null;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this);
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || obj != null && getClass() == obj.getClass();
    }
}
