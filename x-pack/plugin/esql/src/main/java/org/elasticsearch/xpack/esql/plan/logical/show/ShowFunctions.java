/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.show;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.esql.expression.function.Named;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;

public class ShowFunctions extends LeafPlan {

    private final List<Attribute> attributes;

    public ShowFunctions(Source source) {
        super(source);

        attributes = new ArrayList<>();
        for (var name : List.of("name", "synopsis")) {
            attributes.add(new ReferenceAttribute(Source.EMPTY, name, KEYWORD));
        }
    }

    @Override
    public List<Attribute> output() {
        return attributes;
    }

    public List<List<Object>> values(FunctionRegistry functionRegistry) {
        List<List<Object>> rows = new ArrayList<>();
        for (var def : functionRegistry.listFunctions(null)) {
            List<Object> row = new ArrayList<>();
            row.add(asBytesRefOrNull(def.name()));
            row.add(new BytesRef(def.name() + "(" + signature(def).stream().collect(Collectors.joining(", ")) + ")"));
            rows.add(row);
        }
        rows.sort(Comparator.comparing(x -> ((BytesRef) x.get(0))));
        return rows;
    }

    /**
     * Render the function's signature to a string.
     */
    public static List<String> signature(FunctionDefinition def) {
        var constructors = def.clazz().getConstructors();
        if (constructors.length == 0) {
            return List.of();
        }
        var params = constructors[0].getParameters(); // no multiple c'tors supported
        List<String> args = new ArrayList<>(params.length);
        for (int i = 1; i < params.length; i++) { // skipping 1st argument, the source
            if (Configuration.class.isAssignableFrom(params[i].getType()) == false) {
                Named namedAnn = params[i].getAnnotation(Named.class);
                String name = namedAnn == null ? params[i].getName() : namedAnn.value();

                if (List.class.isAssignableFrom(params[i].getType())) {
                    name = name + "...";
                }
                args.add(name);
            }
        }
        return args;
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
