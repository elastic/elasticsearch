/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.show;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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

            var constructors = def.clazz().getConstructors();
            StringBuilder sb = new StringBuilder(def.name());
            sb.append('(');
            if (constructors.length > 0) {
                var params = constructors[0].getParameters(); // no multiple c'tors supported
                for (int i = 1; i < params.length; i++) { // skipping 1st argument, the source
                    if (i > 1) {
                        sb.append(", ");
                    }
                    sb.append(params[i].getName());
                    if (List.class.isAssignableFrom(params[i].getType())) {
                        sb.append("...");
                    }
                }
            }
            sb.append(')');
            row.add(asBytesRefOrNull(sb.toString()));

            rows.add(row);
        }
        rows.sort(Comparator.comparing(x -> ((BytesRef) x.get(0))));
        return rows;
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
