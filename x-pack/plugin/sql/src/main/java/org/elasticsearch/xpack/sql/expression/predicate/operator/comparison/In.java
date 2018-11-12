/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.Foldables;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.CollectionUtils;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

public class In extends ScalarFunction {

    private final Expression value;
    private final List<Expression> list;

    public In(Location location, Expression value, List<Expression> list) {
        super(location, CollectionUtils.combine(list, value));
        this.value = value;
        this.list = new ArrayList<>(new LinkedHashSet<>(list));
    }

    @Override
    protected NodeInfo<In> info() {
        return NodeInfo.create(this, In::new, value, list);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() < 2) {
            throw new IllegalArgumentException("expected at least [2] children but received [" + newChildren.size() + "]");
        }
        return new In(location(), newChildren.get(newChildren.size() - 1), newChildren.subList(0, newChildren.size() - 1));
    }

    public Expression value() {
        return value;
    }

    public List<Expression> list() {
        return list;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children()) ||
            (Expressions.foldable(list) && list().stream().allMatch(e -> e.dataType() == DataType.NULL));
    }

    @Override
    public Boolean fold() {
        // Optimization for early return and Query folding to LocalExec
        if (value.dataType() == DataType.NULL ||
            list.size() == 1 && list.get(0).dataType() == DataType.NULL) {
            return null;
        }
        return InProcessor.apply(value.fold(), Foldables.valuesOf(list, value.dataType()));
    }

    @Override
    public String name() {
        StringJoiner sj = new StringJoiner(", ", " IN (", ")");
        list.forEach(e -> sj.add(Expressions.name(e)));
        return Expressions.name(value) + sj.toString();
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate leftScript = asScript(value);

        // fold & remove duplicates
        List<Object> values = new ArrayList<>(new LinkedHashSet<>(Foldables.valuesOf(list, value.dataType())));

        return new ScriptTemplate(
            formatTemplate(String.format(Locale.ROOT, "{sql}.in(%s, {})", leftScript.template())),
            paramsBuilder()
                .script(leftScript.params())
                .variable(values)
                .build(),
            dataType());
    }

    @Override
    protected Pipe makePipe() {
        return new InPipe(location(), this, children().stream().map(Expressions::pipe).collect(Collectors.toList()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, list);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        In other = (In) obj;
        return Objects.equals(value, other.value)
            && Objects.equals(list, other.list);
    }
}
