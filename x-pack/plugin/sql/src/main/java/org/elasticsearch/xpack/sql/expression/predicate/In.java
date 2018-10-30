/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate;

import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.Params;
import org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptWeaver;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.Comparisons;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.InPipe;
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

import static java.lang.String.format;
import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

public class In extends NamedExpression implements ScriptWeaver {

    private final Expression value;
    private final List<Expression> list;
    private Attribute lazyAttribute;

    public In(Location location, Expression value, List<Expression> list) {
        super(location, null, CollectionUtils.combine(list, value), null);
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
        return Expressions.nullable(children());
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children()) ||
            (Expressions.foldable(list) && list().stream().allMatch(e -> e.dataType() == DataType.NULL));
    }

    @Override
    public Boolean fold() {
        if (value.dataType() == DataType.NULL) {
            return null;
        }
        if (list.size() == 1 && list.get(0).dataType() == DataType.NULL) {
            return false;
        }

        Object foldedLeftValue = value.fold();
        Boolean result = false;
        for (Expression rightValue : list) {
            Boolean compResult = Comparisons.eq(foldedLeftValue, rightValue.fold());
            if (compResult == null) {
                result = null;
            } else if (compResult) {
                return true;
            }
        }
        return result;
    }

    @Override
    public String name() {
        StringJoiner sj = new StringJoiner(", ", " IN(", ")");
        list.forEach(e -> sj.add(Expressions.name(e)));
        return Expressions.name(value) + sj.toString();
    }

    @Override
    public Attribute toAttribute() {
        if (lazyAttribute == null) {
            lazyAttribute = new ScalarFunctionAttribute(location(), name(), dataType(), null,
                false, id(), false, "IN", asScript(), null, asPipe());
        }
        return lazyAttribute;
    }

    @Override
    public ScriptTemplate asScript() {
        StringJoiner sj = new StringJoiner(" || ");
        ScriptTemplate leftScript = asScript(value);
        List<Params> rightParams = new ArrayList<>();
        String scriptPrefix = leftScript + "==";
        LinkedHashSet<Object> values = list.stream().map(Expression::fold).collect(Collectors.toCollection(LinkedHashSet::new));
        for (Object valueFromList : values) {
            // if checked against null => false
            if (valueFromList != null) {
                if (valueFromList instanceof Expression) {
                    ScriptTemplate rightScript = asScript((Expression) valueFromList);
                    sj.add(scriptPrefix + rightScript.template());
                    rightParams.add(rightScript.params());
                } else {
                    if (valueFromList instanceof String) {
                        sj.add(scriptPrefix + '"' + valueFromList + '"');
                    } else {
                        sj.add(scriptPrefix + valueFromList.toString());
                    }
                }
            }
        }

        ParamsBuilder paramsBuilder = paramsBuilder().script(leftScript.params());
        for (Params p : rightParams) {
            paramsBuilder = paramsBuilder.script(p);
        }

        return new ScriptTemplate(format(Locale.ROOT, "%s", sj.toString()), paramsBuilder.build(), dataType());
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
