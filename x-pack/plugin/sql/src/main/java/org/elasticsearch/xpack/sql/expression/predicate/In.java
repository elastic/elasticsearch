/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
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
    private Boolean lazyNullable, lazyFoldable;
    private List<Object> foldedList;
    private Attribute lazyAttribute;

    public In(Location location, Expression value, List<Expression> list) {
        super(location, null, CollectionUtils.combine(list, value), null);
        this.value = value;
        this.list = list;
    }

    @Override
    protected NodeInfo<In> info() {
        return NodeInfo.create(this, In::new, value(), list());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.isEmpty()) {
            throw new IllegalArgumentException("expected one or more children but received [" + newChildren.size() + "]");
        }
        return new In(location(), newChildren.get(newChildren.size() - 1), newChildren.subList(0, newChildren.size() - 1));
    }

    public Expression value() {
        return value;
    }

    public List<Expression> list() {
        return list;
    }

    public List<Object> foldedList() {
        return foldedList;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public boolean nullable() {
        if (lazyNullable == null) {
            lazyNullable = children().stream().anyMatch(Expression::nullable);
        }
        return lazyNullable;
    }

    @Override
    public boolean foldable() {
        if (lazyFoldable == null) {
            lazyFoldable = children().stream().allMatch(Expression::foldable) && value.foldable();
            foldedList = new ArrayList<>(list.size());
            list.forEach(e -> foldedList.add(e.foldable() ? e.fold() : e));
        }
        return lazyFoldable;
    }

    @Override
    public Object fold() {
        if (!foldable()) {
            throw new SqlIllegalArgumentException("Cannot fold if not everything in `IN` expression is foldable");
        }
        Object foldedLeftValue = value.fold();

        for (Object rightValue : foldedList) {
            Boolean compResult = Comparisons.eq(foldedLeftValue, rightValue);
            if (compResult != null && compResult) {
                return true;
            }
        }
        return false;
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
        for (Object e : foldedList) {
            if (e instanceof Expression) {
                ScriptTemplate rightScript = asScript((Expression) e);
                sj.add(scriptPrefix + rightScript.template());
                rightParams.add(rightScript.params());
            } else {
                if (e instanceof String) {
                    sj.add(scriptPrefix + '"' + e + '"');
                } else {
                    sj.add(scriptPrefix + e.toString());
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
        return new InPipe(location(), this, Expressions.pipe(value()), list.stream().map(Expressions::pipe).collect(Collectors.toList()));
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
