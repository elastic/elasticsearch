/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Expression.TypeResolution;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

public abstract class Expressions {

    public static List<NamedExpression> asNamed(List<? extends Expression> exp) {
        return exp.stream()
                .map(NamedExpression.class::cast)
                .collect(toList());
    }

    public static NamedExpression wrapAsNamed(Expression exp) {
        return exp instanceof NamedExpression ? (NamedExpression) exp : new Alias(exp.location(), exp.nodeName(), exp);
    }

    public static List<Attribute> asAttributes(List<? extends NamedExpression> named) {
        if (named.isEmpty()) {
            return emptyList();
        }
        List<Attribute> list = new ArrayList<>(named.size());
        for (NamedExpression exp : named) {
            list.add(exp.toAttribute());
        }
        return list;
    }

    public static AttributeMap<Expression> asAttributeMap(List<? extends NamedExpression> named) {
        if (named.isEmpty()) {
            return new AttributeMap<>(emptyMap());
        }

        AttributeMap<Expression> map = new AttributeMap<>();
        for (NamedExpression exp : named) {
            map.add(exp.toAttribute(), exp);
        }
        return map;
    }

    public static boolean anyMatch(List<? extends Expression> exps, Predicate<? super Expression> predicate) {
        for (Expression exp : exps) {
            if (exp.anyMatch(predicate)) {
                return true;
            }
        }
        return false;
    }

    public static boolean nullable(List<? extends Expression> exps) {
        for (Expression exp : exps) {
            if (!exp.nullable()) {
                return false;
            }
        }
        return true;
    }

    public static AttributeSet references(List<? extends Expression> exps) {
        if (exps.isEmpty()) {
            return AttributeSet.EMPTY;
        }

        AttributeSet set = new AttributeSet();
        for (Expression exp : exps) {
            set.addAll(exp.references());
        }
        return set;
    }

    public static String name(Expression e) {
        return e instanceof NamedExpression ? ((NamedExpression) e).name() : e.nodeName();
    }

    public static List<String> names(Collection<? extends Expression> e) {
        List<String> names = new ArrayList<>(e.size());
        for (Expression ex : e) {
            names.add(name(ex));
        }

        return names;
    }

    public static Attribute attribute(Expression e) {
        if (e instanceof NamedExpression) {
            return ((NamedExpression) e).toAttribute();
        }
        if (e != null && e.foldable()) {
            return Literal.of(e).toAttribute();
        }
        return null;
    }

    public static boolean equalsAsAttribute(Expression left, Expression right) {
        if (!left.semanticEquals(right)) {
            Attribute l = attribute(left);
            return (l != null && l.semanticEquals(attribute(right)));
        }
        return true;
    }

    public static Pipe pipe(Expression e) {
        if (e instanceof NamedExpression) {
            return ((NamedExpression) e).asPipe();
        }
        throw new SqlIllegalArgumentException("Cannot create pipe for {}", e);
    }

    public static TypeResolution typeMustBe(Expression e, Predicate<Expression> predicate, String message) {
        return predicate.test(e) ? TypeResolution.TYPE_RESOLVED : new TypeResolution(message);
    }

    public static TypeResolution typeMustBeNumeric(Expression e) {
        return e.dataType().isNumeric()? TypeResolution.TYPE_RESOLVED : new TypeResolution(
                "Argument required to be numeric ('" + Expressions.name(e) + "' of type '" + e.dataType().esType + "')");
    }
}
