/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.expression.Expression.TypeResolution;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public abstract class Expressions {

    public static List<NamedExpression> asNamed(List<Expression> exp) {
        return exp.stream()
                .map(NamedExpression.class::cast)
                .collect(toList());
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

    public static boolean anyMatchInList(List<? extends Expression> exps, Predicate<? super Expression> predicate) {
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

    public static Attribute attribute(Expression e) {
        return e instanceof NamedExpression ? ((NamedExpression) e).toAttribute() : null;
    }
    
    public static TypeResolution typeMustBe(Expression e, Predicate<Expression> predicate, String message) {
        return predicate.test(e) ? TypeResolution.TYPE_RESOLVED : new TypeResolution(message);
    }
    
    public static TypeResolution typeMustBeNumeric(Expression e) {
        return e.dataType().isNumeric()? TypeResolution.TYPE_RESOLVED : new TypeResolution( 
                "Argument required to be numeric ('%s' of type '%s')", Expressions.name(e), e.dataType().esName());
    }

}