/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;

public final class Expressions {

    private Expressions() {}

    public static NamedExpression wrapAsNamed(Expression exp) {
        return exp instanceof NamedExpression ne ? ne : new Alias(exp.source(), exp.sourceText(), exp);
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

    public static boolean anyMatch(List<? extends Expression> exps, Predicate<? super Expression> predicate) {
        for (Expression exp : exps) {
            if (exp.anyMatch(predicate)) {
                return true;
            }
        }
        return false;
    }

    public static boolean match(List<? extends Expression> exps, Predicate<? super Expression> predicate) {
        for (Expression exp : exps) {
            if (predicate.test(exp)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return the logical AND of a list of {@code Nullability}
     * <pre>
     *  UNKNOWN AND TRUE/FALSE/UNKNOWN = UNKNOWN
     *  FALSE AND FALSE = FALSE
     *  TRUE AND FALSE/TRUE = TRUE
     * </pre>
     */
    public static Nullability nullable(List<? extends Expression> exps) {
        Nullability value = Nullability.FALSE;
        for (Expression exp : exps) {
            switch (exp.nullable()) {
                case UNKNOWN:
                    return Nullability.UNKNOWN;
                case TRUE:
                    value = Nullability.TRUE;
                    break;
                default:
                    // not nullable
                    break;
            }
        }
        return value;
    }

    public static List<Expression> canonicalize(List<? extends Expression> exps) {
        List<Expression> canonical = new ArrayList<>(exps.size());
        for (Expression exp : exps) {
            canonical.add(exp.canonical());
        }
        return canonical;
    }

    public static boolean foldable(List<? extends Expression> exps) {
        for (Expression exp : exps) {
            if (exp.foldable() == false) {
                return false;
            }
        }
        return true;
    }

    public static List<Object> fold(FoldContext ctx, List<? extends Expression> exps) {
        List<Object> folded = new ArrayList<>(exps.size());
        for (Expression exp : exps) {
            folded.add(exp.fold(ctx));
        }

        return folded;
    }

    public static AttributeSet references(List<? extends Expression> exps) {
        return AttributeSet.of(exps, Expression::references);
    }

    public static String name(Expression e) {
        return e instanceof NamedExpression ne ? ne.name() : e.sourceText();
    }

    /**
     * Is this {@linkplain Expression} <strong>guaranteed</strong> to have
     * only the {@code null} value. {@linkplain Expression}s that
     * {@link Expression#fold} to {@code null} <strong>may</strong>
     * return {@code false} here, but should <strong>eventually</strong> be folded
     * into a {@link Literal} containing {@code null} which will return
     * {@code true} from here.
     */
    public static boolean isGuaranteedNull(Expression e) {
        return e.dataType() == DataType.NULL || (e instanceof Literal lit && lit.value() == null);
    }

    public static List<String> names(Collection<? extends Expression> e) {
        List<String> names = new ArrayList<>(e.size());
        for (Expression ex : e) {
            names.add(name(ex));
        }

        return names;
    }

    public static Attribute attribute(Expression e) {
        if (e instanceof NamedExpression ne) {
            return ne.toAttribute();
        }
        return null;
    }

    public static boolean isPresent(NamedExpression e) {
        return e instanceof EmptyAttribute == false;
    }

    public static boolean equalsAsAttribute(Expression left, Expression right) {
        if (left.semanticEquals(right) == false) {
            Attribute l = attribute(left);
            return (l != null && l.semanticEquals(attribute(right)));
        }
        return true;
    }

    public static List<Tuple<Attribute, Expression>> aliases(List<? extends NamedExpression> named) {
        // an alias of same name and data type can be reused (by mistake): need to use a list to collect all refs (and later report them)
        List<Tuple<Attribute, Expression>> aliases = new ArrayList<>();
        for (NamedExpression ne : named) {
            if (ne instanceof Alias as) {
                aliases.add(new Tuple<>(ne.toAttribute(), as.child()));
            }
        }
        return aliases;
    }

    public static String id(Expression e) {
        return Integer.toHexString(e.hashCode());
    }
}
