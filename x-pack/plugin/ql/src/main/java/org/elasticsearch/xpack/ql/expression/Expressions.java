/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.AttributeInput;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.ConstantInput;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;

public final class Expressions {

    private Expressions() {}

    public static NamedExpression wrapAsNamed(Expression exp) {
        return exp instanceof NamedExpression ? (NamedExpression) exp : new Alias(exp.source(), exp.sourceText(), exp);
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
            return AttributeMap.emptyAttributeMap();
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

    public static List<Object> fold(List<? extends Expression> exps) {
        List<Object> folded = new ArrayList<>(exps.size());
        for (Expression exp : exps) {
            folded.add(exp.fold());
        }

        return folded;
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
        return e instanceof NamedExpression ? ((NamedExpression) e).name() : e.sourceText();
    }

    public static boolean isNull(Expression e) {
        return e.dataType() == DataTypes.NULL || (e.foldable() && e.fold() == null);
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
            if (ne instanceof Alias) {
                aliases.add(new Tuple<>(ne.toAttribute(), ((Alias) ne).child()));
            }
        }
        return aliases;
    }

    public static boolean hasReferenceAttribute(Collection<Attribute> output) {
        for (Attribute attribute : output) {
            if (attribute instanceof ReferenceAttribute) {
                return true;
            }
        }
        return false;
    }

    public static List<Attribute> onlyPrimitiveFieldAttributes(Collection<Attribute> attributes) {
        List<Attribute> filtered = new ArrayList<>();
        // add only primitives
        // but filter out multi fields (allow only the top-level value)
        Set<Attribute> seenMultiFields = new LinkedHashSet<>();

        for (Attribute a : attributes) {
            if (DataTypes.isUnsupported(a.dataType()) == false && DataTypes.isPrimitive(a.dataType())) {
                if (a instanceof FieldAttribute fa) {
                    // skip nested fields and seen multi-fields
                    if (fa.isNested() == false && seenMultiFields.contains(fa.parent()) == false) {
                        filtered.add(a);
                        seenMultiFields.add(a);
                    }
                } else {
                    filtered.add(a);
                }
            }
        }

        return filtered;
    }

    public static Pipe pipe(Expression e) {
        if (e.foldable()) {
            return new ConstantInput(e.source(), e, e.fold());
        }
        if (e instanceof NamedExpression) {
            return new AttributeInput(e.source(), e, ((NamedExpression) e).toAttribute());
        }
        if (e instanceof Function) {
            return ((Function) e).asPipe();
        }
        throw new QlIllegalArgumentException("Cannot create pipe for {}", e);
    }

    public static List<Pipe> pipe(List<Expression> expressions) {
        List<Pipe> pipes = new ArrayList<>(expressions.size());
        for (Expression e : expressions) {
            pipes.add(pipe(e));
        }
        return pipes;
    }

    public static String id(Expression e) {
        return Integer.toHexString(e.hashCode());
    }
}
