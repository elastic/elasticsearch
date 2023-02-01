/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.analyzer;

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.rule.ParameterizedRule;
import org.elasticsearch.xpack.ql.rule.Rule;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.InvalidMappedField;
import org.elasticsearch.xpack.ql.type.UnsupportedEsField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;

public final class AnalyzerRules {

    public static class AddMissingEqualsToBoolField extends AnalyzerRule<Filter> {

        @Override
        protected LogicalPlan rule(Filter filter) {
            if (filter.resolved() == false) {
                return filter;
            }
            // check the condition itself
            Expression condition = replaceRawBoolFieldWithEquals(filter.condition());
            // otherwise look for binary logic
            if (condition == filter.condition()) {
                condition = condition.transformUp(
                    BinaryLogic.class,
                    b -> b.replaceChildren(asList(replaceRawBoolFieldWithEquals(b.left()), replaceRawBoolFieldWithEquals(b.right())))
                );
            }

            if (condition != filter.condition()) {
                filter = filter.with(condition);
            }
            return filter;
        }

        private Expression replaceRawBoolFieldWithEquals(Expression e) {
            if (e instanceof FieldAttribute && e.dataType() == BOOLEAN) {
                e = new Equals(e.source(), e, Literal.of(e, Boolean.TRUE));
            }
            return e;
        }

        @Override
        protected boolean skipResolved() {
            return false;
        }
    }

    public abstract static class AnalyzerRule<SubPlan extends LogicalPlan> extends Rule<SubPlan, LogicalPlan> {

        // transformUp (post-order) - that is first children and then the node
        // but with a twist; only if the tree is not resolved or analyzed
        @Override
        public final LogicalPlan apply(LogicalPlan plan) {
            return plan.transformUp(typeToken(), t -> t.analyzed() || skipResolved() && t.resolved() ? t : rule(t));
        }

        protected abstract LogicalPlan rule(SubPlan plan);

        protected boolean skipResolved() {
            return true;
        }
    }

    public abstract static class ParameterizedAnalyzerRule<SubPlan extends LogicalPlan, P> extends ParameterizedRule<
        SubPlan,
        LogicalPlan,
        P> {

        // transformUp (post-order) - that is first children and then the node
        // but with a twist; only if the tree is not resolved or analyzed
        public final LogicalPlan apply(LogicalPlan plan, P context) {
            return plan.transformUp(typeToken(), t -> t.analyzed() || skipResolved() && t.resolved() ? t : rule(t, context));
        }

        protected abstract LogicalPlan rule(SubPlan plan, P context);

        protected boolean skipResolved() {
            return true;
        }
    }

    public abstract static class BaseAnalyzerRule extends AnalyzerRule<LogicalPlan> {

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            if (plan.childrenResolved() == false) {
                return plan;
            }
            return doRule(plan);
        }

        protected abstract LogicalPlan doRule(LogicalPlan plan);
    }

    public static Function resolveFunction(UnresolvedFunction uf, Configuration configuration, FunctionRegistry functionRegistry) {
        Function f = null;
        if (uf.analyzed()) {
            f = uf;
        } else if (uf.childrenResolved() == false) {
            f = uf;
        } else {
            String functionName = functionRegistry.resolveAlias(uf.name());
            if (functionRegistry.functionExists(functionName) == false) {
                f = uf.missing(functionName, functionRegistry.listFunctions());
            } else {
                FunctionDefinition def = functionRegistry.resolveFunction(functionName);
                f = uf.buildResolved(configuration, def);
            }
        }
        return f;
    }

    public static List<Attribute> maybeResolveAgainstList(
        UnresolvedAttribute u,
        Collection<Attribute> attrList,
        boolean allowCompound,
        boolean acceptPattern
    ) {
        List<Attribute> matches = new ArrayList<>();

        // first take into account the qualified version
        boolean qualified = u.qualifier() != null;

        var name = u.name();
        var isPattern = acceptPattern && Regex.isSimpleMatchPattern(name);
        BiFunction<String, String, Boolean> nameMatcher = isPattern ? Regex::simpleMatch : Objects::equals;

        for (Attribute attribute : attrList) {
            if (attribute.synthetic() == false) {
                boolean match = qualified ? Objects.equals(u.qualifiedName(), attribute.qualifiedName()) : // TODO: qualified w/ pattern?
                // if the field is unqualified
                // first check the names directly
                    (nameMatcher.apply(name, attribute.name())
                        // but also if the qualifier might not be quoted and if there's any ambiguity with nested fields
                        || nameMatcher.apply(name, attribute.qualifiedName()));
                if (match) {
                    matches.add(attribute);
                }
            }
        }

        if (matches.isEmpty()) {
            return matches;
        }

        // found exact match or multiple if pattern
        if (matches.size() == 1 || isPattern) {
            // only add the location if the match is univocal; b/c otherwise adding the location will overwrite any preexisting one
            matches.replaceAll(e -> handleSpecialFields(u, e.withLocation(u.source()), allowCompound));
            return matches;
        }

        // report ambiguity
        List<String> refs = matches.stream().sorted((a, b) -> {
            int lineDiff = a.sourceLocation().getLineNumber() - b.sourceLocation().getLineNumber();
            int colDiff = a.sourceLocation().getColumnNumber() - b.sourceLocation().getColumnNumber();
            return lineDiff != 0 ? lineDiff : (colDiff != 0 ? colDiff : a.qualifiedName().compareTo(b.qualifiedName()));
        })
            .map(
                a -> "line "
                    + a.sourceLocation().toString().substring(1)
                    + " ["
                    + (a.qualifier() != null ? "\"" + a.qualifier() + "\".\"" + a.name() + "\"" : a.name())
                    + "]"
            )
            .toList();

        return singletonList(
            u.withUnresolvedMessage(
                "Reference [" + u.qualifiedName() + "] is ambiguous (to disambiguate use quotes or qualifiers); " + "matches any of " + refs
            )
        );
    }

    private static Attribute handleSpecialFields(UnresolvedAttribute u, Attribute named, boolean allowCompound) {
        // if it's a object/compound type, keep it unresolved with a nice error message
        if (named instanceof FieldAttribute fa) {

            // incompatible mappings
            if (fa.field() instanceof InvalidMappedField) {
                named = u.withUnresolvedMessage(
                    "Cannot use field [" + fa.name() + "] due to ambiguities being " + ((InvalidMappedField) fa.field()).errorMessage()
                );
            }
            // unsupported types
            else if (DataTypes.isUnsupported(fa.dataType())) {
                UnsupportedEsField unsupportedField = (UnsupportedEsField) fa.field();
                if (unsupportedField.hasInherited()) {
                    named = u.withUnresolvedMessage(
                        "Cannot use field ["
                            + fa.name()
                            + "] with unsupported type ["
                            + unsupportedField.getOriginalType()
                            + "] in hierarchy (field ["
                            + unsupportedField.getInherited()
                            + "])"
                    );
                } else {
                    named = u.withUnresolvedMessage(
                        "Cannot use field [" + fa.name() + "] with unsupported type [" + unsupportedField.getOriginalType() + "]"
                    );
                }
            }
            // compound fields
            else if (allowCompound == false && DataTypes.isPrimitive(fa.dataType()) == false) {
                named = u.withUnresolvedMessage(
                    "Cannot use field [" + fa.name() + "] type [" + fa.dataType().typeName() + "] only its subfields"
                );
            }
        }
        return named;
    }
}
