/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedPattern;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The rule handles fields that don't show up in the index mapping, but are used within the query. These fields can either be missing
 * entirely, or be present in the document, but not in the mapping (which can happen with non-dynamic mappings).
 * <p>
 * In the case of the former ones, the rule introducees {@code EVAL missing = NULL} commands (null-aliasing / null-Eval'ing).
 * <p>
 * In the case of the latter ones, it introduces field extractors in the source (where this supports it).
 * <p>
 * In both cases, the rule takes care of propagation of the aliases, where needed (i.e., through "artifical" projections introduced within
 * the analyzer itself; vs. the KEEP/RENAME/DROP-introduced projections). Note that this doesn't "boost" the visibility of such an
 * attribute: if, for instance, referencing a mapping-missing attribute occurs after a STATS that doesn't group by it, that attribute will
 * remain unresolved and fail the verification. The language remains semantically consistent.
 */
public class ResolveUnmapped extends AnalyzerRules.ParameterizedAnalyzerRule<LogicalPlan, AnalyzerContext> {

    @Override
    protected LogicalPlan rule(LogicalPlan plan, AnalyzerContext context) {
        return switch (context.unmappedResolution()) {
            case UnmappedResolution.FAIL -> plan;
            case UnmappedResolution.NULLIFY -> resolve(plan, false);
            case UnmappedResolution.LOAD -> resolve(plan, true);
        };
    }

    private static LogicalPlan resolve(LogicalPlan plan, boolean load) {
        if (plan.childrenResolved() == false) {
            return plan;
        }

        var unresolved = collectUnresolved(plan);
        if (unresolved.isEmpty()) {
            return plan;
        }

        var transformed = load ? load(plan, unresolved) : nullify(plan, unresolved);

        return transformed.equals(plan) ? plan : refreshUnresolved(transformed, unresolved);
    }

    /**
     * The method introduces {@code EVAL missing_field = NULL}-equivalent into the plan, on top of the source, for every attribute in
     * {@code unresolved}. It also "patches" the introduced attributes through the plan, where needed (like through Fork/UntionAll).
     */
    private static LogicalPlan nullify(LogicalPlan plan, List<UnresolvedAttribute> unresolved) {
        var nullAliases = nullAliases(unresolved);

        var transformed = plan.transformUp(
            n -> n instanceof UnaryPlan unary && unary.child() instanceof LeafPlan,
            p -> evalUnresolved((UnaryPlan) p, nullAliases)
        );
        transformed = transformed.transformUp(
            n -> n instanceof UnaryPlan == false && n instanceof LeafPlan == false,
            nAry -> evalUnresolved(nAry, nullAliases)
        );

        return transformed.transformUp(Fork.class, f -> patchFork(f, Expressions.asAttributes(nullAliases)));
    }

    private static LogicalPlan load(LogicalPlan plan, List<UnresolvedAttribute> unresolved) {
        throw new EsqlIllegalArgumentException("unmapped fields loading not yet supported");
    }

    // TODO: would an alternative to this be to drop the current Fork and have ResolveRefs#resolveFork re-resolve it. We might need
    // some plan delimiters/markers to make it unequivocal which nodes belong to "make Fork work" - like (Limit-Project[-Eval])s - and
    // which don't.
    private static Fork patchFork(Fork fork, List<Attribute> aliasAttributes) {
        // if no child outputs the attribute, don't patch it through at all.
        aliasAttributes.removeIf(a -> fork.children().stream().anyMatch(f -> descendantOutputsAttribute(f, a)) == false);
        if (aliasAttributes.isEmpty()) {
            return fork;
        }

        List<LogicalPlan> newChildren = new ArrayList<>(fork.children().size());
        for (var child : fork.children()) {
            Holder<Boolean> patched = new Holder<>(false);
            child = child.transformDown(
                // TODO add a suitable forEachDownMayReturnEarly equivalent
                n -> patched.get() == false && n instanceof Project, // process top Project only (Fork-injected)
                n -> {
                    patched.set(true);
                    return patchForkProject((Project) n, aliasAttributes);
                }
            );
            if (patched.get() == false) { // assert
                throw new EsqlIllegalArgumentException("Fork child misses a top projection");
            }
            newChildren.add(child);
        }

        List<Attribute> newAttributes = new ArrayList<>(fork.output().size() + aliasAttributes.size());
        newAttributes.addAll(fork.output());
        newAttributes.addAll(aliasAttributes);

        return fork.replaceSubPlansAndOutput(newChildren, newAttributes);
    }

    private static Project patchForkProject(Project project, List<Attribute> aliasAttributes) {
        // refresh the IDs for each UnionAll child (needed for correct resolution of convert functions; see collectConvertFunctions())
        aliasAttributes = aliasAttributes.stream().map(a -> a.withId(new NameId())).toList();

        List<NamedExpression> newProjections = new ArrayList<>(project.projections().size() + aliasAttributes.size());
        newProjections.addAll(project.projections());
        newProjections.addAll(aliasAttributes);
        project = project.withProjections(newProjections);

        // If Project's child doesn't output the attribute, introduce a null-Eval'ing. This is similar to what Fork-resolution does.
        List<Alias> nullAliases = new ArrayList<>(aliasAttributes.size());
        for (var attribute : aliasAttributes) {
            if (descendantOutputsAttribute(project, attribute) == false) {
                nullAliases.add(new Alias(attribute.source(), attribute.name(), Literal.NULL));
            }
        }
        return nullAliases.isEmpty() ? project : project.replaceChild(new Eval(project.source(), project.child(), nullAliases));
    }

    /**
     * Fork injects a {@code Limit - Project (- Eval)} top structure into its subtrees. Skip the top Limit (if present) and Project in
     * the {@code plan} and look at the output of the remaining fragment.
     * @return {@code true} if this fragment's output contains the {@code attribute}.
     */
    private static boolean descendantOutputsAttribute(LogicalPlan plan, Attribute attribute) {
        plan = plan instanceof Limit limit ? limit.child() : plan;
        if (plan instanceof Project project) {
            return project.child().outputSet().names().contains(attribute.name());
        }
        throw new EsqlIllegalArgumentException("unexpected node type [{}]", plan); // assert
    }

    private static LogicalPlan refreshUnresolved(LogicalPlan plan, List<UnresolvedAttribute> unresolved) {
        // These UAs haven't been resolved, so they're marked as unresolvable with a custom message. This needs to be removed for
        // ResolveRefs to attempt again to wire them to the newly added aliases.
        return plan.transformExpressionsOnlyUp(UnresolvedAttribute.class, ua -> {
            if (unresolved.contains(ua)) {
                unresolved.remove(ua);
                // Besides clearing the message, we need to refresh the nameId to avoid equality with the previous plan.
                // (A `new UnresolvedAttribute(ua.source(), ua.name())` would save an allocation, but is problematic with subtypes.)
                ua = (ua.withId(new NameId())).withUnresolvedMessage(null);
            }
            return ua;
        });
    }

    private static LogicalPlan evalUnresolved(LogicalPlan nAry, List<Alias> nullAliases) {
        List<LogicalPlan> newChildren = new ArrayList<>(nAry.children().size());
        boolean changed = false;
        for (var child : nAry.children()) {
            if (child instanceof LeafPlan source) {
                assertSourceType(source);
                child = new Eval(source.source(), source, nullAliases);
                changed = true;
            }
            newChildren.add(child);
        }
        return changed ? nAry.replaceChildren(newChildren) : nAry;
    }

    private static LogicalPlan evalUnresolved(UnaryPlan unaryAtopSource, List<Alias> nullAliases) {
        assertSourceType(unaryAtopSource.child());
        if (unaryAtopSource instanceof Eval eval && eval.resolved()) { // if this Eval isn't resolved, insert a new (resolved) one
            List<Alias> newAliases = new ArrayList<>(eval.fields().size() + nullAliases.size());
            List<Alias> pre = new ArrayList<>(nullAliases.size());
            List<Alias> post = new ArrayList<>(nullAliases.size());
            var outputNames = eval.outputSet().names();
            var evalRefNames = eval.references().names();
            for (Alias a : nullAliases) {
                if (outputNames.contains(a.name()) == false) {
                    var target = evalRefNames.contains(a.name()) ? pre : post;
                    target.add(a);
                }
            }
            if (pre.size() + post.size() == 0) {
                return eval;
            }
            newAliases.addAll(pre);
            newAliases.addAll(eval.fields());
            newAliases.addAll(post);
            return new Eval(eval.source(), eval.child(), newAliases);
        } else {
            return unaryAtopSource.replaceChild(new Eval(unaryAtopSource.source(), unaryAtopSource.child(), nullAliases));
        }
    }

    private static void assertSourceType(LogicalPlan source) {
        switch (source) {
            case EsRelation unused -> {
            }
            case Row unused -> {
            }
            case LocalRelation unused -> {
            }
            default -> throw new EsqlIllegalArgumentException("unexpected source type [{}]", source);
        }
    }

    private static List<Alias> nullAliases(List<UnresolvedAttribute> unresolved) {
        Map<String, Alias> aliasesMap = new LinkedHashMap<>(unresolved.size());
        for (var u : unresolved) {
            if (aliasesMap.containsKey(u.name()) == false) {
                aliasesMap.put(u.name(), new Alias(u.source(), u.name(), Literal.NULL));
            }
        }
        return new ArrayList<>(aliasesMap.values());
    }

    // collect all UAs in the node
    private static List<UnresolvedAttribute> collectUnresolved(LogicalPlan plan) {
        List<UnresolvedAttribute> unresolved = new ArrayList<>();
        plan.forEachExpression(UnresolvedAttribute.class, ua -> {
            if ((ua instanceof UnresolvedPattern || ua instanceof UnresolvedTimestamp) == false) {
                unresolved.add(ua);
            }
        });
        return unresolved;
    }
}
