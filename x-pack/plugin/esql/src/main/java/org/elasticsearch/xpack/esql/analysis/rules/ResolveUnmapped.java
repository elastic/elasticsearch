/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedPattern;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.analysis.Analyzer.ResolveRefs.insistKeyword;
import static org.elasticsearch.xpack.esql.core.util.CollectionUtils.combine;

/**
 * The rule handles fields that don't show up in the index mapping, but are used within the query. These fields can either be missing
 * entirely, or be present in the document, but not in the mapping (which can happen with non-dynamic mappings). The handling strategy is
 * driven by the {@link AnalyzerContext#unmappedResolution()} setting.
 * <p>
 * In the case of the former ones, the rule introduces {@code EVAL missing = NULL} commands (null-aliasing / null-Eval'ing).
 * <p>
 * In the case of the latter ones, it introduces field extractors in the source (where this supports it).
 * <p>
 * In both cases, the rule takes care of propagation of the aliases, where needed (i.e., through "artificial" projections introduced within
 * the analyzer itself; vs. the KEEP/RENAME/DROP-introduced projections). Note that this doesn't "boost" the visibility of such an
 * attribute: if, for instance, referencing a mapping-missing attribute occurs after a STATS that doesn't group by it, that attribute will
 * remain unresolved and fail the verification. The language remains semantically consistent.
 */
public class ResolveUnmapped extends AnalyzerRules.ParameterizedAnalyzerRule<LogicalPlan, AnalyzerContext> {

    private static final Literal NULLIFIED = Literal.NULL;

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

        return transformed.equals(plan) ? plan : refreshPlan(transformed, unresolved);
    }

    /**
     * The method introduces {@code EVAL missing_field = NULL}-equivalent into the plan, on top of the source, for every attribute in
     * {@code unresolved}. It also "patches" the introduced attributes through the plan, where needed (like through Fork/UntionAll).
     */
    private static LogicalPlan nullify(LogicalPlan plan, List<UnresolvedAttribute> unresolved) {
        var nullAliases = nullAliases(unresolved);

        // insert an Eval on top of every LeafPlan, if there's a UnaryPlan atop it
        var transformed = plan.transformUp(
            n -> n instanceof UnaryPlan unary && unary.child() instanceof LeafPlan,
            p -> evalUnresolvedUnary((UnaryPlan) p, nullAliases)
        );
        // insert an Eval on top of those LeafPlan that are children of n-ary plans (could happen with UnionAll)
        transformed = transformed.transformUp(
            n -> n instanceof UnaryPlan == false && n instanceof LeafPlan == false,
            nAry -> evalUnresolvedNary(nAry, nullAliases)
        );

        return transformed.transformUp(Fork.class, f -> patchFork(f, Expressions.asAttributes(nullAliases)));
    }

    /**
     * This method introduces field extractors - via "insisted", {@link PotentiallyUnmappedKeywordEsField} wrapped in
     * {@link FieldAttribute} - for every attribute in {@code unresolved}, within the {@link EsRelation}s in the plan accessible from
     * the given {@code plan}.
     * <p>
     * It also "patches" the introduced attributes through the plan, where needed (like through Fork/UntionAll).
     */
    private static LogicalPlan load(LogicalPlan plan, List<UnresolvedAttribute> unresolved) {
        // TODO: this will need to be revisited for non-lookup joining or scenarios where we won't want extraction from specific sources
        var transformed = plan.transformUp(EsRelation.class, esr -> {
            if (esr.indexMode() == IndexMode.LOOKUP) {
                return esr;
            }
            List<FieldAttribute> fieldsToLoad = fieldsToLoad(unresolved, esr.outputSet().names());
            // there shouldn't be any duplicates, we can just merge the two lists
            return fieldsToLoad.isEmpty() ? esr : esr.withAttributes(combine(esr.output(), fieldsToLoad));
        });

        return transformed.transformUp(Fork.class, f -> patchFork(f, Expressions.asAttributes(fieldsToLoad(unresolved, Set.of()))));
    }

    private static List<FieldAttribute> fieldsToLoad(List<UnresolvedAttribute> unresolved, Set<String> exclude) {
        List<FieldAttribute> insisted = new ArrayList<>(unresolved.size());
        Set<String> names = new LinkedHashSet<>(unresolved.size());
        for (var ua : unresolved) {
            // some plans may reference the same UA multiple times (Aggregate groupings in aggregates, Eval)
            if (names.contains(ua.name()) == false && exclude.contains(ua.name()) == false) {
                insisted.add(insistKeyword(ua));
                names.add(ua.name());
            }
        }
        return insisted;
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

        return fork.replaceSubPlansAndOutput(newChildren, combine(fork.output(), aliasAttributes));
    }

    private static Project patchForkProject(Project project, List<Attribute> aliasAttributes) {
        // refresh the IDs for each UnionAll child (needed for correct resolution of convert functions; see collectConvertFunctions())
        aliasAttributes = aliasAttributes.stream().map(a -> a.withId(new NameId())).toList();

        project = project.withProjections(combine(project.projections(), aliasAttributes));

        // If Project's child doesn't output the attribute, introduce a null-Eval'ing. This is similar to what Fork-resolution does.
        List<Alias> nullAliases = new ArrayList<>(aliasAttributes.size());
        for (var attribute : aliasAttributes) {
            if (descendantOutputsAttribute(project, attribute) == false) {
                nullAliases.add(nullAlias(attribute));
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

    private static LogicalPlan refreshPlan(LogicalPlan plan, List<UnresolvedAttribute> unresolved) {
        var refreshed = refreshUnresolved(plan, unresolved);
        return refreshChildren(refreshed);
    }

    /**
     * The UAs that haven't been resolved are marked as unresolvable with a custom message. This needs to be removed for
     * {@link Analyzer.ResolveRefs} to attempt again to wire them to the newly added aliases. That's what this method does.
     */
    private static LogicalPlan refreshUnresolved(LogicalPlan plan, List<UnresolvedAttribute> unresolved) {
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

    /**
     * @return A plan having all nodes recreated (no properties changed, otherwise). This is needed to clear internal, lazy-eval'd and
     * cached state, such as the output. The rule inserts new attributes in the plan, so the output of all the nodes downstream these
     * insertions need be recomputed.
     */
    private static LogicalPlan refreshChildren(LogicalPlan plan) {
        var planChildren = plan.children();
        if (planChildren.isEmpty()) {
            return plan;
        }
        List<LogicalPlan> newChildren = new ArrayList<>(planChildren.size());
        planChildren.forEach(child -> newChildren.add(refreshChildren(child)));
        return plan.replaceChildren(newChildren);
    }

    /**
     * Inserts an Eval atop each child of the given {@code nAry}, if the child is a LeafPlan.
     */
    private static LogicalPlan evalUnresolvedNary(LogicalPlan nAry, List<Alias> nullAliases) {
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

    /**
     * Inserts an Eval atop the given {@code unaryAtopSource}, if this isn't an Eval already. Otherwise it merges the nullAliases into it.
     */
    private static LogicalPlan evalUnresolvedUnary(UnaryPlan unaryAtopSource, List<Alias> nullAliases) {
        assertSourceType(unaryAtopSource.child());
        if (unaryAtopSource instanceof Eval eval && eval.resolved()) { // if this Eval isn't resolved, insert a new (resolved) one
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
            return new Eval(eval.source(), eval.child(), combine(pre, eval.fields(), post));
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
        unresolved.forEach(u -> aliasesMap.computeIfAbsent(u.name(), k -> nullAlias(u)));
        return new ArrayList<>(aliasesMap.values());
    }

    private static Alias nullAlias(Attribute attribute) {
        return new Alias(attribute.source(), attribute.name(), NULLIFIED);
    }

    /**
     * @return all the {@link UnresolvedAttribute}s in the given node / {@code plan}, but excluding the {@link UnresolvedPattern} and
     * {@link UnresolvedTimestamp} subtypes.
     */
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
