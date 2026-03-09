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
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedPattern;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.MissingEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.esql.analysis.Analyzer.ResolveRefs.insistKeyword;
import static org.elasticsearch.xpack.esql.core.util.CollectionUtils.combine;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

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
        // In PromQL, queries never fail due to a field not being mapped, instead an empty result is returned.
        if (plan instanceof PromqlCommand) {
            return resolve(plan, false);
        }
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

        // Filter out unresolved attributes that exist in the children's output. These attributes are not truly unmapped;
        // they just haven't been resolved yet by ResolveRefs (e.g. because the children only became resolved after ImplicitCasting).
        // ResolveRefs will wire them up in the next iteration of the resolution batch.
        Set<String> childOutputNames = new java.util.HashSet<>();
        for (LogicalPlan child : plan.children()) {
            for (Attribute attr : child.output()) {
                childOutputNames.add(attr.name());
            }
        }
        unresolved.removeIf(ua -> childOutputNames.contains(ua.name()));
        if (unresolved.isEmpty()) {
            return plan;
        }

        var unresolvedLinkedSet = unresolvedLinkedSet(unresolved);
        var transformed = load ? load(plan, unresolvedLinkedSet) : nullify(plan, unresolvedLinkedSet);
        return transformed.equals(plan) ? plan : refreshPlan(transformed, unresolved);
    }

    /**
     * This method introduces null-typed field attributes for every attribute in {@code unresolved}, within the {@link EsRelation}s
     * in the plan. The fields are added with {@link DataType#NULL} type, which causes {@code ReplaceFieldWithConstantOrNull}
     * (in the local logical optimizer) to replace them with {@code Literal.NULL}.
     * <p>
     * For non-EsRelation sources (Row, LocalRelation), it falls back to inserting Eval nodes with null assignments.
     * <p>
     * It also "patches" the introduced attributes through the plan, where needed (like through Fork/UnionAll).
     */
    private static LogicalPlan nullify(LogicalPlan plan, Set<UnresolvedAttribute> unresolved) {
        // For EsRelation sources: add null-typed fields to the relation's output
        var transformed = plan.transformUp(EsRelation.class, esr -> {
            if (esr.indexMode() == IndexMode.LOOKUP) {
                return esr;
            }
            List<FieldAttribute> fieldsToNullify = fieldsToNullify(unresolved, Expressions.names(esr.output()));
            return fieldsToNullify.isEmpty() ? esr : esr.withAttributes(combine(esr.output(), fieldsToNullify));
        });

        // For non-EsRelation sources (Row, LocalRelation): insert Eval nodes with null assignments
        // This handles cases like: ROW x = 1 | EVAL y = unmapped_field
        transformed = transformed.transformUp(
            n -> n instanceof UnaryPlan unary && unary.child() instanceof LeafPlan leaf && leaf instanceof EsRelation == false,
            p -> evalUnresolvedAtopUnary((UnaryPlan) p, nullAliases(unresolved))
        );
        return transformed.transformUp(
            n -> n instanceof UnaryPlan == false && n instanceof LeafPlan == false,
            nAry -> evalUnresolvedAtopNaryNonEsRelation(nAry, nullAliases(unresolved))
        );
    }

    private static List<FieldAttribute> fieldsToNullify(Set<UnresolvedAttribute> unresolved, List<String> exclude) {
        List<FieldAttribute> nullified = new ArrayList<>(unresolved.size());
        for (var ua : unresolved) {
            if (exclude.contains(ua.name()) == false) {
                nullified.add(nullifyField(ua));
            }
        }
        return nullified;
    }

    private static FieldAttribute nullifyField(Attribute attribute) {
        return new FieldAttribute(
            attribute.source(),
            null,
            attribute.qualifier(),
            attribute.name(),
            new MissingEsField(attribute.name(), DataType.NULL, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
    }

    /**
     * This method introduces field extractors - via "insisted", {@link PotentiallyUnmappedKeywordEsField} wrapped in
     * {@link FieldAttribute} - for every attribute in {@code unresolved}, within the {@link EsRelation}s in the plan accessible from
     * the given {@code plan}.
     * <p>
     * It also "patches" the introduced attributes through the plan, where needed (like through Fork/UntionAll).
     */
    private static LogicalPlan load(LogicalPlan plan, Set<UnresolvedAttribute> unresolved) {
        // TODO: this will need to be revisited for non-lookup joining or scenarios where we won't want extraction from specific sources
        return plan.transformUp(EsRelation.class, esr -> {
            if (esr.indexMode() == IndexMode.LOOKUP) {
                return esr;
            }
            List<FieldAttribute> fieldsToLoad = fieldsToLoad(unresolved, Expressions.names(esr.output()));
            // there shouldn't be any duplicates, we can just merge the two lists
            return fieldsToLoad.isEmpty() ? esr : esr.withAttributes(combine(esr.output(), fieldsToLoad));
        });
    }

    private static List<FieldAttribute> fieldsToLoad(Set<UnresolvedAttribute> unresolved, List<String> exclude) {
        List<FieldAttribute> insisted = new ArrayList<>(unresolved.size());
        for (var ua : unresolved) {
            if (exclude.contains(ua.name()) == false) {
                insisted.add(insistKeyword(ua));
            }
        }
        return insisted;
    }

    // TODO: would an alternative to this be to have ResolveRefs#resolveFork re-resolve the Fork?
    // We might need some plan delimiters/markers to make it unequivocal which nodes belong to
    // "make Fork work" - like ([Limit -] Project [- Eval])s - and which don't.
    // PruneColumns does the same dance. There's some fragility w.r.t. assuming there to be a top Project and danger of the outputs not
    // being aligned after applying the changes.
    /**
     * Update the Fork's top Projects in the subplans, and correspondingly, its output, to account for newly introduced aliases.
     */
    private static Fork patchFork(Fork fork) {
        List<LogicalPlan> newChildren = new ArrayList<>(fork.children().size());
        boolean childrenChanged = false;
        for (var child : fork.children()) {
            Holder<Boolean> patched = new Holder<>(false);
            var transformed = child.transformDown(
                // TODO add a suitable forEachDownMayReturnEarly equivalent
                n -> patched.get() == false && n instanceof Project, // process top Project only (Fork-injected)
                n -> {
                    patched.set(true);
                    return patchForkProject((Project) n);
                }
            );
            childrenChanged |= transformed != child;
            newChildren.add(transformed);
        }
        return childrenChanged ? fork.withSubPlans(newChildren) : fork;
    }

    /**
     * Add any missing attributes that are found in the child's output but not in the Project's output. These have been injected before
     * by the evalUnresolvedAtopXXX methods and need to be "let through" the Project.
     */
    private static Project patchForkProject(Project project) {
        var projectOutput = project.output();
        var childOutput = project.child().output();
        if (projectOutput.equals(childOutput) == false) {
            List<Attribute> delta = new ArrayList<>(childOutput);
            delta.removeAll(projectOutput);
            project = project.withProjections(mergeOutputAttributes(delta, projectOutput));
        }
        return project;
    }

    private static LogicalPlan refreshPlan(LogicalPlan plan, List<UnresolvedAttribute> unresolved) {
        var refreshed = refreshUnresolved(plan, unresolved);
        return refreshed.transformDown(Fork.class, ResolveUnmapped::patchFork);
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
     * Inserts an Eval atop each child of the given {@code nAry}, if the child is a LeafPlan.
     */
    private static LogicalPlan evalUnresolvedAtopNary(LogicalPlan nAry, List<Alias> nullAliases) {
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
     * Inserts an Eval atop each child of the given {@code nAry}, if the child is a non-EsRelation LeafPlan.
     * EsRelation sources are handled separately by adding fields to their output.
     */
    private static LogicalPlan evalUnresolvedAtopNaryNonEsRelation(LogicalPlan nAry, List<Alias> nullAliases) {
        List<LogicalPlan> newChildren = new ArrayList<>(nAry.children().size());
        boolean changed = false;
        for (var child : nAry.children()) {
            if (child instanceof LeafPlan source && source instanceof EsRelation == false) {
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
    private static LogicalPlan evalUnresolvedAtopUnary(UnaryPlan unaryAtopSource, List<Alias> nullAliases) {
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

    private static List<Alias> nullAliases(Set<UnresolvedAttribute> unresolved) {
        List<Alias> aliases = new ArrayList<>(unresolved.size());
        unresolved.forEach(u -> aliases.add(nullAlias(u)));
        return aliases;
    }

    private static Alias nullAlias(NamedExpression attribute) {
        return new Alias(attribute.source(), attribute.name(), NULLIFIED);
    }

    // Some plans may reference the same UA multiple times (Aggregate groupings in aggregates, Eval): dedupe
    private static LinkedHashSet<UnresolvedAttribute> unresolvedLinkedSet(List<UnresolvedAttribute> unresolved) {
        Map<String, UnresolvedAttribute> aliasesMap = new LinkedHashMap<>(unresolved.size());
        unresolved.forEach(u -> aliasesMap.putIfAbsent(u.name(), u));
        return new LinkedHashSet<>(aliasesMap.values());
    }

    /**
     * @return all the {@link UnresolvedAttribute}s in the given node / {@code plan}, but excluding the {@link UnresolvedPattern} and
     * {@link UnresolvedTimestamp} subtypes.
     */
    public static List<UnresolvedAttribute> collectUnresolved(LogicalPlan plan) {
        List<UnresolvedAttribute> unresolved = new ArrayList<>();
        Consumer<UnresolvedAttribute> collectUnresolved = ua -> {
            // Exclude metadata fields so they fail with a proper verification error instead of being silently nullified/loaded.
            if ((ua instanceof UnresolvedPattern || ua instanceof UnresolvedTimestamp) == false
                && MetadataAttribute.isSupported(ua.name()) == false) {
                unresolved.add(ua);
            }
        };
        if (plan instanceof PromqlCommand promqlCommand) {
            // The expressions of the PromqlCommand itself are not relevant here.
            // The promqlPlan is a separate tree and its children may contain UnresolvedAttribute expressions
            promqlCommand.promqlPlan().forEachExpressionDown(UnresolvedAttribute.class, collectUnresolved);
        } else {
            plan.forEachExpression(UnresolvedAttribute.class, collectUnresolved);
        }
        return unresolved;
    }
}
