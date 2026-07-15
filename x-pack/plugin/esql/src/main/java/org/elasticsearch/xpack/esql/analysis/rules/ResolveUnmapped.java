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
import org.elasticsearch.xpack.esql.core.expression.Expression;
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
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

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

    private static EsRelation withAdditionalAttributesUnlessLookup(EsRelation esr, List<? extends Attribute> fields) {
        if (esr.indexMode() == IndexMode.LOOKUP || fields.isEmpty()) {
            return esr;
        }
        // Once real fields are added to an empty-mapping relation, the no-fields marker must not remain in the output.
        if (esr.output().equals(Analyzer.NO_FIELDS)) {
            return esr.withAttributes(new ArrayList<>(fields));
        }
        return esr.withAdditionalAttributes(fields);
    }

    @Override
    protected LogicalPlan rule(LogicalPlan plan, AnalyzerContext context) {
        return switch (context.unmappedResolution()) {
            case UnmappedResolution.DEFAULT -> plan;
            case UnmappedResolution.NULLIFY -> resolve(plan, false);
            case UnmappedResolution.LOAD -> resolve(plan, true);
        };
    }

    private static LogicalPlan resolve(LogicalPlan plan, boolean load) {
        if (plan.childrenResolved() == false) {
            return plan;
        }

        LinkedHashMap<String, List<UnresolvedAttribute>> unresolvedByName = collectUnresolved(plan);
        if (unresolvedByName.isEmpty()) {
            return plan;
        }

        // One representative UA per name for nullify/load (which only need distinct field names).
        LinkedHashSet<UnresolvedAttribute> unresolved = new LinkedHashSet<>(unresolvedByName.size());
        // All UAs (multiple per name) for refreshPlan (which needs to refresh every occurrence in the plan).
        Set<UnresolvedAttribute> allUnresolved = new HashSet<>();
        for (List<UnresolvedAttribute> uas : unresolvedByName.values()) {
            unresolved.add(uas.getFirst());
            allUnresolved.addAll(uas);
        }

        var transformed = load ? load(plan, unresolved) : nullify(plan, unresolved);
        return transformed == plan ? plan : refreshPlan(transformed, allUnresolved);
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
    private static LogicalPlan nullify(LogicalPlan plan, LinkedHashSet<UnresolvedAttribute> unresolved) {
        // For EsRelation sources: add null-typed fields to the relation's output
        var transformed = plan.transformUp(EsRelation.class, esr -> {
            List<FieldAttribute> fieldsToNullify = fieldsToNullify(unresolved, Expressions.names(esr.output()));
            return withAdditionalAttributesUnlessLookup(esr, fieldsToNullify);
        });
        return nullifyNonEsRelationSources(transformed, unresolved);
    }

    /**
     * Inserts {@code EVAL <name> = NULL} atop non-{@link EsRelation} sources (Row/LocalRelation) for every attribute in
     * {@code unresolved}. EsRelation sources are handled separately (their output gains the fields directly). This handles cases
     * like {@code ROW x = 1 | EVAL y = unmapped_field}.
     */
    private static LogicalPlan nullifyNonEsRelationSources(LogicalPlan plan, LinkedHashSet<UnresolvedAttribute> unresolved) {
        var transformed = plan.transformUp(
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
     * Inserts {@link PotentiallyUnmappedKeywordEsField} loaders (insisted keywords wrapped in {@link FieldAttribute}) for
     * {@code unresolved} into the plan's {@link EsRelation}s, scope-aware across subqueries/views: an outer reference (surfaced by
     * no {@link UnionAll} branch) is broadcast into all branches; an in-branch reference stays scoped to its own source. See #142033.
     */
    private static LogicalPlan load(LogicalPlan plan, Set<UnresolvedAttribute> unresolved) {
        // TODO: this will need to be revisited for non-lookup joining or scenarios where we won't want extraction from specific sources
        if (plan.anyMatch(p -> p instanceof UnionAll)) {
            // Outer references only: a name already surfaced by a branch resolves through the union output. #142033
            Set<String> surfacedByAnyBranch = mainSpineUnionBranchOutputNames(plan);
            LinkedHashSet<UnresolvedAttribute> outerReferences = new LinkedHashSet<>();
            for (UnresolvedAttribute ua : unresolved) {
                if (surfacedByAnyBranch.contains(ua.name()) == false) {
                    outerReferences.add(ua);
                }
            }
            return outerReferences.isEmpty() ? plan : loadIntoSources(plan, outerReferences);
        }
        return loadIntoSources(plan, unresolved);
    }

    /**
     * Adds {@code _source} keyword loaders for {@code toLoad} to every non-LOOKUP {@link EsRelation} reachable from {@code plan};
     * Row/LocalRelation sources can't load from {@code _source} and are left for {@code ResolveRefs#resolveFork} to null-fill.
     */
    private static LogicalPlan loadIntoSources(LogicalPlan plan, Set<UnresolvedAttribute> toLoad) {
        return plan.transformUp(EsRelation.class, esr -> {
            List<FieldAttribute> fieldsToLoad = fieldsToLoad(toLoad, Expressions.names(esr.output()));
            return withAdditionalAttributesUnlessLookup(esr, fieldsToLoad);
        });
    }

    /**
     * Names surfaced by {@link UnionAll} branches on the main (left) pipeline of {@code plan}. A join's right side is an
     * independent subquery scope (e.g. the RHS of an {@code IN} subquery) and is NOT traversed: its transient branch outputs
     * must not suppress broadcast-loading of an outer reference belonging to a sibling union. See #142033 / PR #151750.
     */
    private static Set<String> mainSpineUnionBranchOutputNames(LogicalPlan plan) {
        Set<String> names = new HashSet<>();
        collectMainSpineUnionBranchOutputNames(plan, names);
        return names;
    }

    private static void collectMainSpineUnionBranchOutputNames(LogicalPlan plan, Set<String> names) {
        if (plan instanceof UnionAll ua) {
            // Outermost union's direct branch outputs only; nested unions are rejected downstream by checkNestedUnionAlls.
            for (LogicalPlan branch : ua.children()) {
                names.addAll(Expressions.names(branch.output()));
            }
            return;
        }
        if (plan instanceof Join join) {
            collectMainSpineUnionBranchOutputNames(join.left(), names); // right side is an independent subquery scope
            return;
        }
        for (LogicalPlan child : plan.children()) {
            collectMainSpineUnionBranchOutputNames(child, names);
        }
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
        Holder<Boolean> changed = new Holder<>(false);
        Fork transformed = (Fork) fork.transformDownSkipBranch((plan, skip) -> {
            if (plan instanceof Project project) {
                skip.set(true); // process top Project only (Fork-injected)
                plan = patchForkProject(project);
                if (plan != project) {
                    changed.set(Boolean.TRUE);
                }
            }
            return plan;
        });

        return changed.get() ? transformed.refreshOutput() : fork;
    }

    /**
     * Add any missing attributes that are found in the child's output but not in the Project's output. These have been injected before
     * by the evalUnresolvedAtopXXX methods and need to be "let through" the Project.
     */
    // Maybe using ResolvingProjects at the top of the Fork branches would be a more simple solution; adding the `*` pattern
    // would let any newly introduced attribute through without the need to patch the Projects, we'd just have to refresh the fork output.
    private static Project patchForkProject(Project project) {
        List<Attribute> projectOutput = project.output();
        List<Attribute> childOutput = project.child().output();
        if (projectOutput.equals(childOutput) == false) {
            List<Attribute> delta = new ArrayList<>(childOutput);
            delta.removeAll(projectOutput);
            if (delta.isEmpty() == false) {
                project = project.withProjections(mergeOutputAttributes(delta, projectOutput));
            }
        }
        return project;
    }

    /**
     * UAs that weren't resolvable at first were added to the plan. But {@link Analyzer.ResolveRefs} has marked all or some of them as
     * unresolvable by attaching a custom message. This needs to be removed for {@link Analyzer.ResolveRefs} to attempt resolving them
     * again. That's what this method does.
     */
    private static LogicalPlan refreshPlan(LogicalPlan plan, Set<UnresolvedAttribute> maybeNowResolvableAttributes) {
        Map<UnresolvedAttribute, UnresolvedAttribute> oldAttributesToNewAttributes = new HashMap<>();
        Function<UnresolvedAttribute, UnresolvedAttribute> refresh = ua -> {
            if (maybeNowResolvableAttributes.contains(ua)) {
                // Besides clearing the message, we need to refresh the nameId to avoid equality with the previous plan.
                // (A `new UnresolvedAttribute(ua.source(), ua.name())` would save an allocation, but is problematic with subtypes.)
                return oldAttributesToNewAttributes.computeIfAbsent(ua, u -> (u.withId(new NameId())).withUnresolvedMessage(null));
            }
            return ua;
        };
        var refreshed = plan.transformExpressionsOnlyUp(UnresolvedAttribute.class, refresh);
        return refreshed.transformDown(Fork.class, ResolveUnmapped::patchFork);
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
            List<Alias> filteredNullAliases = removeShadowing(nullAliases, unaryAtopSource.child().output());
            var newChild = filteredNullAliases.isEmpty()
                ? unaryAtopSource.child()
                : new Eval(unaryAtopSource.source(), unaryAtopSource.child(), filteredNullAliases);
            return unaryAtopSource.replaceChild(newChild);
        }
    }

    private static List<Alias> removeShadowing(List<Alias> aliases, List<Attribute> exclude) {
        Set<String> excludeNames = new HashSet<>(Expressions.names(exclude));
        aliases.removeIf(a -> excludeNames.contains(a.name()));
        return aliases;
    }

    private static void assertSourceType(LogicalPlan source) {
        switch (source) {
            case EsRelation esRelation -> {
                IndexMode mode = esRelation.indexMode();
                if ((mode == IndexMode.STANDARD || mode.isTsdb()) == false) {
                    throw new EsqlIllegalArgumentException(
                        "invalid source type [{}] for unmapped field resolution",
                        esRelation.indexMode()
                    );
                }
            }
            case ExternalRelation unused -> {
            }
            case Row unused -> {
            }
            case LocalRelation unused -> {
            }
            default -> throw new EsqlIllegalArgumentException("unexpected source type [{}]", source);
        }
    }

    private static List<Alias> nullAliases(LinkedHashSet<UnresolvedAttribute> unresolved) {
        List<Alias> aliases = new ArrayList<>(unresolved.size());
        unresolved.forEach(u -> aliases.add(nullAlias(u)));
        return aliases;
    }

    private static Alias nullAlias(NamedExpression attribute) {
        return new Alias(attribute.source(), attribute.name(), NULLIFIED);
    }

    /**
     * @return all the {@link UnresolvedAttribute}s in the given node / {@code plan}, grouped by name (preserving insertion order), but
     * excluding the {@link UnresolvedPattern} and {@link UnresolvedTimestamp} subtypes.
     */
    private static LinkedHashMap<String, List<UnresolvedAttribute>> collectUnresolved(LogicalPlan plan) {
        Set<String> aliasedGroupings = aliasNamesInAggregateGroupings(plan);

        LinkedHashMap<String, List<UnresolvedAttribute>> unresolved = new LinkedHashMap<>();
        Consumer<UnresolvedAttribute> sink = ua -> {
            if (leaveUnresolved(ua) == false
                // The aggs will "export" the aliases as UnresolvedAttributes part of their .aggregates(); we don't need to consider those
                // as they'll be resolved as refs once the aliased expression is resolved.
                && aliasedGroupings.contains(ua.name()) == false) {
                unresolved.computeIfAbsent(ua.name(), k -> new ArrayList<>()).add(ua);
            }
        };
        if (plan instanceof PromqlCommand promqlCommand) {
            // The expressions of the PromqlCommand itself are not relevant here.
            // The promqlPlan is a separate tree and its children may contain UnresolvedAttribute expressions
            promqlCommand.promqlPlan().forEachExpressionDown(UnresolvedAttribute.class, sink);
        } else {
            plan.forEachDown(LogicalPlan.class, node -> collectLoadCandidates(node, sink));
        }
        return unresolved;
    }

    /**
     * Per-node walk: which unresolved attributes from this node's local context (its own expressions or join config)
     * cannot be resolved against its immediate children's outputs, and so are candidates for {@code _source} loading?
     * UAs whose names match a child's output are skipped — ResolveRefs will wire them up on the next iteration.
     */
    private static void collectLoadCandidates(LogicalPlan node, Consumer<UnresolvedAttribute> sink) {
        if (node instanceof LookupJoin lj) {
            Set<String> leftOutputNames = new HashSet<>(Expressions.names(lj.left().output()));
            Set<String> rightOutputNames = new HashSet<>(Expressions.names(lj.right().output()));
            collectUnresolvedLeftKeys(lj, leftOutputNames, sink);
            // joinOnConditions UAs not found in either child → load candidates.
            Expression conds = lj.config().joinOnConditions();
            if (conds != null) {
                conds.forEachUp(UnresolvedAttribute.class, ua -> {
                    if (leftOutputNames.contains(ua.name()) == false && rightOutputNames.contains(ua.name()) == false) {
                        sink.accept(ua);
                    }
                });
            }
            // Unresolved right keys are intentionally ignored — a query that references them is invalid and will fail verification.
        } else if (node instanceof AbstractSubqueryJoin sj) {
            // RHS surfaces the same single-column name, so the generic else-branch would mask an unmapped IN left key. #142033
            collectUnresolvedLeftKeys(sj, new HashSet<>(Expressions.names(sj.left().output())), sink);
        } else {
            Set<String> childOutputNames = new HashSet<>();
            for (LogicalPlan child : node.children()) {
                for (Attribute a : child.output()) {
                    childOutputNames.add(a.name());
                }
            }
            for (Expression expr : node.expressions()) {
                expr.forEachUp(UnresolvedAttribute.class, ua -> {
                    if (childOutputNames.contains(ua.name()) == false) {
                        sink.accept(ua);
                    }
                });
            }
        }
    }

    /**
     * Unresolved {@code leftFields} of a {@link Join} whose name is absent from the left child's output → {@code _source} load
     * candidates. Left keys must resolve against the left side only (mirrors {@code ResolveRefs} join resolution).
     */
    private static void collectUnresolvedLeftKeys(Join join, Set<String> leftOutputNames, Consumer<UnresolvedAttribute> sink) {
        for (Attribute lf : join.config().leftFields()) {
            if (lf instanceof UnresolvedAttribute ua && leftOutputNames.contains(ua.name()) == false) {
                sink.accept(ua);
            }
        }
    }

    private static boolean leaveUnresolved(UnresolvedAttribute attribute) {
        return attribute instanceof UnresolvedPattern || attribute instanceof UnresolvedTimestamp
        // Exclude metadata fields so they fail with a proper verification error instead of being silently nullified/loaded.
            || MetadataAttribute.isSupported(attribute.name());
    }

    /**
     * @return the names of the aliases used in the grouping expressions of any Aggregate found in the plan.
     */
    private static Set<String> aliasNamesInAggregateGroupings(LogicalPlan plan) {
        Set<String> aliasNames = new HashSet<>();
        plan.forEachUp(Aggregate.class, agg -> {
            for (var grouping : agg.groupings()) {
                if (grouping instanceof Alias alias) {
                    aliasNames.add(alias.name());
                }
            }
        });
        return aliasNames;
    }
}
