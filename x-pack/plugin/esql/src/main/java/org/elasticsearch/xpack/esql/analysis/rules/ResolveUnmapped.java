/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.BranchIdentity;
import org.elasticsearch.xpack.esql.plan.logical.BranchPlan;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.ResolvingProject;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.view.ViewQuerySettings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.analysis.Analyzer.ResolveRefs.branchCanSurfaceLoadedField;
import static org.elasticsearch.xpack.esql.analysis.Analyzer.ResolveRefs.nullifyField;
import static org.elasticsearch.xpack.esql.analysis.Analyzer.ResolveRefs.unmappedKeyword;
import static org.elasticsearch.xpack.esql.analysis.Analyzer.ResolveRefs.withAdditionalAttributesUnlessLookup;
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
 * <p>
 * None of this reaches inside a view body. A view is a virtual index, and SET unmapped_fields is an ES-index concept
 * (loading from {@code _source}) that callers should not see through the view boundary — the same principle that keeps
 * the raw request-filter DSL out of a view's source scans. The boundary is structural: every view branch of a
 * {@link BranchPlan} (which view resolution keeps for every view while this setting is active, see
 * {@code ViewBoundaryPreservation#KEEP_VIEW_IDENTITY}) is skipped wholesale, both by this rule's own traversal and by
 * every walk it makes, so references in a view body are never load/nullify candidates and relations in one receive no
 * synthetic fields. The only thing done at a view is answering an outer reference at the view branch's output with
 * null ({@link #nullFillViewBoundaries}) — the virtual index has no {@code _source} to load from.
 * Re-enabled inside a body only by a future SET unmapped_fields in the view definition itself.
 */
public class ResolveUnmapped extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

    private static final Literal NULLIFIED = Literal.NULL;

    @Override
    public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
        return applyScoped(plan, context.unmappedResolution(), null, context);
    }

    /**
     * Applies {@code resolution} to one scope's territory — the nodes of {@code plan} outside view branches — after
     * first recursing into every view branch with the resolution that view's own definition {@code SET} (none, when
     * the definition has no {@code SET}; the caller's never applies in there). Inner scopes go first so a widened
     * view-branch output is visible when this scope's pass runs. A view branch only contains further view branches
     * when its own settings required nested view identity, so a definition without {@code SET}s is one flat skip,
     * exactly as before per-view settings existed. {@code settingView} is the view whose definition {@code SET}
     * {@code resolution}, or {@code null} for the caller's query.
     */
    private static LogicalPlan applyScoped(
        LogicalPlan plan,
        UnmappedResolution resolution,
        @Nullable String settingView,
        AnalyzerContext context
    ) {
        LogicalPlan withInnerScopes = transformViewBranches(plan, (viewName, branch) -> {
            UnmappedResolution viewResolution = ViewQuerySettings.forView(viewName, context.viewSettings()).unmappedFields();
            return applyScoped(branch, viewResolution, viewName, context);
        });
        BiConsumer<BranchIdentity, String> onViewNullFill = (identity, fieldName) -> {
            String viewName = identity.settingsViewName();
            boolean viewLoads = ViewQuerySettings.forView(viewName, context.viewSettings()).unmappedFields().loadsUnmappedFields();
            List<String> lineage = identity.lineage();
            String enclosingView = lineage.size() < 2 ? null : lineage.get(lineage.size() - 2);
            var boundary = new AnalyzerContext.ViewBoundary(resolution, settingView, viewName, enclosingView, viewLoads);
            context.viewBoundaryNullFills().add(new AnalyzerContext.ViewBoundaryNullFill(boundary, fieldName));
        };
        return switch (resolution) {
            case UnmappedResolution.DEFAULT -> withInnerScopes;
            case UnmappedResolution.NULLIFY -> resolveOutsideViews(withInnerScopes, false, onViewNullFill);
            case UnmappedResolution.LOAD, UnmappedResolution.LOAD_ALL -> resolveOutsideViews(withInnerScopes, true, onViewNullFill);
        };
    }

    /**
     * Rewrites every view branch of {@code plan} (outside deeper view branches — those are the inner scopes'
     * own business) through {@code fn}, which receives the owning view's name ({@code BranchIdentity#settingsViewName},
     * the innermost view after a nested view is lifted into its parent) and the branch subtree.
     */
    private static LogicalPlan transformViewBranches(LogicalPlan plan, BiFunction<String, LogicalPlan, LogicalPlan> fn) {
        List<LogicalPlan> children = plan.children();
        List<LogicalPlan> newChildren = new ArrayList<>(children.size());
        boolean changed = false;
        for (int i = 0; i < children.size(); i++) {
            LogicalPlan child = children.get(i);
            LogicalPlan newChild = ViewUtils.isViewBody(plan, i)
                ? fn.apply(((BranchPlan) plan).branchIdentity(i).settingsViewName(), child)
                : transformViewBranches(child, fn);
            changed |= newChild != child;
            newChildren.add(newChild);
        }
        return changed ? plan.replaceChildren(newChildren) : plan;
    }

    /**
     * Applies {@link #resolve} bottom-up to every unresolved node, like an
     * {@link AnalyzerRules.ParameterizedAnalyzerRule} would, except that view
     * bodies are skipped entirely: the rule never fires on a node inside one.
     */
    private static LogicalPlan resolveOutsideViews(LogicalPlan plan, boolean load, BiConsumer<BranchIdentity, String> onViewNullFill) {
        return ViewUtils.transformUpOutsideViews(plan, p -> p.analyzed() || p.resolved() ? p : resolve(p, load, onViewNullFill));
    }

    private static LogicalPlan resolve(LogicalPlan plan, boolean load, BiConsumer<BranchIdentity, String> onViewNullFill) {
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

        var transformed = load ? load(plan, unresolved, observedOnly(plan, onViewNullFill)) : nullify(plan, unresolved);
        if (transformed == plan) {
            // Nothing newly loaded or filled, but an earlier fill can still be pending: a view-boundary fill
            // (nullFillViewBoundaries) below a nested merge only widens that merge's output once ResolveRefs
            // realigns it, which happens after this rule's refreshPlan already ran its patch pass. The outer
            // merge's alignment Projects then still hide the new column and resolution is stuck. Re-run the
            // patch pass so the column surfaces, and refresh the waiting references (ResolveRefs marked them
            // unresolvable in the meantime and skips marked attributes); once they resolve, this rule no-ops.
            var patched = patchMergePlans(plan);
            return patched == plan ? plan : refreshPlan(patched, allUnresolved);
        }
        return refreshPlan(transformed, allUnresolved);
    }

    /**
     * Passes on only fills of names that something other than a {@code DROP} references: a field the query drops is not in its
     * result, so whether the view would have loaded it does not matter.
     */
    private static BiConsumer<BranchIdentity, String> observedOnly(LogicalPlan plan, BiConsumer<BranchIdentity, String> onViewNullFill) {
        Set<String> observed = new HashSet<>();
        ViewUtils.forEachOutsideViews(plan, node -> {
            if ((node instanceof ResolvingProject rp && rp.kind() == ResolvingProject.Kind.DROP) == false) {
                collectLoadCandidates(node, ua -> observed.add(ua.name()));
            }
        });
        return (identity, fieldName) -> {
            if (observed.contains(fieldName)) {
                onViewNullFill.accept(identity, fieldName);
            }
        };
    }

    /**
     * This method introduces null-typed field attributes for every attribute in {@code unresolved}, within the {@link EsRelation}s
     * in the plan. The fields are added with {@link DataType#NULL} type, which causes {@code ReplaceFieldWithConstantOrNull}
     * (in the local logical optimizer) to replace them with {@code Literal.NULL}.
     * <p>
     * For non-EsRelation sources (Row, LocalRelation), it falls back to inserting Eval nodes with null assignments.
     * <p>
     * It also "patches" the introduced attributes through the plan, where needed (like through a {@link MergePlan}).
     */
    private static LogicalPlan nullify(LogicalPlan plan, LinkedHashSet<UnresolvedAttribute> unresolved) {
        // For EsRelation sources: add null-typed fields to the relation's output
        var transformed = ViewUtils.transformUpOutsideViews(plan, p -> {
            if (p instanceof EsRelation esr) {
                List<FieldAttribute> fieldsToNullify = fieldsToNullify(unresolved, Expressions.names(esr.output()));
                return withAdditionalAttributesUnlessLookup(esr, fieldsToNullify, true);
            }
            return p;
        });
        // Under NULLIFY the view's own rows would be null for the field anyway, so the boundary changes nothing worth reporting.
        transformed = nullFillViewBoundaries(transformed, unresolved, false, (identity, fieldName) -> {});
        return nullifyNonEsRelationSources(transformed, unresolved);
    }

    /**
     * Inserts {@code EVAL <name> = NULL} atop non-{@link EsRelation} sources (Row/LocalRelation) for every attribute in
     * {@code unresolved}. EsRelation sources are handled separately (their output gains the fields directly). This handles cases
     * like {@code ROW x = 1 | EVAL y = unmapped_field}.
     */
    private static LogicalPlan nullifyNonEsRelationSources(LogicalPlan plan, LinkedHashSet<UnresolvedAttribute> unresolved) {
        return ViewUtils.transformUpOutsideViews(plan, p -> switch (p) {
            case UnaryPlan unary when unary.child() instanceof LeafPlan leaf && leaf instanceof EsRelation == false ->
                evalUnresolvedAtopUnary(unary, nullAliases(unresolved));
            case UnaryPlan unary -> unary;
            case LeafPlan leaf -> leaf;
            default -> evalUnresolvedAtopNaryNonEsRelation(p, nullAliases(unresolved));
        });
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

    /**
     * Inserts {@link PotentiallyUnmappedKeywordEsField} loaders (unmapped keywords wrapped in {@link FieldAttribute}) for
     * {@code unresolved} into the plan's {@link EsRelation}s, scope-aware across subqueries/views: an outer reference (surfaced by
     * no {@link UnionAll} branch) is broadcast into all branches; an in-branch reference stays scoped to its own source. See #142033.
     */
    private static LogicalPlan load(
        LogicalPlan plan,
        Set<UnresolvedAttribute> unresolved,
        BiConsumer<BranchIdentity, String> onViewNullFill
    ) {
        // TODO: this will need to be revisited for non-lookup joining or scenarios where we won't want extraction from specific sources
        if (plan.anyMatch(p -> p instanceof UnionAll)) {
            // Outer references only: a name already surfaced by a branch resolves through the merge output. #142033
            Set<String> surfacedByAnyBranch = mainSpineUnionBranchOutputNames(plan);
            LinkedHashSet<UnresolvedAttribute> outerReferences = new LinkedHashSet<>();
            for (UnresolvedAttribute ua : unresolved) {
                if (surfacedByAnyBranch.contains(ua.name()) == false) {
                    outerReferences.add(ua);
                }
            }
            return outerReferences.isEmpty()
                ? plan
                : nullFillViewBoundaries(loadIntoSources(plan, outerReferences), outerReferences, true, onViewNullFill);
        }
        return nullFillViewBoundaries(loadIntoSources(plan, unresolved), unresolved, true, onViewNullFill);
    }

    /**
     * Answers references at every view branch's <em>output</em> with {@code EVAL <name> = NULL}: a view contributes
     * null for a column it does not surface — under LOAD a KEYWORD-typed null, exactly like an index whose
     * {@code _source} lacks the field; under NULLIFY the same untyped null every other nullified source produces.
     * The boundary is each view branch of a {@link BranchPlan}, found by descending non-view branches; the Eval sits
     * atop the whole branch, making the null a column of the view's output rather than an input to the body's own
     * processing, and the view's body is never entered. A {@link Join}'s right side is an independent scope whose
     * output must keep its arity (lookup index or IN-subquery RHS), so only the left side is walked.
     * <p>
     * {@code onViewNullFill} receives the view branch's identity and the field name of every fill that only happened because
     * the mode stops at the view boundary; see {@link #nullFillViewOutput}.
     */
    private static LogicalPlan nullFillViewBoundaries(
        LogicalPlan plan,
        Set<UnresolvedAttribute> unresolved,
        boolean load,
        BiConsumer<BranchIdentity, String> onViewNullFill
    ) {
        List<LogicalPlan> children = plan.children();
        boolean changed = false;
        List<LogicalPlan> newChildren = new ArrayList<>(children.size());
        for (int i = 0; i < children.size(); i++) {
            LogicalPlan child = children.get(i);
            LogicalPlan newChild;
            if (plan instanceof Join && i > 0) {
                newChild = child;
            } else if (ViewUtils.isViewBody(plan, i)) {
                newChild = nullFillViewOutput(((BranchPlan) plan).branchIdentity(i), child, unresolved, load, onViewNullFill);
            } else {
                newChild = nullFillViewBoundaries(child, unresolved, load, onViewNullFill);
            }
            changed |= newChild != child;
            newChildren.add(newChild);
        }
        // No output surgery is needed on a merge above a fill: the fill Eval widens its branch's output, which
        // makes the merge report unresolved (MergePlan.expressionsResolved requires every branch to match the
        // stored output), so ResolveRefs realigns it — null-filling siblings and preserving existing output ids.
        return changed ? plan.replaceChildren(newChildren) : plan;
    }

    /**
     * Puts {@code EVAL <name> = NULL} atop a view branch for every reference in {@code unresolved} the branch does not
     * already output; see {@link #nullFillViewBoundaries}. Under LOAD, a fill the view body would have surfaced had the mode
     * applied inside it is reported to {@code onViewNullFill}: those are the rows a caller who knows the view's contents may
     * expect to be loaded. A body that projects or aggregates the field away would hide it either way, so is not reported.
     */
    private static LogicalPlan nullFillViewOutput(
        BranchIdentity identity,
        LogicalPlan viewBranch,
        Set<UnresolvedAttribute> unresolved,
        boolean load,
        BiConsumer<BranchIdentity, String> onViewNullFill
    ) {
        List<Alias> aliases = new ArrayList<>();
        var outputNames = viewBranch.outputSet().names();
        LogicalPlan body = withoutMergeAlignment(viewBranch);
        for (UnresolvedAttribute ua : unresolved) {
            if (outputNames.contains(ua.name()) == false) {
                aliases.add(load ? new Alias(ua.source(), ua.name(), new Literal(ua.source(), null, DataType.KEYWORD)) : nullAlias(ua));
                if (load && viewBodyCanSurfaceLoadedField(body, ua.name())) {
                    onViewNullFill.accept(identity, ua.name());
                }
            }
        }
        // Always a new Eval, never appended to the branch's top one: that may be the view body's own.
        return aliases.isEmpty() ? viewBranch : new Eval(viewBranch.source(), viewBranch, aliases);
    }

    /**
     * Whether the view body {@code body} would surface a field {@code name} loaded at its sources. A merge retained in the body
     * surfaces it when any branch does, judged below that branch's alignment layers. A view branch of a nested {@link BranchPlan}
     * is skipped: that view's own boundary stops the mode whether or not the enclosing view applies it.
     */
    private static boolean viewBodyCanSurfaceLoadedField(LogicalPlan body, String name) {
        return branchCanSurfaceLoadedField(body, name, merge -> {
            for (int i = 0; i < merge.children().size(); i++) {
                if (ViewUtils.isViewBody(merge, i) == false
                    && viewBodyCanSurfaceLoadedField(withoutMergeAlignment(merge.children().get(i)), name)) {
                    return true;
                }
            }
            return false;
        });
    }

    /**
     * {@code branch} without the nodes stacked atop the view body to align branch outputs, whose Projects would otherwise hide
     * every field the body could surface. Each analyzer pass that fills a field adds a fill Eval, and the outer merge then
     * realigns with its own missing-column Eval and Project, so a field referenced only by a later command sees several such
     * layers. All of them are plain Projects or Evals reusing their child's source. A view body without a {@code SET} resolves
     * a user's KEEP, DROP or RENAME to a plain Project too, but one with the source of its own command, so peeling stops there.
     */
    private static LogicalPlan withoutMergeAlignment(LogicalPlan branch) {
        LogicalPlan plan = branch;
        while ((plan.getClass() == Project.class || plan instanceof Eval) && plan.source().equals(((UnaryPlan) plan).child().source())) {
            plan = ((UnaryPlan) plan).child();
        }
        return plan;
    }

    /**
     * Adds {@code _source} keyword loaders for {@code toLoad} to every non-LOOKUP {@link EsRelation} reachable from {@code plan}
     * outside view bodies; Row/LocalRelation sources can't load from {@code _source} and are left for
     * {@code ResolveRefs#resolveMergePlan} to null-fill.
     */
    private static LogicalPlan loadIntoSources(LogicalPlan plan, Set<UnresolvedAttribute> toLoad) {
        return ViewUtils.transformUpOutsideViews(plan, p -> {
            if (p instanceof EsRelation esr) {
                List<FieldAttribute> fieldsToLoad = fieldsToLoad(toLoad, Expressions.names(esr.output()));
                return withAdditionalAttributesUnlessLookup(esr, fieldsToLoad, true);
            }
            return p;
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
            // Outermost union's direct branch outputs only: a branch's root output already surfaces any union nested inside it exposes,
            // and names surfaced only inside a nested union but not by the branch itself do not resolve past the outer union anyway.
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
        List<FieldAttribute> loaded = new ArrayList<>(unresolved.size());
        for (var ua : unresolved) {
            if (exclude.contains(ua.name()) == false) {
                loaded.add(unmappedKeyword(ua));
            }
        }
        return loaded;
    }

    // TODO: would an alternative to this be to have ResolveRefs#resolveMergePlan re-resolve the MergePlan?
    // We might need some plan delimiters/markers to make it unequivocal which nodes belong to
    // "make the merge work" - like ([Limit -] Project [- Eval])s - and which don't.
    // PruneColumns does the same dance. There's some fragility w.r.t. assuming there to be a top Project and danger of the outputs not
    // being aligned after applying the changes.
    /**
     * Update the merge's top Projects in the subplans, and correspondingly, its output, to account for newly introduced aliases.
     */
    private static MergePlan patchMergePlan(MergePlan mergePlan) {
        Holder<Boolean> changed = new Holder<>(false);
        MergePlan transformed = (MergePlan) mergePlan.transformDownSkipBranch((plan, skip) -> {
            if (plan instanceof Project project) {
                skip.set(true); // process top Project only (merge-injected)
                // Only a merge-injected alignment Project (exact class; resolveMergePlan emits plain Projects) may
                // surface newly loaded attributes. The branch's top projection can also be a user-written one
                // (KEEP/DROP/RENAME Project subclasses) now that branches are no longer wrapped in Subquery nodes:
                // such an explicit column list must keep hiding what it hides, otherwise a broadcast load would
                // e.g. resurrect a DROPped unmapped field inside that branch.
                // TODO: keying on the exact class is fragile. Either have resolveMergePlan emit ResolvingProject
                // (which passes unmapped fields through by design) or add an explicit merge-injected marker on
                // Project, and match on that instead.
                if (plan.getClass() == Project.class) {
                    plan = patchMergeProject(project);
                    if (plan != project) {
                        changed.set(Boolean.TRUE);
                    }
                }
            }
            return plan;
        });

        return changed.get() ? transformed.refreshOutput() : mergePlan;
    }

    /**
     * Add any missing attributes that are found in the child's output but not in the Project's output. These have been injected before
     * by the evalUnresolvedAtopXXX methods and need to be "let through" the Project.
     */
    // Maybe using ResolvingProjects at the top of the merge branches would be a more simple solution; adding the `*` pattern
    // would let any newly introduced attribute through without the need to patch the Projects, we'd just have to refresh the merge output.
    private static Project patchMergeProject(Project project) {
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
        // Bottom-up: patchForkProject reads project.child().output(), and a Fork reports a stored output rather than recomputing it,
        // so a nested union has to be patched (and refreshOutput'd) before its parent reads it. Top-down leaves the outer branch's
        // alignment Project without the newly loaded attribute, and resolveFork then null-fills the column.
        return patchMergePlans(refreshed);
    }

    /**
     * {@link #patchMergePlan} for every merge outside view bodies, bottom-up.
     */
    private static LogicalPlan patchMergePlans(LogicalPlan plan) {
        return ViewUtils.transformUpOutsideViews(plan, p -> p instanceof MergePlan mergePlan ? patchMergePlan(mergePlan) : p);
    }

    /**
     * Inserts an Eval atop each child of the given {@code nAry}, if the child is a non-EsRelation LeafPlan outside view bodies.
     * EsRelation sources are handled separately by adding fields to their output.
     */
    private static LogicalPlan evalUnresolvedAtopNaryNonEsRelation(LogicalPlan nAry, List<Alias> nullAliases) {
        List<LogicalPlan> newChildren = new ArrayList<>(nAry.children().size());
        boolean changed = false;
        for (int i = 0; i < nAry.children().size(); i++) {
            LogicalPlan child = nAry.children().get(i);
            if (child instanceof LeafPlan source && source instanceof EsRelation == false && ViewUtils.isViewBody(nAry, i) == false) {
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
     * excluding the {@link UnresolvedPattern} and {@link UnresolvedTimestamp} subtypes, and those inside view bodies: a reference
     * written in a view body fails resolution exactly as it would without the setting, which does not reach through the view boundary.
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
            ViewUtils.forEachOutsideViews(plan, node -> collectLoadCandidates(node, sink));
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
     * @return the names of the aliases used in the grouping expressions of any Aggregate found in the plan outside view bodies.
     */
    private static Set<String> aliasNamesInAggregateGroupings(LogicalPlan plan) {
        Set<String> aliasNames = new HashSet<>();
        ViewUtils.forEachOutsideViews(plan, p -> {
            if (p instanceof Aggregate agg) {
                for (var grouping : agg.groupings()) {
                    if (grouping instanceof Alias alias) {
                        aliasNames.add(alias.name());
                    }
                }
            }
        });
        return aliasNames;
    }
}
