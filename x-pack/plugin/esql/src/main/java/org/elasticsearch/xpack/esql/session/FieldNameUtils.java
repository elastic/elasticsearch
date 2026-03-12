/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.EmptyAttribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedPattern;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedStar;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.TBucket;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.TRange;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.CompoundOutputEval;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Insist;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.session.EsqlSession.PreAnalysisResult;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.WILDCARD;

public class FieldNameUtils {

    private static final Set<String> FUNCTIONS_REQUIRING_TIMESTAMP = Set.of(
        TBucket.NAME.toLowerCase(Locale.ROOT),
        TRange.NAME.toLowerCase(Locale.ROOT)
    );

    public static PreAnalysisResult resolveFieldNames(LogicalPlan parsed, boolean hasEnriches) {

        // get the field names from the parsed plan combined with the ENRICH match fields from the ENRICH policy
        List<LogicalPlan> inlinestats = parsed.collect(InlineStats.class::isInstance);
        Set<Aggregate> inlinestatsAggs = new HashSet<>();
        for (LogicalPlan i : inlinestats) {
            inlinestatsAggs.add(((InlineStats) i).aggregate());
        }

        if (false == parsed.anyMatch(p -> shouldCollectReferencedFields(p, inlinestatsAggs))) {
            // no explicit columns selection, for example "from employees"
            // also, inlinestats only adds columns to the existent output, its Aggregate shouldn't interfere with potentially using "*"
            return new PreAnalysisResult(IndexResolver.ALL_FIELDS, Set.of());
        }

        Holder<Boolean> projectAll = new Holder<>(false);
        parsed.forEachExpressionDown(UnresolvedStar.class, us -> {// explicit "*" fields selection
            if (projectAll.get()) {
                return;
            }
            projectAll.set(true);
        });

        if (projectAll.get()) {
            return new PreAnalysisResult(IndexResolver.ALL_FIELDS, Set.of());
        }

        var referencesBuilder = new Holder<>(AttributeSet.builder());
        // "keep" and "drop" attributes are special whenever a wildcard is used in their name, as the wildcard can cover some
        // attributes ("lookup join" generated columns among others); steps like removal of Aliases should ignore fields matching the
        // wildcards.
        //
        // E.g. "from test | eval lang = languages + 1 | keep *l" should consider both "languages" and "*l" as valid fields to ask for
        // "from test | eval first_name = 1 | drop first_name | drop *name" should also consider "*name" as valid field to ask for
        //
        // NOTE: the grammar allows wildcards to be used in other commands as well, but these are forbidden in the LogicalPlanBuilder
        // Except in KEEP and DROP.
        var keepRefs = AttributeSet.builder();
        var currentBranchKeepRefs = new Holder<>(AttributeSet.builder());
        var dropWildcardRefs = AttributeSet.builder();
        // fields required to request for lookup joins to work
        var joinRefs = AttributeSet.builder();
        // lookup indices where we request "*" because we may require all their fields
        Set<String> wildcardJoinIndices = new java.util.HashSet<>();

        var canRemoveAliases = new Holder<>(true);

        var forEachDownProcessor = new Holder<BiConsumer<LogicalPlan, Holder<Boolean>>>();
        Holder<LogicalPlan> lastSeenFork = new Holder<>(null);
        // Track if there are plans after FORK that reduce columns to a known set (e.g., Project, Aggregate)
        Holder<Boolean> reduceColumnsAfterFork = new Holder<>(false);
        forEachDownProcessor.set((LogicalPlan p, Holder<Boolean> breakEarly) -> {// go over each plan top-down
            // Check if we see a column-reducing plan before encountering a Fork
            if (lastSeenFork.get() == null && shouldCollectReferencedFields(p, inlinestatsAggs)) {
                reduceColumnsAfterFork.set(true);
            }

            if (p instanceof Fork fork) {
                lastSeenFork.set(fork);

                // Early return from forEachDown. We will iterate over the children manually and end the recursion via forEachDown early.
                var forkRefsResult = AttributeSet.builder();
                forkRefsResult.addAll(referencesBuilder.get());
                var parentKeepRefs = AttributeSet.builder();
                parentKeepRefs.addAll(keepRefs);

                for (var forkBranch : fork.children()) {
                    // Reset branch-specific state for each fork branch
                    currentBranchKeepRefs.set(AttributeSet.builder());
                    currentBranchKeepRefs.get().addAll(parentKeepRefs);
                    referencesBuilder.set(AttributeSet.builder());

                    var isNestedFork = forkBranch.forEachDownMayReturnEarly(forEachDownProcessor.get());

                    // This assert is just for good measure. FORKs within FORKs is yet not supported.
                    LogicalPlan lastFork = lastSeenFork.get();
                    if (lastFork != null && fork instanceof UnionAll == false && lastFork instanceof UnionAll == false) {
                        // UnionAll is a special case of FORK, fork inside subquery or fork after subquery or nested subqueries can
                        // be flattened and supported by LogicalPlanOptimizer and ComputeService in the future, defer this assertion
                        // LogicalPlanOptimizer verifier. Add the check here to avoid assertion on subqueries nested with fork.
                        // TODO consider deferring the nested fork check to Analyzer verifier or LogicalPlanOptimizer verifier.
                        assert isNestedFork == false : "Nested FORKs are not yet supported";
                    }

                    // Determine if this fork branch requires all fields from the index (projectAll = true).
                    // This happens when a branch has no explicit field selection and no KEEP constraints.
                    //
                    // We trigger projectAll when ALL the following conditions are met:
                    // 1. No KEEP commands in this branch (currentBranchKeepRefs is empty)
                    // 2. AND either:
                    // a) No field references were collected (referencesBuilder is empty), OR
                    // b) The branch contains no commands that require explicit field collection
                    // (e.g., no PROJECT or STATS commands that would limit field selection)
                    //
                    // Examples:
                    // - "fork (where true) (where a is not null)" → needs all fields (no KEEP, only filters)
                    // - "fork (eval x = 1 | keep x) (where true)" → needs all fields (second branch has no KEEP)
                    // UNLESS there's a column-reducing command after FORK (e.g., stats)
                    // - "fork (eval x = 1 | keep x) (eval y = 2 | keep y)" → specific fields only (both branches have KEEP)
                    // - "fork (eval x = 1 | keep x) (where true) | stats c = count(*)" → specific fields (stats reduces columns)
                    if (currentBranchKeepRefs.get().isEmpty()
                        && (referencesBuilder.get().isEmpty()
                            || false == forkBranch.anyMatch(forkPlan -> shouldCollectReferencedFields(forkPlan, inlinestatsAggs)))
                        && false == reduceColumnsAfterFork.get()) {
                        projectAll.set(true);
                        // Return early, we'll be returning all references no matter what the remainder of the query is.
                        breakEarly.set(true);
                        return;
                    }
                    forkRefsResult.addAll(referencesBuilder.get());
                }

                forkRefsResult.removeIf(attr -> attr.name().equals(Fork.FORK_FIELD));
                referencesBuilder.set(forkRefsResult);

                // Return early, we've already explored all fork branches.
                breakEarly.set(true);
                return;
            } else if (p instanceof RegexExtract re) { // for Grok and Dissect
                // keep the inputs needed by Grok/Dissect
                referencesBuilder.get().addAll(re.input().references());
            } else if (p instanceof CompoundOutputEval<?> coe) {
                // keep the input field needed by the CompoundOutputEval
                referencesBuilder.get().addAll(coe.getInput().references());
            } else if (p instanceof Enrich enrich) {
                AttributeSet enrichFieldRefs = Expressions.references(enrich.enrichFields());
                AttributeSet.Builder enrichRefs = enrichFieldRefs.combine(enrich.matchField().references()).asBuilder();
                // Enrich adds an EmptyAttribute if no match field is specified
                // The exact name of the field will be added later as part of enrichPolicyMatchFields Set
                enrichRefs.removeIf(attr -> attr instanceof EmptyAttribute);
                referencesBuilder.get().addAll(enrichRefs);
            } else if (p instanceof LookupJoin join) {
                joinRefs.addAll(join.config().leftFields());
                if (join.config().joinOnConditions() != null) {
                    joinRefs.addAll(join.config().joinOnConditions().references());
                }
                if (keepRefs.isEmpty()) {
                    // No KEEP commands after the JOIN, so we need to mark this index for "*" field resolution
                    wildcardJoinIndices.add(((UnresolvedRelation) join.right()).indexPattern().indexPattern());
                } else {
                    // Keep commands can reference the join columns with names that shadow aliases, so we block their removal
                    joinRefs.addAll(keepRefs);
                }
            } else {
                referencesBuilder.get().addAll(p.references());
                if (p instanceof UnresolvedRelation ur && ur.isTimeSeriesMode()) {
                    // METRICS aggs generally rely on @timestamp without the user having to mention it.
                    referencesBuilder.get().add(UnresolvedTimestamp.withSource(ur.source()));
                }

                p.forEachExpression(UnresolvedFunction.class, uf -> {
                    if (FUNCTIONS_REQUIRING_TIMESTAMP.contains(uf.name().toLowerCase(Locale.ROOT))) {
                        referencesBuilder.get().add(UnresolvedTimestamp.withSource(uf.source()));
                    }
                });

                // special handling for UnresolvedPattern (which is not an UnresolvedAttribute)
                p.forEachExpression(UnresolvedNamePattern.class, up -> {
                    var ua = new UnresolvedPattern(up.source(), up.name());
                    referencesBuilder.get().add(ua);
                    if (p instanceof Keep) {
                        keepRefs.add(ua);
                        currentBranchKeepRefs.get().add(ua);
                    } else if (p instanceof Drop) {
                        dropWildcardRefs.add(ua);
                    } else {
                        throw new IllegalStateException("Only KEEP and DROP should allow wildcards");
                    }
                });
                if (p instanceof Keep) {
                    keepRefs.addAll(p.references());
                    currentBranchKeepRefs.get().addAll(p.references());
                }
            }

            // If the current node in the tree is of type JOIN (lookup join, inlinestats) or ENRICH or other type of
            // command that we may add in the future which can override already defined Aliases with EVAL
            // (for example
            //
            // from test
            // | eval ip = 123
            // | enrich ips_policy ON hostname
            // | rename ip AS my_ip
            //
            // and ips_policy enriches the results with the same name ip field),
            // these aliases should be kept in the list of fields.
            if (canRemoveAliases.get() && p.anyMatch(FieldNameUtils::couldOverrideAliases)) {
                canRemoveAliases.set(false);
            }
            if (canRemoveAliases.get()) {
                // remove any already discovered UnresolvedAttributes that are in fact aliases defined later down in the tree
                // for example "from test | eval x = salary | stats max = max(x) by gender"
                // remove the UnresolvedAttribute "x", since that is an Alias defined in "eval"
                // also remove other down-the-tree references to the extracted fields from "grok" and "dissect"
                AttributeSet planRefs = p.references();
                Set<String> fieldNames = planRefs.names();
                p.forEachExpressionDown(NamedExpression.class, ne -> {
                    if ((ne instanceof Alias || ne instanceof ReferenceAttribute) == false) {
                        return;
                    }
                    // do not remove the UnresolvedAttribute that has the same name as its alias, ie "rename id AS id"
                    // or the UnresolvedAttributes that are used in Functions that have aliases "STATS id = MAX(id)"
                    if (fieldNames.contains(ne.name())) {
                        return;
                    }
                    referencesBuilder.get()
                        .removeIf(attr -> matchByName(attr, ne.name(), keepRefs.contains(attr) || dropWildcardRefs.contains(attr)));
                });
            }
        });
        parsed.forEachDownMayReturnEarly(forEachDownProcessor.get());

        if (projectAll.get()) {
            return new PreAnalysisResult(IndexResolver.ALL_FIELDS, Set.of());
        }

        // Add JOIN ON column references afterward to avoid Alias removal
        referencesBuilder.get().addAll(joinRefs);
        // If any JOIN commands need wildcard field-caps calls, persist the index names

        // remove valid metadata attributes because they will be filtered out by the IndexResolver anyway
        // otherwise, in some edge cases, we will fail to ask for "*" (all fields) instead
        referencesBuilder.get().removeIf(a -> a instanceof MetadataAttribute || MetadataAttribute.isSupported(a.name()));
        Set<String> fieldNames = referencesBuilder.get().build().names();

        if (hasEnriches) {
            // we do not know names of the enrich policy match fields beforehand. We need to resolve all fields in this case
            return new PreAnalysisResult(IndexResolver.ALL_FIELDS, wildcardJoinIndices);
        } else if (fieldNames.isEmpty()) {
            // there cannot be an empty list of fields, we'll ask the simplest and lightest one instead: _index
            return new PreAnalysisResult(IndexResolver.INDEX_METADATA_FIELD, wildcardJoinIndices);
        } else {
            HashSet<String> allFields = new HashSet<>(fieldNames.stream().flatMap(FieldNameUtils::withSubfields).collect(toSet()));
            allFields.add(MetadataAttribute.INDEX);
            return new PreAnalysisResult(allFields, wildcardJoinIndices);
        }
    }

    private static Stream<String> withSubfields(String name) {
        return name.endsWith(WILDCARD) ? Stream.of(name) : Stream.of(name, name + ".*");
    }

    /**
     * Indicates whether the given plan gives an exact list of fields that we need to collect from field_caps.
     */
    private static boolean shouldCollectReferencedFields(LogicalPlan plan, Set<Aggregate> inlinestatsAggs) {
        return plan instanceof Project || (plan instanceof Aggregate agg && inlinestatsAggs.contains(agg) == false);
    }

    /**
     * Could a plan "accidentally" override aliases?
     * Examples are JOIN and ENRICH, that _could_ produce fields with the same
     * name of an existing alias, based on their index mapping.
     * Here we just have to consider commands where this information is not available before index resolution,
     * eg. EVAL, GROK, DISSECT can override an alias, but we know it in advance, ie. we don't need to resolve indices to know.
     */
    private static boolean couldOverrideAliases(LogicalPlan p) {
        return (p instanceof Aggregate
            || p instanceof Completion
            || p instanceof Drop
            || p instanceof Eval
            || p instanceof Filter
            || p instanceof Fork
            || p instanceof InlineStats
            || p instanceof Insist
            || p instanceof Keep
            || p instanceof Limit
            || p instanceof MvExpand
            || p instanceof OrderBy
            || p instanceof Project
            || p instanceof RegexExtract
            || p instanceof CompoundOutputEval<?>
            || p instanceof Rename
            || p instanceof TopN
            || p instanceof UnresolvedRelation) == false;
    }

    private static boolean matchByName(Attribute attr, String other, boolean skipIfPattern) {
        boolean isPattern = Regex.isSimpleMatchPattern(attr.name());
        if (skipIfPattern && isPattern) {
            return false;
        }
        var name = attr.name();
        return isPattern ? Regex.simpleMatch(name, other) : name.equals(other);
    }
}
