/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolution;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsqlUnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.stats.FeatureMetric;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.analyzer.AnalyzerRules;
import org.elasticsearch.xpack.ql.analyzer.AnalyzerRules.BaseAnalyzerRule;
import org.elasticsearch.xpack.ql.analyzer.AnalyzerRules.ParameterizedAnalyzerRule;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.EmptyAttribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedStar;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.rule.ParameterizedRule;
import org.elasticsearch.xpack.ql.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.ql.rule.Rule;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.InvalidMappedField;
import org.elasticsearch.xpack.ql.type.UnsupportedEsField;
import org.elasticsearch.xpack.ql.util.CollectionUtils;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.stats.FeatureMetric.LIMIT;
import static org.elasticsearch.xpack.ql.analyzer.AnalyzerRules.resolveFunction;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.NESTED;

public class Analyzer extends ParameterizedRuleExecutor<LogicalPlan, AnalyzerContext> {
    static final List<Attribute> NO_FIELDS = List.of(
        new ReferenceAttribute(Source.EMPTY, "<no-fields>", DataTypes.NULL, null, Nullability.TRUE, null, false)
    );
    private static final Iterable<RuleExecutor.Batch<LogicalPlan>> rules;

    static {
        var resolution = new Batch<>(
            "Resolution",
            new ResolveTable(),
            new ResolveEnrich(),
            new ResolveRefs(),
            new ResolveFunctions(),
            new RemoveDuplicateProjections()
        );
        var finish = new Batch<>("Finish Analysis", Limiter.ONCE, new AddImplicitLimit(), new PromoteStringsInDateComparisons());
        rules = List.of(resolution, finish);
    }

    private final Verifier verifier;

    public Analyzer(AnalyzerContext context, Verifier verifier) {
        super(context);
        this.verifier = verifier;
    }

    public LogicalPlan analyze(LogicalPlan plan) {
        BitSet partialMetrics = new BitSet(FeatureMetric.values().length);
        return verify(execute(plan), gatherPreAnalysisMetrics(plan, partialMetrics));
    }

    public LogicalPlan verify(LogicalPlan plan, BitSet partialMetrics) {
        Collection<Failure> failures = verifier.verify(plan, partialMetrics);
        if (failures.isEmpty() == false) {
            throw new VerificationException(failures);
        }
        return plan;
    }

    @Override
    protected Iterable<RuleExecutor.Batch<LogicalPlan>> batches() {
        return rules;
    }

    private static class ResolveTable extends ParameterizedAnalyzerRule<EsqlUnresolvedRelation, AnalyzerContext> {

        @Override
        protected LogicalPlan rule(EsqlUnresolvedRelation plan, AnalyzerContext context) {
            if (context.indexResolution().isValid() == false) {
                return plan.unresolvedMessage().equals(context.indexResolution().toString())
                    ? plan
                    : new EsqlUnresolvedRelation(plan.source(), plan.table(), plan.metadataFields(), context.indexResolution().toString());
            }
            TableIdentifier table = plan.table();
            if (context.indexResolution().matches(table.index()) == false) {
                new EsqlUnresolvedRelation(
                    plan.source(),
                    plan.table(),
                    plan.metadataFields(),
                    "invalid [" + table + "] resolution to [" + context.indexResolution() + "]"
                );
            }

            EsIndex esIndex = context.indexResolution().get();
            var attributes = mappingAsAttributes(plan.source(), esIndex.mapping());
            attributes.addAll(plan.metadataFields());
            return new EsRelation(plan.source(), esIndex, attributes.isEmpty() ? NO_FIELDS : attributes);
        }

    }

    /**
     * Specific flattening method, different from the default EsRelation that:
     * 1. takes care of data type widening (for certain types)
     * 2. drops the object and keyword hierarchy
     */
    private static List<Attribute> mappingAsAttributes(Source source, Map<String, EsField> mapping) {
        var list = new ArrayList<Attribute>();
        mappingAsAttributes(list, source, null, mapping);
        list.sort(Comparator.comparing(Attribute::qualifiedName));
        return list;
    }

    private static void mappingAsAttributes(List<Attribute> list, Source source, String parentName, Map<String, EsField> mapping) {
        for (Map.Entry<String, EsField> entry : mapping.entrySet()) {
            String name = entry.getKey();
            EsField t = entry.getValue();

            if (t != null) {
                name = parentName == null ? name : parentName + "." + name;
                var fieldProperties = t.getProperties();
                // widen the data type
                var type = EsqlDataTypes.widenSmallNumericTypes(t.getDataType());
                // due to a bug also copy the field since the Attribute hierarchy extracts the data type
                // directly even if the data type is passed explicitly
                if (type != t.getDataType()) {
                    t = new EsField(t.getName(), type, t.getProperties(), t.isAggregatable(), t.isAlias());
                }

                // primitive branch
                if (EsqlDataTypes.isPrimitive(type)) {
                    Attribute attribute;
                    if (t instanceof UnsupportedEsField uef) {
                        attribute = new UnsupportedAttribute(source, name, uef);
                    } else {
                        attribute = new FieldAttribute(source, null, name, t);
                    }
                    list.add(attribute);
                }
                // allow compound object even if they are unknown (but not NESTED)
                if (type != NESTED && fieldProperties.isEmpty() == false) {
                    mappingAsAttributes(list, source, name, fieldProperties);
                }
            }
        }
    }

    private static class ResolveEnrich extends ParameterizedAnalyzerRule<Enrich, AnalyzerContext> {

        @Override
        protected LogicalPlan rule(Enrich plan, AnalyzerContext context) {
            if (plan.policyName().resolved() == false) {
                // the policy does not exist
                return plan;
            }
            String policyName = (String) plan.policyName().fold();
            EnrichPolicyResolution policyRes = context.enrichResolution()
                .resolvedPolicies()
                .stream()
                .filter(x -> x.policyName().equals(policyName))
                .findFirst()
                .orElse(new EnrichPolicyResolution(policyName, null, null));

            IndexResolution idx = policyRes.index();
            EnrichPolicy policy = policyRes.policy();

            var policyNameExp = policy == null || idx == null
                ? new UnresolvedAttribute(
                    plan.policyName().source(),
                    policyName,
                    null,
                    unresolvedPolicyError(policyName, context.enrichResolution())
                )
                : plan.policyName();

            var matchField = policy != null && (plan.matchField() == null || plan.matchField() instanceof EmptyAttribute)
                ? new UnresolvedAttribute(plan.source(), policy.getMatchField())
                : plan.matchField();

            List<NamedExpression> enrichFields = policy == null || idx == null
                ? (plan.enrichFields() == null ? List.of() : plan.enrichFields())
                : calculateEnrichFields(
                    plan.source(),
                    policyName,
                    mappingAsAttributes(plan.source(), idx.get().mapping()),
                    plan.enrichFields(),
                    policy
                );

            return new Enrich(plan.source(), plan.child(), plan.mode(), policyNameExp, matchField, policyRes, enrichFields);
        }

        private String unresolvedPolicyError(String policyName, EnrichResolution enrichResolution) {
            List<String> potentialMatches = StringUtils.findSimilar(policyName, enrichResolution.existingPolicies());
            String msg = "unresolved enrich policy [" + policyName + "]";
            if (CollectionUtils.isEmpty(potentialMatches) == false) {
                msg += ", did you mean "
                    + (potentialMatches.size() == 1 ? "[" + potentialMatches.get(0) + "]" : "any of " + potentialMatches)
                    + "?";
            }
            return msg;
        }

        public static List<NamedExpression> calculateEnrichFields(
            Source source,
            String policyName,
            List<Attribute> mapping,
            List<NamedExpression> enrichFields,
            EnrichPolicy policy
        ) {
            Set<String> policyEnrichFieldSet = new HashSet<>(policy.getEnrichFields());
            Map<String, Attribute> fieldMap = mapping.stream()
                .filter(e -> policyEnrichFieldSet.contains(e.name()))
                .collect(Collectors.toMap(NamedExpression::name, Function.identity()));
            List<NamedExpression> result = new ArrayList<>();
            if (enrichFields == null || enrichFields.isEmpty()) {
                // use the policy to infer the enrich fields
                for (String enrichFieldName : policy.getEnrichFields()) {
                    result.add(createEnrichFieldExpression(source, policyName, fieldMap, enrichFieldName));
                }
            } else {
                for (NamedExpression enrichField : enrichFields) {
                    String enrichFieldName = Expressions.name(enrichField instanceof Alias a ? a.child() : enrichField);
                    NamedExpression field = createEnrichFieldExpression(source, policyName, fieldMap, enrichFieldName);
                    result.add(enrichField instanceof Alias a ? new Alias(a.source(), a.name(), field) : field);
                }
            }
            return result;
        }

        private static NamedExpression createEnrichFieldExpression(
            Source source,
            String policyName,
            Map<String, Attribute> fieldMap,
            String enrichFieldName
        ) {
            Attribute mappedField = fieldMap.get(enrichFieldName);
            if (mappedField == null) {
                String msg = "Enrich field [" + enrichFieldName + "] not found in enrich policy [" + policyName + "]";
                List<String> similar = StringUtils.findSimilar(enrichFieldName, fieldMap.keySet());
                if (CollectionUtils.isEmpty(similar) == false) {
                    msg += ", did you mean " + (similar.size() == 1 ? "[" + similar.get(0) + "]" : "any of " + similar) + "?";
                }
                return new UnresolvedAttribute(source, enrichFieldName, null, msg);
            } else {
                return new ReferenceAttribute(source, enrichFieldName, mappedField.dataType(), null, Nullability.TRUE, null, false);
            }
        }
    }

    private static class ResolveRefs extends BaseAnalyzerRule {

        @Override
        protected LogicalPlan doRule(LogicalPlan plan) {
            final List<Attribute> childrenOutput = new ArrayList<>();

            for (LogicalPlan child : plan.children()) {
                var output = child.output();
                childrenOutput.addAll(output);
            }

            if (plan instanceof Drop d) {
                return resolveDrop(d, childrenOutput);
            }

            if (plan instanceof Rename r) {
                return resolveRename(r, childrenOutput);
            }

            if (plan instanceof Keep p) {
                return resolveKeep(p, childrenOutput);
            }

            if (plan instanceof Eval p) {
                return resolveEval(p, childrenOutput);
            }

            if (plan instanceof Enrich p) {
                return resolveEnrich(p, childrenOutput);
            }

            if (plan instanceof MvExpand p) {
                return resolveMvExpand(p, childrenOutput);
            }

            return plan.transformExpressionsUp(UnresolvedAttribute.class, ua -> maybeResolveAttribute(ua, childrenOutput));
        }

        private LogicalPlan resolveMvExpand(MvExpand p, List<Attribute> childrenOutput) {
            if (p.target() instanceof UnresolvedAttribute ua) {
                Attribute resolved = maybeResolveAttribute(ua, childrenOutput);
                if (resolved == ua) {
                    return p;
                }
                return new MvExpand(
                    p.source(),
                    p.child(),
                    resolved,
                    resolved.resolved()
                        ? new ReferenceAttribute(
                            resolved.source(),
                            resolved.name(),
                            resolved.dataType(),
                            null,
                            resolved.nullable(),
                            null,
                            false
                        )
                        : resolved
                );
            }
            return p;
        }

        private Attribute maybeResolveAttribute(UnresolvedAttribute ua, List<Attribute> childrenOutput) {
            if (ua.customMessage()) {
                return ua;
            }
            return resolveAttribute(ua, childrenOutput);
        }

        private Attribute resolveAttribute(UnresolvedAttribute ua, List<Attribute> childrenOutput) {
            Attribute resolved = ua;
            var named = resolveAgainstList(ua, childrenOutput);
            // if resolved, return it; otherwise keep it in place to be resolved later
            if (named.size() == 1) {
                resolved = named.get(0);
                if (log.isTraceEnabled() && resolved.resolved()) {
                    log.trace("Resolved {} to {}", ua, resolved);
                }
            } else {
                if (named.size() > 0) {
                    resolved = ua.withUnresolvedMessage("Resolved [" + ua + "] unexpectedly to multiple attributes " + named);
                }
            }
            return resolved;
        }

        private LogicalPlan resolveEval(Eval eval, List<Attribute> childOutput) {
            List<Attribute> allResolvedInputs = new ArrayList<>(childOutput);
            List<Alias> newFields = new ArrayList<>();
            boolean changed = false;
            for (Alias field : eval.fields()) {
                Alias result = (Alias) field.transformUp(UnresolvedAttribute.class, ua -> resolveAttribute(ua, allResolvedInputs));

                changed |= result != field;
                newFields.add(result);

                if (result.resolved()) {
                    // for proper resolution, duplicate attribute names are problematic, only last occurrence matters
                    Attribute existing = allResolvedInputs.stream()
                        .filter(attr -> attr.name().equals(result.name()))
                        .findFirst()
                        .orElse(null);
                    if (existing != null) {
                        allResolvedInputs.remove(existing);
                    }
                    allResolvedInputs.add(result.toAttribute());
                }
            }
            return changed ? new Eval(eval.source(), eval.child(), newFields) : eval;
        }

        /**
         * resolve each item manually.
         *
         * Fields are added in the order they appear.
         *
         * If one field matches multiple expressions, the following precedence rules apply (higher to lower):
         * 1. complete field name (ie. no wildcards)
         * 2. partial wildcard expressions (eg. fieldNam*)
         * 3. wildcard only (ie. *)
         *
         * If a field name matches multiple expressions with the same precedence, last one is used.
         *
         * A few examples below:
         *
         * // full name
         * row foo = 1, bar = 2 | keep foo, bar, foo   ->  bar, foo
         *
         * // the full name has precedence on wildcard expression
         * row foo = 1, bar = 2 | keep foo, bar, foo*   ->  foo, bar
         *
         * // the two wildcard expressions have the same priority, even though the first one is more specific
         * // so last one wins
         * row foo = 1, bar = 2 | keep foo*, bar, fo*   ->  bar, foo
         *
         * // * has the lowest priority
         * row foo = 1, bar = 2 | keep *, foo   ->  bar, foo
         * row foo = 1, bar = 2 | keep foo, *   ->  foo, bar
         * row foo = 1, bar = 2 | keep bar*, foo, *   ->  bar, foo
         *
         *
         * @param p
         * @param childOutput
         * @return
         */
        private LogicalPlan resolveKeep(Project p, List<Attribute> childOutput) {
            List<NamedExpression> resolvedProjections = new ArrayList<>();
            var projections = p.projections();
            // start with projections

            // no projection specified or just *
            if (projections.isEmpty() || (projections.size() == 1 && projections.get(0) instanceof UnresolvedStar)) {
                resolvedProjections.addAll(childOutput);
            }
            // otherwise resolve them
            else {
                Map<NamedExpression, Integer> priorities = new LinkedHashMap<>();
                for (var proj : projections) {
                    final List<Attribute> resolved;
                    final int priority;
                    if (proj instanceof UnresolvedStar) {
                        resolved = childOutput;
                        priority = 2;
                    } else if (proj instanceof UnresolvedAttribute ua) {
                        resolved = resolveAgainstList(ua, childOutput);
                        priority = Regex.isSimpleMatchPattern(ua.name()) ? 1 : 0;
                    } else {
                        assert false : "unexpected projection: " + proj;
                        throw new IllegalStateException("unexpected projection: " + proj);
                    }
                    for (Attribute attr : resolved) {
                        Integer previousPrio = priorities.get(attr);
                        if (previousPrio == null || previousPrio >= priority) {
                            priorities.remove(attr);
                            priorities.put(attr, priority);
                        }
                    }
                }
                resolvedProjections = new ArrayList<>(priorities.keySet());
            }

            return new EsqlProject(p.source(), p.child(), resolvedProjections);
        }

        private LogicalPlan resolveDrop(Drop drop, List<Attribute> childOutput) {
            List<NamedExpression> resolvedProjections = new ArrayList<>(childOutput);

            for (var ne : drop.removals()) {
                var resolved = ne instanceof UnresolvedAttribute ua ? resolveAgainstList(ua, childOutput) : singletonList(ne);
                // the return list might contain either resolved elements or unresolved ones.
                // if things are resolved, remove them - if not add them to the list to trip the Verifier;
                // thus make sure to remove the intersection but add the unresolved difference (if any).
                // so, remove things that are in common
                resolvedProjections.removeIf(resolved::contains);
                // but add non-projected, unresolved extras to later trip the Verifier.
                resolved.forEach(r -> {
                    if (r.resolved() == false && r instanceof UnsupportedAttribute == false) {
                        resolvedProjections.add(r);
                    }
                });
            }

            return new EsqlProject(drop.source(), drop.child(), resolvedProjections);
        }

        private LogicalPlan resolveRename(Rename rename, List<Attribute> childrenOutput) {
            List<NamedExpression> projections = new ArrayList<>(childrenOutput);

            int renamingsCount = rename.renamings().size();
            List<NamedExpression> unresolved = new ArrayList<>(renamingsCount);
            Map<String, String> reverseAliasing = new HashMap<>(renamingsCount); // `| rename a as x` => map(a: x)

            rename.renamings().forEach(alias -> {
                // skip NOPs: `| rename a as a`
                if (alias.child() instanceof UnresolvedAttribute ua && alias.name().equals(ua.name()) == false) {
                    // remove attributes overwritten by a renaming: `| keep a, b, c | rename a as b`
                    projections.removeIf(x -> x.name().equals(alias.name()));

                    var resolved = maybeResolveAttribute(ua, childrenOutput);
                    if (resolved instanceof UnsupportedAttribute || resolved.resolved()) {
                        var realiased = (NamedExpression) alias.replaceChildren(List.of(resolved));
                        projections.replaceAll(x -> x.equals(resolved) ? realiased : x);
                        childrenOutput.removeIf(x -> x.equals(resolved));
                        reverseAliasing.put(resolved.name(), alias.name());
                    } else { // remained UnresolvedAttribute
                        // is the current alias referencing a previously declared alias?
                        boolean updated = false;
                        if (reverseAliasing.containsValue(resolved.name())) {
                            for (var li = projections.listIterator(); li.hasNext();) {
                                // does alias still exist? i.e. it hasn't been renamed again (`| rename a as b, b as c, b as d`)
                                if (li.next() instanceof Alias a && a.name().equals(resolved.name())) {
                                    reverseAliasing.put(resolved.name(), alias.name());
                                    // update aliased projection in place
                                    li.set((NamedExpression) alias.replaceChildren(a.children()));
                                    updated = true;
                                    break;
                                }
                            }
                        }
                        if (updated == false) {
                            var u = resolved;
                            var previousAliasName = reverseAliasing.get(resolved.name());
                            if (previousAliasName != null) {
                                String message = format(
                                    null,
                                    "Column [{}] renamed to [{}] and is no longer available [{}]",
                                    resolved.name(),
                                    previousAliasName,
                                    alias.sourceText()
                                );
                                u = ua.withUnresolvedMessage(message);
                            }
                            unresolved.add(u);
                        }
                    }
                }
            });

            // add unresolved renamings to later trip the Verifier.
            projections.addAll(unresolved);

            return new EsqlProject(rename.source(), rename.child(), projections);
        }

        private LogicalPlan resolveEnrich(Enrich enrich, List<Attribute> childrenOutput) {

            if (enrich.matchField().toAttribute() instanceof UnresolvedAttribute ua) {
                Attribute resolved = maybeResolveAttribute(ua, childrenOutput);
                if (resolved.equals(ua)) {
                    return enrich;
                }
                if (resolved.resolved() && resolved.dataType() != KEYWORD) {
                    resolved = ua.withUnresolvedMessage(
                        "Unsupported type [" + resolved.dataType() + "] for enrich matching field [" + ua.name() + "]; only KEYWORD allowed"
                    );
                }
                return new Enrich(
                    enrich.source(),
                    enrich.child(),
                    enrich.mode(),
                    enrich.policyName(),
                    resolved,
                    enrich.policy(),
                    enrich.enrichFields()
                );
            }
            return enrich;
        }
    }

    private static List<Attribute> resolveAgainstList(UnresolvedAttribute u, Collection<Attribute> attrList) {
        var matches = AnalyzerRules.maybeResolveAgainstList(u, attrList, false, true, Analyzer::handleSpecialFields);

        // none found - add error message
        if (matches.isEmpty()) {
            UnresolvedAttribute unresolved;
            var name = u.name();
            if (Regex.isSimpleMatchPattern(name)) {
                unresolved = u.withUnresolvedMessage(format(null, "No match found for [{}]", name));
            } else {
                Set<String> names = new HashSet<>(attrList.size());
                for (var a : attrList) {
                    String nameCandidate = a.name();
                    if (EsqlDataTypes.isPrimitive(a.dataType())) {
                        names.add(nameCandidate);
                    }
                }
                unresolved = u.withUnresolvedMessage(UnresolvedAttribute.errorMessage(name, StringUtils.findSimilar(name, names)));
            }
            return singletonList(unresolved);
        }

        return matches;
    }

    private static Attribute handleSpecialFields(UnresolvedAttribute u, Attribute named) {
        if (named instanceof FieldAttribute fa) {
            // incompatible mappings
            var field = fa.field();
            if (field instanceof InvalidMappedField imf) {
                named = u.withUnresolvedMessage("Cannot use field [" + fa.name() + "] due to ambiguities being " + imf.errorMessage());
            }
        }

        return named;
    }

    private static class ResolveFunctions extends ParameterizedAnalyzerRule<LogicalPlan, AnalyzerContext> {

        @Override
        protected LogicalPlan rule(LogicalPlan plan, AnalyzerContext context) {
            return plan.transformExpressionsUp(
                UnresolvedFunction.class,
                uf -> resolveFunction(uf, context.configuration(), context.functionRegistry())
            );
        }
    }

    /**
     * Rule that removes duplicate projects - this is done as a separate rule to allow
     * full validation of the node before looking at the duplication.
     * The duplication needs to be addressed to avoid ambiguity errors from commands further down
     * the line.
     */
    private static class RemoveDuplicateProjections extends BaseAnalyzerRule {

        @Override
        protected boolean skipResolved() {
            return false;
        }

        @Override
        protected LogicalPlan doRule(LogicalPlan plan) {
            if (plan.resolved()) {
                if (plan instanceof Aggregate agg) {
                    plan = removeAggDuplicates(agg);
                }
            }
            return plan;
        }

        private static LogicalPlan removeAggDuplicates(Aggregate agg) {
            var groupings = agg.groupings();
            var newGroupings = new LinkedHashSet<>(groupings);
            // reuse existing objects
            groupings = newGroupings.size() == groupings.size() ? groupings : new ArrayList<>(newGroupings);

            var aggregates = agg.aggregates();
            var newAggregates = new ArrayList<>(aggregates);
            var nameSet = Sets.newHashSetWithExpectedSize(newAggregates.size());
            // remove duplicates in reverse to preserve the last one appearing
            for (int i = newAggregates.size() - 1; i >= 0; i--) {
                var aggregate = newAggregates.get(i);
                if (nameSet.add(aggregate.name()) == false) {
                    newAggregates.remove(i);
                }
            }
            // reuse existing objects
            aggregates = newAggregates.size() == aggregates.size() ? aggregates : newAggregates;
            // replace aggregate if needed
            agg = (groupings == agg.groupings() && newAggregates == agg.aggregates())
                ? agg
                : new Aggregate(agg.source(), agg.child(), groupings, aggregates);
            return agg;
        }
    }

    private static class AddImplicitLimit extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {
        @Override
        public LogicalPlan apply(LogicalPlan logicalPlan, AnalyzerContext context) {
            List<LogicalPlan> limits = logicalPlan.collectFirstChildren(Limit.class::isInstance);
            int limit;
            if (limits.isEmpty()) {
                HeaderWarning.addWarning(
                    "No limit defined, adding default limit of [{}]",
                    context.configuration().resultTruncationDefaultSize()
                );
                limit = context.configuration().resultTruncationDefaultSize(); // user provided no limit: cap to a default
            } else {
                limit = context.configuration().resultTruncationMaxSize(); // user provided a limit: cap result entries to the max
            }
            return new Limit(Source.EMPTY, new Literal(Source.EMPTY, limit, DataTypes.INTEGER), logicalPlan);
        }
    }

    private static class PromoteStringsInDateComparisons extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            return plan.transformExpressionsUp(BinaryComparison.class, PromoteStringsInDateComparisons::promote);
        }

        private static Expression promote(BinaryComparison cmp) {
            if (cmp.resolved() == false) {
                return cmp;
            }
            var left = cmp.left();
            var right = cmp.right();
            boolean modified = false;
            if (left.dataType() == DATETIME) {
                if (right.dataType() == KEYWORD && right.foldable()) {
                    right = stringToDate(right);
                    modified = true;
                }
            } else {
                if (right.dataType() == DATETIME) {
                    if (left.dataType() == KEYWORD && left.foldable()) {
                        left = stringToDate(left);
                        modified = true;
                    }
                }
            }
            return modified ? cmp.replaceChildren(List.of(left, right)) : cmp;
        }

        private static Expression stringToDate(Expression stringExpression) {
            var str = stringExpression.fold().toString();

            Long millis = null;
            // TODO: better control over this string format - do we want this to be flexible or always redirect folks to use date parsing
            try {
                millis = str == null ? null : DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(str);
            } catch (Exception ex) { // in case of exception, millis will be null which will trigger an error
            }

            var source = stringExpression.source();
            Expression result;
            if (millis == null) {
                var errorMessage = format(null, "Invalid date [{}]", str);
                result = new UnresolvedAttribute(source, source.text(), null, errorMessage);
            } else {
                result = new Literal(source, millis, DATETIME);
            }
            return result;
        }
    }

    private BitSet gatherPreAnalysisMetrics(LogicalPlan plan, BitSet b) {
        // count only the explicit "limit" the user added, otherwise all queries will have a "limit" and telemetry won't reflect reality
        if (plan.collectFirstChildren(Limit.class::isInstance).isEmpty() == false) {
            b.set(LIMIT.ordinal());
        }
        plan.forEachDown(p -> FeatureMetric.set(p, b));
        return b;
    }
}
