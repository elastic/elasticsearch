/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ProjectReorderRenameRemove;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.analyzer.AnalyzerRules;
import org.elasticsearch.xpack.ql.analyzer.AnalyzerRules.BaseAnalyzerRule;
import org.elasticsearch.xpack.ql.analyzer.AnalyzerRules.ParameterizedAnalyzerRule;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedStar;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.ql.rule.ParameterizedRule;
import org.elasticsearch.xpack.ql.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.ql.rule.Rule;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.InvalidMappedField;
import org.elasticsearch.xpack.ql.type.UnsupportedEsField;
import org.elasticsearch.xpack.ql.util.Holder;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.analyzer.AnalyzerRules.resolveFunction;
import static org.elasticsearch.xpack.ql.type.DataTypes.NESTED;

public class Analyzer extends ParameterizedRuleExecutor<LogicalPlan, AnalyzerContext> {
    private static final Iterable<RuleExecutor.Batch<LogicalPlan>> rules;

    static {
        var resolution = new Batch<>(
            "Resolution",
            new ResolveTable(),
            new ResolveRefs(),
            new ResolveFunctions(),
            new RemoveDuplicateProjections()
        );
        var finish = new Batch<>("Finish Analysis", Limiter.ONCE, new AddMissingProjection(), new AddImplicitLimit());
        rules = List.of(resolution, finish);
    }

    private final Verifier verifier;

    public Analyzer(AnalyzerContext context, Verifier verifier) {
        super(context);
        this.verifier = verifier;
    }

    public LogicalPlan analyze(LogicalPlan plan) {
        return verify(execute(plan));
    }

    public LogicalPlan verify(LogicalPlan plan) {
        Collection<Failure> failures = verifier.verify(plan);
        if (failures.isEmpty() == false) {
            throw new VerificationException(failures);
        }
        return plan;
    }

    @Override
    protected Iterable<RuleExecutor.Batch<LogicalPlan>> batches() {
        return rules;
    }

    private static class ResolveTable extends ParameterizedAnalyzerRule<UnresolvedRelation, AnalyzerContext> {

        @Override
        protected LogicalPlan rule(UnresolvedRelation plan, AnalyzerContext context) {
            if (context.indexResolution().isValid() == false) {
                return plan.unresolvedMessage().equals(context.indexResolution().toString())
                    ? plan
                    : new UnresolvedRelation(
                        plan.source(),
                        plan.table(),
                        plan.alias(),
                        plan.frozen(),
                        context.indexResolution().toString()
                    );
            }
            TableIdentifier table = plan.table();
            if (context.indexResolution().matches(table.index()) == false) {
                new UnresolvedRelation(
                    plan.source(),
                    plan.table(),
                    plan.alias(),
                    plan.frozen(),
                    "invalid [" + table + "] resolution to [" + context.indexResolution() + "]"
                );
            }

            EsIndex esIndex = context.indexResolution().get();
            return new EsRelation(plan.source(), esIndex, mappingAsAttributes(plan.source(), esIndex.mapping()));
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
    }

    private static class ResolveRefs extends BaseAnalyzerRule {

        @Override
        protected LogicalPlan doRule(LogicalPlan plan) {
            final List<Attribute> childrenOutput = new ArrayList<>();
            final var lazyNames = new Holder<Set<String>>();

            for (LogicalPlan child : plan.children()) {
                var output = child.output();
                childrenOutput.addAll(output);
            }

            if (plan instanceof ProjectReorderRenameRemove p) {
                return resolveProject(p, childrenOutput);
            }

            if (plan instanceof Eval p) {
                return resolveEval(p, childrenOutput);
            }

            return plan.transformExpressionsUp(UnresolvedAttribute.class, ua -> resolveAttribute(ua, childrenOutput, lazyNames));
        }

        private Expression resolveAttribute(UnresolvedAttribute ua, List<Attribute> childrenOutput, Holder<Set<String>> lazyNames) {
            if (ua.customMessage()) {
                return ua;
            }
            Expression resolved = ua;
            var named = resolveAgainstList(ua, childrenOutput, lazyNames);
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
            final var lazyNames = new Holder<Set<String>>();
            List<NamedExpression> newFields = new ArrayList<>();
            boolean changed = false;
            for (NamedExpression field : eval.fields()) {
                NamedExpression result = (NamedExpression) field.transformUp(
                    UnresolvedAttribute.class,
                    ua -> resolveAttribute(ua, allResolvedInputs, lazyNames)
                );

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

        private LogicalPlan resolveProject(ProjectReorderRenameRemove p, List<Attribute> childOutput) {
            var lazyNames = new Holder<Set<String>>();

            List<NamedExpression> resolvedProjections = new ArrayList<>();
            var projections = p.projections();
            // start with projections

            // no projection specified or just *
            if (projections.isEmpty() || (projections.size() == 1 && projections.get(0) instanceof UnresolvedStar)) {
                resolvedProjections.addAll(childOutput);
            }
            // otherwise resolve them
            else {
                var starPosition = -1; // no star
                // resolve each item manually while paying attention to:
                // 1. name patterns a*, *b, a*b
                // 2. star * - which can only appear once and signifies "everything else" - this will be added at the end
                for (var ne : projections) {
                    if (ne instanceof UnresolvedStar) {
                        starPosition = resolvedProjections.size();
                    } else if (ne instanceof UnresolvedAttribute ua) {
                        resolvedProjections.addAll(resolveAgainstList(ua, childOutput, lazyNames));
                    } else {
                        // if this gets here it means it was already resolved
                        resolvedProjections.add(ne);
                    }
                }
                // compute star if specified and add it to the list
                if (starPosition >= 0) {
                    var remainingProjections = new ArrayList<>(childOutput);
                    remainingProjections.removeAll(resolvedProjections);
                    resolvedProjections.addAll(starPosition, remainingProjections);
                }
            }

            // continue with removals
            for (var ne : p.removals()) {
                var resolved = ne instanceof UnresolvedAttribute ua ? resolveAgainstList(ua, childOutput, lazyNames) : singletonList(ne);

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

            return new EsqlProject(p.source(), p.child(), resolvedProjections);
        }
    }

    public static List<Attribute> resolveAgainstList(UnresolvedAttribute u, Collection<Attribute> attrList, Holder<Set<String>> lazyNames) {
        var matches = AnalyzerRules.maybeResolveAgainstList(u, attrList, false, true, Analyzer::handleSpecialFields);

        // none found - add error message
        if (matches.isEmpty()) {
            UnresolvedAttribute unresolved;
            var name = u.name();
            if (Regex.isSimpleMatchPattern(name)) {
                unresolved = u.withUnresolvedMessage(format(null, "No match found for [{}]", name));
            } else {
                var names = lazyNames.get();
                if (names == null) {
                    names = new HashSet<>(attrList.size());
                    for (var a : attrList) {
                        String nameCandidate = a.name();
                        if (EsqlDataTypes.isPrimitive(a.dataType())) {
                            names.add(nameCandidate);
                        }
                    }
                    lazyNames.set(names);
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

    private static class AddMissingProjection extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            var projections = plan.collect(e -> e instanceof Project || e instanceof Aggregate);
            if (projections.isEmpty()) {
                // TODO: should unsupported fields be filtered?
                plan = new EsqlProject(plan.source(), plan, plan.output());
            }
            return plan;
        }
    }

    private static class AddImplicitLimit extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {
        @Override
        public LogicalPlan apply(LogicalPlan logicalPlan, AnalyzerContext context) {
            return new Limit(
                Source.EMPTY,
                new Literal(Source.EMPTY, context.configuration().resultTruncationMaxSize(), DataTypes.INTEGER),
                logicalPlan
            );
        }
    }
}
