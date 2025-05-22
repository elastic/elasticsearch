/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules.ParameterizedAnalyzerRule;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.EmptyAttribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedStar;
import org.elasticsearch.xpack.esql.core.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Greatest;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Least;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ConvertFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FoldablesConvertFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToUnsignedLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.DateTimeArithmeticOperation;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.ResolvedInference;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SubstituteSurrogateExpressions;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Dedup;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Insist;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Lookup;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.RrfScoreEval;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinType;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.UsingJoinType;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.esql.rule.Rule;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.telemetry.FeatureMetric;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.time.Duration;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.core.enrich.EnrichPolicy.GEO_MATCH_TYPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TIME_DURATION;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.core.type.DataType.isTemporalAmount;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.LIMIT;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.maybeParseTemporalAmount;

/**
 * This class is part of the planner. Resolves references (such as variable and index names) and performs implicit casting.
 */
public class Analyzer extends ParameterizedRuleExecutor<LogicalPlan, AnalyzerContext> {
    // marker list of attributes for plans that do not have any concrete fields to return, but have other computed columns to return
    // ie from test | stats c = count(*)
    public static final String NO_FIELDS_NAME = "<no-fields>";
    public static final List<Attribute> NO_FIELDS = List.of(
        new ReferenceAttribute(Source.EMPTY, NO_FIELDS_NAME, DataType.NULL, Nullability.TRUE, null, true)
    );

    private static final List<Batch<LogicalPlan>> RULES = List.of(
        new Batch<>(
            "Initialize",
            Limiter.ONCE,
            new ResolveTable(),
            new ResolveEnrich(),
            new ResolveInference(),
            new ResolveLookupTables(),
            new ResolveFunctions()
        ),
        new Batch<>(
            "Resolution",
            new ResolveRefs(),
            new ImplicitCasting(),
            new ResolveUnionTypes()  // Must be after ResolveRefs, so union types can be found
        ),
        new Batch<>("Finish Analysis", Limiter.ONCE, new AddImplicitLimit(), new AddImplicitForkLimit(), new UnionTypesCleanup())
    );

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
    protected List<Batch<LogicalPlan>> batches() {
        return RULES;
    }

    private static class ResolveTable extends ParameterizedAnalyzerRule<UnresolvedRelation, AnalyzerContext> {

        @Override
        protected LogicalPlan rule(UnresolvedRelation plan, AnalyzerContext context) {
            return resolveIndex(
                plan,
                plan.indexMode().equals(IndexMode.LOOKUP)
                    ? context.lookupResolution().get(plan.indexPattern().indexPattern())
                    : context.indexResolution()
            );
        }

        private LogicalPlan resolveIndex(UnresolvedRelation plan, IndexResolution indexResolution) {
            if (indexResolution == null || indexResolution.isValid() == false) {
                String indexResolutionMessage = indexResolution == null ? "[none specified]" : indexResolution.toString();
                return plan.unresolvedMessage().equals(indexResolutionMessage)
                    ? plan
                    : new UnresolvedRelation(
                        plan.source(),
                        plan.indexPattern(),
                        plan.frozen(),
                        plan.metadataFields(),
                        plan.indexMode(),
                        indexResolutionMessage,
                        plan.telemetryLabel()
                    );
            }
            IndexPattern table = plan.indexPattern();
            if (indexResolution.matches(table.indexPattern()) == false) {
                // TODO: fix this (and tests), or drop check (seems SQL-inherited, where's also defective)
                new UnresolvedRelation(
                    plan.source(),
                    plan.indexPattern(),
                    plan.frozen(),
                    plan.metadataFields(),
                    plan.indexMode(),
                    "invalid [" + table + "] resolution to [" + indexResolution + "]",
                    plan.telemetryLabel()
                );
            }

            EsIndex esIndex = indexResolution.get();

            var attributes = mappingAsAttributes(plan.source(), esIndex.mapping());
            attributes.addAll(plan.metadataFields());
            return new EsRelation(
                plan.source(),
                esIndex.name(),
                plan.indexMode(),
                esIndex.indexNameWithModes(),
                attributes.isEmpty() ? NO_FIELDS : attributes
            );
        }
    }

    /**
     * Specific flattening method, different from the default EsRelation that:
     * 1. takes care of data type widening (for certain types)
     * 2. drops the object and keyword hierarchy
     * <p>
     *     Public for testing.
     * </p>
     */
    public static List<Attribute> mappingAsAttributes(Source source, Map<String, EsField> mapping) {
        var list = new ArrayList<Attribute>();
        mappingAsAttributes(list, source, null, mapping);
        list.sort(Comparator.comparing(Attribute::name));
        return list;
    }

    private static void mappingAsAttributes(List<Attribute> list, Source source, String parentName, Map<String, EsField> mapping) {
        for (Map.Entry<String, EsField> entry : mapping.entrySet()) {
            String name = entry.getKey();
            EsField t = entry.getValue();

            if (t != null) {
                name = parentName == null ? name : parentName + "." + name;
                var fieldProperties = t.getProperties();
                var type = t.getDataType().widenSmallNumeric();
                // due to a bug also copy the field since the Attribute hierarchy extracts the data type
                // directly even if the data type is passed explicitly
                if (type != t.getDataType()) {
                    t = new EsField(t.getName(), type, t.getProperties(), t.isAggregatable(), t.isAlias());
                }

                FieldAttribute attribute = t instanceof UnsupportedEsField uef
                    ? new UnsupportedAttribute(source, name, uef)
                    : new FieldAttribute(source, parentName, name, t);
                // primitive branch
                if (DataType.isPrimitive(type)) {
                    list.add(attribute);
                }
                // allow compound object even if they are unknown
                if (fieldProperties.isEmpty() == false) {
                    mappingAsAttributes(list, source, attribute.name(), fieldProperties);
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
            final String policyName = (String) plan.policyName().fold(FoldContext.small() /* TODO remove me */);
            final var resolved = context.enrichResolution().getResolvedPolicy(policyName, plan.mode());
            if (resolved != null) {
                var policy = new EnrichPolicy(resolved.matchType(), null, List.of(), resolved.matchField(), resolved.enrichFields());
                var matchField = plan.matchField() == null || plan.matchField() instanceof EmptyAttribute
                    ? new UnresolvedAttribute(plan.source(), policy.getMatchField())
                    : plan.matchField();
                List<NamedExpression> enrichFields = calculateEnrichFields(
                    plan.source(),
                    policyName,
                    mappingAsAttributes(plan.source(), resolved.mapping()),
                    plan.enrichFields(),
                    policy
                );
                return new Enrich(
                    plan.source(),
                    plan.child(),
                    plan.mode(),
                    plan.policyName(),
                    matchField,
                    policy,
                    resolved.concreteIndices(),
                    enrichFields
                );
            } else {
                String error = context.enrichResolution().getError(policyName, plan.mode());
                var policyNameExp = new UnresolvedAttribute(plan.policyName().source(), policyName, error);
                return new Enrich(plan.source(), plan.child(), plan.mode(), policyNameExp, plan.matchField(), null, Map.of(), List.of());
            }
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
                return new UnresolvedAttribute(source, enrichFieldName, msg);
            } else {
                return new ReferenceAttribute(source, enrichFieldName, mappedField.dataType(), Nullability.TRUE, null, false);
            }
        }
    }

    private static class ResolveInference extends ParameterizedAnalyzerRule<InferencePlan<?>, AnalyzerContext> {
        @Override
        protected LogicalPlan rule(InferencePlan<?> plan, AnalyzerContext context) {
            assert plan.inferenceId().resolved() && plan.inferenceId().foldable();

            String inferenceId = plan.inferenceId().fold(FoldContext.small()).toString();
            ResolvedInference resolvedInference = context.inferenceResolution().getResolvedInference(inferenceId);

            if (resolvedInference != null && resolvedInference.taskType() == plan.taskType()) {
                return plan;
            } else if (resolvedInference != null) {
                String error = "cannot use inference endpoint ["
                    + inferenceId
                    + "] with task type ["
                    + resolvedInference.taskType()
                    + "] within a "
                    + plan.nodeName()
                    + " command. Only inference endpoints with the task type ["
                    + plan.taskType()
                    + "] are supported.";
                return plan.withInferenceResolutionError(inferenceId, error);
            } else {
                String error = context.inferenceResolution().getError(inferenceId);
                return plan.withInferenceResolutionError(inferenceId, error);
            }
        }
    }

    private static class ResolveLookupTables extends ParameterizedAnalyzerRule<Lookup, AnalyzerContext> {

        @Override
        protected LogicalPlan rule(Lookup lookup, AnalyzerContext context) {
            // the parser passes the string wrapped in a literal
            Source source = lookup.source();
            Expression tableNameExpression = lookup.tableName();
            String tableName = lookup.tableName().toString();
            Map<String, Map<String, Column>> tables = context.configuration().tables();
            LocalRelation localRelation = null;

            if (tables.containsKey(tableName) == false) {
                String message = "Unknown table [" + tableName + "]";
                // typos check
                List<String> potentialMatches = StringUtils.findSimilar(tableName, tables.keySet());
                if (CollectionUtils.isEmpty(potentialMatches) == false) {
                    message = UnresolvedAttribute.errorMessage(tableName, potentialMatches).replace("column", "table");
                }
                tableNameExpression = new UnresolvedAttribute(tableNameExpression.source(), tableName, message);
            }
            // wrap the table in a local relationship for idiomatic field resolution
            else {
                localRelation = tableMapAsRelation(source, tables.get(tableName));
                // postpone the resolution for ResolveRefs
            }

            return new Lookup(source, lookup.child(), tableNameExpression, lookup.matchFields(), localRelation);
        }

        private LocalRelation tableMapAsRelation(Source source, Map<String, Column> mapTable) {
            Block[] blocks = new Block[mapTable.size()];

            List<Attribute> attributes = new ArrayList<>(blocks.length);
            int i = 0;
            for (Map.Entry<String, Column> entry : mapTable.entrySet()) {
                String name = entry.getKey();
                Column column = entry.getValue();
                // create a fake ES field - alternative is to use a ReferenceAttribute
                EsField field = new EsField(name, column.type(), Map.of(), false, false);
                attributes.add(new FieldAttribute(source, null, name, field));
                // prepare the block for the supplier
                blocks[i++] = column.values();
            }
            LocalSupplier supplier = LocalSupplier.of(blocks);
            return new LocalRelation(source, attributes, supplier);
        }
    }

    public static class ResolveRefs extends ParameterizedAnalyzerRule<LogicalPlan, AnalyzerContext> {
        @Override
        protected LogicalPlan rule(LogicalPlan plan, AnalyzerContext context) {
            if (plan.childrenResolved() == false) {
                return plan;
            }
            final List<Attribute> childrenOutput = new ArrayList<>();

            // Gather all the children's output in case of non-unary plans; even for unaries, we need to copy because we may mutate this to
            // simplify resolution of e.g. RENAME.
            for (LogicalPlan child : plan.children()) {
                var output = child.output();
                childrenOutput.addAll(output);
            }

            if (plan instanceof Aggregate aggregate) {
                return resolveAggregate(aggregate, childrenOutput);
            }

            if (plan instanceof Completion c) {
                return resolveCompletion(c, childrenOutput);
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

            if (plan instanceof Fork f) {
                return resolveFork(f, context);
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

            if (plan instanceof Lookup l) {
                return resolveLookup(l, childrenOutput);
            }

            if (plan instanceof LookupJoin j) {
                return resolveLookupJoin(j);
            }

            if (plan instanceof Insist i) {
                return resolveInsist(i, childrenOutput, context.indexResolution());
            }

            if (plan instanceof Dedup dedup) {
                return resolveDedup(dedup, childrenOutput);
            }

            if (plan instanceof RrfScoreEval rrf) {
                return resolveRrfScoreEval(rrf, childrenOutput);
            }

            if (plan instanceof Rerank r) {
                return resolveRerank(r, childrenOutput);
            }

            return plan.transformExpressionsOnly(UnresolvedAttribute.class, ua -> maybeResolveAttribute(ua, childrenOutput));
        }

        private Aggregate resolveAggregate(Aggregate aggregate, List<Attribute> childrenOutput) {
            // if the grouping is resolved but the aggs are not, use the former to resolve the latter
            // e.g. STATS a ... GROUP BY a = x + 1
            Holder<Boolean> changed = new Holder<>(false);
            List<Expression> groupings = aggregate.groupings();
            List<? extends NamedExpression> aggregates = aggregate.aggregates();
            // first resolve groupings since the aggs might refer to them
            // trying to globally resolve unresolved attributes will lead to some being marked as unresolvable
            if (Resolvables.resolved(groupings) == false) {
                List<Expression> newGroupings = new ArrayList<>(groupings.size());
                for (Expression g : groupings) {
                    Expression resolved = g.transformUp(UnresolvedAttribute.class, ua -> maybeResolveAttribute(ua, childrenOutput));
                    if (resolved != g) {
                        changed.set(true);
                    }
                    newGroupings.add(resolved);
                }
                groupings = newGroupings;
                if (changed.get()) {
                    aggregate = aggregate.with(aggregate.child(), newGroupings, aggregate.aggregates());
                    changed.set(false);
                }
            }

            if (Resolvables.resolved(groupings) == false || Resolvables.resolved(aggregates) == false) {
                ArrayList<Attribute> resolved = new ArrayList<>();
                for (Expression e : groupings) {
                    Attribute attr = Expressions.attribute(e);
                    if (attr != null && attr.resolved()) {
                        resolved.add(attr);
                    }
                }
                List<Attribute> resolvedList = NamedExpressions.mergeOutputAttributes(resolved, childrenOutput);

                List<NamedExpression> newAggregates = new ArrayList<>();
                // If the groupings are not resolved, skip the resolution of the references to groupings in the aggregates, resolve the
                // aggregations that do not reference to groupings, so that the fields/attributes referenced by the aggregations can be
                // resolved, and verifier doesn't report field/reference/column not found errors for them.
                boolean groupingResolved = Resolvables.resolved(groupings);
                int size = groupingResolved ? aggregates.size() : aggregates.size() - groupings.size();
                for (int i = 0; i < aggregates.size(); i++) {
                    NamedExpression maybeResolvedAgg = aggregates.get(i);
                    if (i < size) { // Skip resolving references to groupings in the aggregations if the groupings are not resolved yet.
                        maybeResolvedAgg = (NamedExpression) maybeResolvedAgg.transformUp(UnresolvedAttribute.class, ua -> {
                            Expression ne = ua;
                            Attribute maybeResolved = maybeResolveAttribute(ua, resolvedList);
                            // An item in aggregations can reference to groupings explicitly, if groupings are not resolved yet and
                            // maybeResolved is not resolved, return the original UnresolvedAttribute, so that it has another chance
                            // to get resolved in the next iteration.
                            // For example STATS c = count(emp_no), x = d::int + 1 BY d = (date == "2025-01-01")
                            if (groupingResolved || maybeResolved.resolved()) {
                                changed.set(true);
                                ne = maybeResolved;
                            }
                            return ne;
                        });
                    }
                    newAggregates.add(maybeResolvedAgg);
                }

                // TODO: remove this when Stats interface is removed
                aggregate = changed.get() ? aggregate.with(aggregate.child(), groupings, newAggregates) : aggregate;
            }

            return aggregate;
        }

        private LogicalPlan resolveCompletion(Completion p, List<Attribute> childrenOutput) {
            Attribute targetField = p.targetField();
            Expression prompt = p.prompt();

            if (targetField instanceof UnresolvedAttribute ua) {
                targetField = new ReferenceAttribute(ua.source(), ua.name(), KEYWORD);
            }

            if (prompt.resolved() == false) {
                prompt = prompt.transformUp(UnresolvedAttribute.class, ua -> maybeResolveAttribute(ua, childrenOutput));
            }

            return new Completion(p.source(), p.child(), p.inferenceId(), prompt, targetField);
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
                        ? new ReferenceAttribute(resolved.source(), resolved.name(), resolved.dataType(), resolved.nullable(), null, false)
                        : resolved
                );
            }
            return p;
        }

        private LogicalPlan resolveLookup(Lookup l, List<Attribute> childrenOutput) {
            // check if the table exists before performing any resolution
            if (l.localRelation() == null) {
                return l;
            }

            // check the on field against both the child output and the inner relation
            List<Attribute> matchFields = new ArrayList<>(l.matchFields().size());
            List<Attribute> localOutput = l.localRelation().output();
            boolean modified = false;

            for (Attribute matchField : l.matchFields()) {
                Attribute matchFieldChildReference = matchField;
                if (matchField instanceof UnresolvedAttribute ua && ua.customMessage() == false) {
                    modified = true;
                    Attribute joinedAttribute = maybeResolveAttribute(ua, localOutput);
                    // can't find the field inside the local relation
                    if (joinedAttribute instanceof UnresolvedAttribute lua) {
                        // adjust message
                        matchFieldChildReference = lua.withUnresolvedMessage(
                            lua.unresolvedMessage().replace("Unknown column", "Unknown column in lookup target")
                        );
                    } else {
                        // check also the child output by resolving to it
                        Attribute attr = maybeResolveAttribute(ua, childrenOutput);
                        matchFieldChildReference = attr;
                        if (attr instanceof UnresolvedAttribute == false) {
                            /*
                             * If they do, make sure the data types line up. If either is
                             * null it's fine to match it against anything.
                             */
                            boolean dataTypesOk = joinedAttribute.dataType().equals(attr.dataType());
                            if (false == dataTypesOk) {
                                dataTypesOk = joinedAttribute.dataType() == DataType.NULL || attr.dataType() == DataType.NULL;
                            }
                            if (false == dataTypesOk) {
                                dataTypesOk = joinedAttribute.dataType().equals(KEYWORD) && attr.dataType().equals(TEXT);
                            }
                            if (false == dataTypesOk) {
                                matchFieldChildReference = new UnresolvedAttribute(
                                    attr.source(),
                                    attr.name(),
                                    attr.id(),
                                    "column type mismatch, table column was ["
                                        + joinedAttribute.dataType().typeName()
                                        + "] and original column was ["
                                        + attr.dataType().typeName()
                                        + "]",
                                    null
                                );
                            }
                        }
                    }
                }

                matchFields.add(matchFieldChildReference);
            }
            if (modified) {
                return new Lookup(l.source(), l.child(), l.tableName(), matchFields, l.localRelation());
            }
            return l;
        }

        private Join resolveLookupJoin(LookupJoin join) {
            JoinConfig config = join.config();
            // for now, support only (LEFT) USING clauses
            JoinType type = config.type();
            // rewrite the join into an equi-join between the field with the same name between left and right
            if (type instanceof UsingJoinType using) {
                List<Attribute> cols = using.columns();
                // the lookup cannot be resolved, bail out
                if (Expressions.anyMatch(cols, c -> c instanceof UnresolvedAttribute ua && ua.customMessage())) {
                    return join;
                }

                JoinType coreJoin = using.coreJoin();
                // verify the join type
                if (coreJoin != JoinTypes.LEFT) {
                    String name = cols.get(0).name();
                    UnresolvedAttribute errorAttribute = new UnresolvedAttribute(
                        join.source(),
                        name,
                        "Only LEFT join is supported with USING"
                    );
                    return join.withConfig(new JoinConfig(type, singletonList(errorAttribute), emptyList(), emptyList()));
                }
                // resolve the using columns against the left and the right side then assemble the new join config
                List<Attribute> leftKeys = resolveUsingColumns(cols, join.left().output(), "left");
                List<Attribute> rightKeys = resolveUsingColumns(cols, join.right().output(), "right");

                config = new JoinConfig(coreJoin, leftKeys, leftKeys, rightKeys);
                join = new LookupJoin(join.source(), join.left(), join.right(), config);
            } else if (type != JoinTypes.LEFT) {
                // everything else is unsupported for now
                // LEFT can only happen by being mapped from a USING above. So we need to exclude this as well because this rule can be run
                // more than once.
                UnresolvedAttribute errorAttribute = new UnresolvedAttribute(join.source(), "unsupported", "Unsupported join type");
                // add error message
                return join.withConfig(new JoinConfig(type, singletonList(errorAttribute), emptyList(), emptyList()));
            }
            return join;
        }

        private LogicalPlan resolveFork(Fork fork, AnalyzerContext context) {
            // we align the outputs of the sub plans such that they have the same columns
            boolean changed = false;
            List<LogicalPlan> newSubPlans = new ArrayList<>();
            Set<String> forkColumns = fork.outputSet().names();

            for (LogicalPlan logicalPlan : fork.children()) {
                Source source = logicalPlan.source();

                // find the missing columns
                List<Attribute> missing = new ArrayList<>();
                Set<String> currentNames = logicalPlan.outputSet().names();
                for (Attribute attr : fork.outputSet()) {
                    if (currentNames.contains(attr.name()) == false) {
                        missing.add(attr);
                    }
                }

                List<Alias> aliases = missing.stream().map(attr -> new Alias(source, attr.name(), Literal.of(attr, null))).toList();

                // add the missing columns
                if (aliases.size() > 0) {
                    logicalPlan = new Eval(source, logicalPlan, aliases);
                    changed = true;
                }

                List<String> subPlanColumns = logicalPlan.output().stream().map(Attribute::name).toList();
                // We need to add an explicit Keep even if the outputs align
                // This is because at the moment the sub plans are executed and optimized separately and the output might change
                // during optimizations. Once we add streaming we might not need to add a Keep when the outputs already align.
                // Note that until we add explicit support for KEEP in FORK branches, this condition will always be true.
                if (logicalPlan instanceof Keep == false || subPlanColumns.equals(forkColumns) == false) {
                    changed = true;
                    List<Attribute> newOutput = new ArrayList<>();
                    for (String attrName : forkColumns) {
                        for (Attribute subAttr : logicalPlan.output()) {
                            if (attrName.equals(subAttr.name())) {
                                newOutput.add(subAttr);
                            }
                        }
                    }
                    logicalPlan = new Keep(logicalPlan.source(), logicalPlan, newOutput);
                }

                newSubPlans.add(logicalPlan);
            }

            return changed ? new Fork(fork.source(), newSubPlans) : fork;
        }

        private LogicalPlan resolveRerank(Rerank rerank, List<Attribute> childrenOutput) {
            List<Alias> newFields = new ArrayList<>();
            boolean changed = false;

            // First resolving fields used in expression
            for (Alias field : rerank.rerankFields()) {
                Alias result = (Alias) field.transformUp(UnresolvedAttribute.class, ua -> resolveAttribute(ua, childrenOutput));
                newFields.add(result);
                changed |= result != field;
            }

            if (changed) {
                rerank = rerank.withRerankFields(newFields);
            }

            // Ensure the score attribute is present in the output.
            if (rerank.scoreAttribute() instanceof UnresolvedAttribute ua) {
                Attribute resolved = resolveAttribute(ua, childrenOutput);
                if (resolved.resolved() == false || resolved.dataType() != DOUBLE) {
                    resolved = MetadataAttribute.create(Source.EMPTY, MetadataAttribute.SCORE);
                }
                rerank = rerank.withScoreAttribute(resolved);
            }

            return rerank;
        }

        private List<Attribute> resolveUsingColumns(List<Attribute> cols, List<Attribute> output, String side) {
            List<Attribute> resolved = new ArrayList<>(cols.size());
            for (Attribute col : cols) {
                if (col instanceof UnresolvedAttribute ua) {
                    Attribute resolvedField = maybeResolveAttribute(ua, output);
                    if (resolvedField instanceof UnresolvedAttribute ucol) {
                        String message = ua.unresolvedMessage();
                        String match = "column [" + ucol.name() + "]";
                        resolvedField = ucol.withUnresolvedMessage(message.replace(match, match + " in " + side + " side of join"));
                    }
                    resolved.add(resolvedField);
                } else {
                    throw new IllegalStateException(
                        "Surprised to discover column [ " + col.name() + "] already resolved when resolving JOIN keys"
                    );
                }
            }
            return resolved;
        }

        private LogicalPlan resolveInsist(Insist insist, List<Attribute> childrenOutput, IndexResolution indexResolution) {
            List<Attribute> list = new ArrayList<>();
            for (Attribute a : insist.insistedAttributes()) {
                list.add(resolveInsistAttribute(a, childrenOutput, indexResolution));
            }
            return insist.withAttributes(list);
        }

        private Attribute resolveInsistAttribute(Attribute attribute, List<Attribute> childrenOutput, IndexResolution indexResolution) {
            Attribute resolvedCol = maybeResolveAttribute((UnresolvedAttribute) attribute, childrenOutput);
            // Field isn't mapped anywhere.
            if (resolvedCol instanceof UnresolvedAttribute) {
                return insistKeyword(attribute);
            }

            // Field is partially unmapped.
            if (resolvedCol instanceof FieldAttribute fa && indexResolution.get().isPartiallyUnmappedField(fa.name())) {
                return fa.dataType() == KEYWORD ? insistKeyword(fa) : invalidInsistAttribute(fa);
            }

            // Either the field is mapped everywhere and we can just use the resolved column, or the INSIST clause isn't on top of a FROM
            // clause—for example, it might be on top of a ROW clause—so the verifier will catch it and fail.
            return resolvedCol;
        }

        private static Attribute invalidInsistAttribute(FieldAttribute fa) {
            var name = fa.name();
            EsField field = fa.field() instanceof InvalidMappedField imf
                ? new InvalidMappedField(name, InvalidMappedField.makeErrorsMessageIncludingInsistKeyword(imf.getTypesToIndices()))
                : new InvalidMappedField(
                    name,
                    Strings.format(
                        "mapped as [2] incompatible types: [keyword] enforced by INSIST command, and [%s] in index mappings",
                        fa.dataType().typeName()
                    )
                );
            return new FieldAttribute(fa.source(), name, field);
        }

        private static FieldAttribute insistKeyword(Attribute attribute) {
            return new FieldAttribute(attribute.source(), attribute.name(), new PotentiallyUnmappedKeywordEsField(attribute.name()));
        }

        private LogicalPlan resolveDedup(Dedup dedup, List<Attribute> childrenOutput) {
            List<NamedExpression> aggregates = dedup.finalAggs();
            List<Attribute> groupings = dedup.groupings();
            List<NamedExpression> newAggs = new ArrayList<>();
            List<Attribute> newGroupings = new ArrayList<>();

            for (NamedExpression agg : aggregates) {
                var newAgg = (NamedExpression) agg.transformUp(UnresolvedAttribute.class, ua -> {
                    Expression ne = ua;
                    Attribute maybeResolved = maybeResolveAttribute(ua, childrenOutput);
                    if (maybeResolved != null) {
                        ne = maybeResolved;
                    }
                    return ne;
                });
                newAggs.add(newAgg);
            }

            for (Attribute attr : groupings) {
                if (attr instanceof UnresolvedAttribute ua) {
                    newGroupings.add(resolveAttribute(ua, childrenOutput));
                } else {
                    newGroupings.add(attr);
                }
            }

            return new Dedup(dedup.source(), dedup.child(), newAggs, newGroupings);
        }

        private LogicalPlan resolveRrfScoreEval(RrfScoreEval rrf, List<Attribute> childrenOutput) {
            Attribute scoreAttr = rrf.scoreAttribute();
            Attribute forkAttr = rrf.forkAttribute();

            if (scoreAttr instanceof UnresolvedAttribute ua) {
                scoreAttr = resolveAttribute(ua, childrenOutput);
            }

            if (forkAttr instanceof UnresolvedAttribute ua) {
                forkAttr = resolveAttribute(ua, childrenOutput);
            }

            if (forkAttr != rrf.forkAttribute() || scoreAttr != rrf.scoreAttribute()) {
                return new RrfScoreEval(rrf.source(), rrf.child(), scoreAttr, forkAttr);
            }

            return rrf;
        }

        private Attribute maybeResolveAttribute(UnresolvedAttribute ua, List<Attribute> childrenOutput) {
            return maybeResolveAttribute(ua, childrenOutput, log);
        }

        private static Attribute maybeResolveAttribute(UnresolvedAttribute ua, List<Attribute> childrenOutput, Logger logger) {
            if (ua.customMessage()) {
                return ua;
            }
            return resolveAttribute(ua, childrenOutput, logger);
        }

        private Attribute resolveAttribute(UnresolvedAttribute ua, List<Attribute> childrenOutput) {
            return resolveAttribute(ua, childrenOutput, log);
        }

        private static Attribute resolveAttribute(UnresolvedAttribute ua, List<Attribute> childrenOutput, Logger logger) {
            Attribute resolved = ua;
            var named = resolveAgainstList(ua, childrenOutput);
            // if resolved, return it; otherwise keep it in place to be resolved later
            if (named.size() == 1) {
                resolved = named.get(0);
                if (logger != null && logger.isTraceEnabled() && resolved.resolved()) {
                    logger.trace("Resolved {} to {}", ua, resolved);
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
                    } else if (proj instanceof UnresolvedNamePattern up) {
                        resolved = resolveAgainstList(up, childOutput);
                        priority = 1;
                    } else if (proj instanceof UnresolvedAttribute ua) {
                        resolved = resolveAgainstList(ua, childOutput);
                        priority = 0;
                    } else {
                        throw new EsqlIllegalArgumentException("unexpected projection: " + proj);
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
                List<? extends NamedExpression> resolved;

                if (ne instanceof UnresolvedNamePattern np) {
                    resolved = resolveAgainstList(np, childOutput);
                } else if (ne instanceof UnresolvedAttribute ua) {
                    resolved = resolveAgainstList(ua, childOutput);
                } else {
                    resolved = singletonList(ne);
                }

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
            List<NamedExpression> projections = projectionsForRename(rename, childrenOutput, log);

            return new EsqlProject(rename.source(), rename.child(), projections);
        }

        /**
         * This will turn a {@link Rename} into an equivalent {@link Project}.
         * Can mutate {@code childrenOutput}; hand this a copy if you want to avoid mutation.
         */
        public static List<NamedExpression> projectionsForRename(Rename rename, List<Attribute> childrenOutput, Logger logger) {
            List<NamedExpression> projections = new ArrayList<>(childrenOutput);

            int renamingsCount = rename.renamings().size();
            List<NamedExpression> unresolved = new ArrayList<>(renamingsCount);
            Map<String, String> reverseAliasing = new HashMap<>(renamingsCount); // `| rename a as x` => map(a: x)

            rename.renamings().forEach(alias -> {
                // skip NOPs: `| rename a as a`
                if (alias.child() instanceof UnresolvedAttribute ua && alias.name().equals(ua.name()) == false) {
                    // remove attributes overwritten by a renaming: `| keep a, b, c | rename a as b`
                    projections.removeIf(x -> x.name().equals(alias.name()));
                    childrenOutput.removeIf(x -> x.name().equals(alias.name()));

                    var resolved = maybeResolveAttribute(ua, childrenOutput, logger);
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
                                    li.set(alias.replaceChildren(a.children()));
                                    updated = true;
                                    break;
                                }
                            }
                        }
                        if (updated == false) {
                            var u = resolved;
                            var previousAliasName = reverseAliasing.get(resolved.name());
                            if (previousAliasName != null) {
                                String message = LoggerMessageFormat.format(
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

            return projections;
        }

        private LogicalPlan resolveEnrich(Enrich enrich, List<Attribute> childrenOutput) {

            if (enrich.matchField().toAttribute() instanceof UnresolvedAttribute ua) {
                Attribute resolved = maybeResolveAttribute(ua, childrenOutput);
                if (resolved.equals(ua)) {
                    return enrich;
                }
                if (resolved.resolved() && enrich.policy() != null) {
                    final DataType dataType = resolved.dataType();
                    String matchType = enrich.policy().getType();
                    DataType[] allowed = allowedEnrichTypes(matchType);
                    if (Arrays.asList(allowed).contains(dataType) == false) {
                        String suffix = "only ["
                            + Arrays.stream(allowed).map(DataType::typeName).collect(Collectors.joining(", "))
                            + "] allowed for type ["
                            + matchType
                            + "]";
                        resolved = ua.withUnresolvedMessage(
                            "Unsupported type ["
                                + resolved.dataType().typeName()
                                + "] for enrich matching field ["
                                + ua.name()
                                + "]; "
                                + suffix
                        );
                    }
                }
                return new Enrich(
                    enrich.source(),
                    enrich.child(),
                    enrich.mode(),
                    enrich.policyName(),
                    resolved,
                    enrich.policy(),
                    enrich.concreteIndices(),
                    enrich.enrichFields()
                );
            }
            return enrich;
        }

        private static final DataType[] GEO_TYPES = new DataType[] { GEO_POINT, GEO_SHAPE };
        private static final DataType[] NON_GEO_TYPES = new DataType[] { KEYWORD, TEXT, IP, LONG, INTEGER, FLOAT, DOUBLE, DATETIME };

        private DataType[] allowedEnrichTypes(String matchType) {
            return matchType.equals(GEO_MATCH_TYPE) ? GEO_TYPES : NON_GEO_TYPES;
        }
    }

    private static List<Attribute> resolveAgainstList(UnresolvedNamePattern up, Collection<Attribute> attrList) {
        UnresolvedAttribute ua = new UnresolvedAttribute(up.source(), up.pattern());
        Predicate<Attribute> matcher = a -> up.match(a.name());
        var matches = AnalyzerRules.maybeResolveAgainstList(matcher, () -> ua, attrList, true, a -> Analyzer.handleSpecialFields(ua, a));
        return potentialCandidatesIfNoMatchesFound(ua, matches, attrList, list -> UnresolvedNamePattern.errorMessage(up.pattern(), list));
    }

    private static List<Attribute> resolveAgainstList(UnresolvedAttribute ua, Collection<Attribute> attrList) {
        var matches = AnalyzerRules.maybeResolveAgainstList(ua, attrList, a -> Analyzer.handleSpecialFields(ua, a));
        return potentialCandidatesIfNoMatchesFound(ua, matches, attrList, list -> UnresolvedAttribute.errorMessage(ua.name(), list));
    }

    private static List<Attribute> potentialCandidatesIfNoMatchesFound(
        UnresolvedAttribute ua,
        List<Attribute> matches,
        Collection<Attribute> attrList,
        java.util.function.Function<List<String>, String> messageProducer
    ) {
        if (ua.customMessage()) {
            return List.of();
        }
        // none found - add error message
        if (matches.isEmpty()) {
            Set<String> names = new HashSet<>(attrList.size());
            for (var a : attrList) {
                String nameCandidate = a.name();
                if (DataType.isPrimitive(a.dataType())) {
                    names.add(nameCandidate);
                }
            }
            var name = ua.name();
            UnresolvedAttribute unresolved = ua.withUnresolvedMessage(messageProducer.apply(StringUtils.findSimilar(name, names)));
            matches = singletonList(unresolved);
        }
        return matches;
    }

    private static Attribute handleSpecialFields(UnresolvedAttribute u, Attribute named) {
        return named.withLocation(u.source());
    }

    private static class ResolveFunctions extends ParameterizedAnalyzerRule<LogicalPlan, AnalyzerContext> {

        @Override
        protected LogicalPlan rule(LogicalPlan plan, AnalyzerContext context) {
            // Allow resolving snapshot-only functions, but do not include them in the documentation
            final EsqlFunctionRegistry snapshotRegistry = context.functionRegistry().snapshotRegistry();
            return plan.transformExpressionsOnly(
                UnresolvedFunction.class,
                uf -> resolveFunction(uf, context.configuration(), snapshotRegistry)
            );
        }

        public static org.elasticsearch.xpack.esql.core.expression.function.Function resolveFunction(
            UnresolvedFunction uf,
            Configuration configuration,
            EsqlFunctionRegistry functionRegistry
        ) {
            org.elasticsearch.xpack.esql.core.expression.function.Function f = null;
            if (uf.analyzed()) {
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
            var source = logicalPlan.source();
            return new Limit(source, new Literal(source, limit, DataType.INTEGER), logicalPlan);
        }
    }

    private static class AddImplicitForkLimit extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {
        private final AddImplicitLimit addImplicitLimit = new AddImplicitLimit();

        @Override
        public LogicalPlan apply(LogicalPlan logicalPlan, AnalyzerContext context) {
            return logicalPlan.transformUp(Fork.class, fork -> addImplicitLimitToForkSubQueries(fork, context));
        }

        private LogicalPlan addImplicitLimitToForkSubQueries(Fork fork, AnalyzerContext ctx) {
            List<LogicalPlan> newSubPlans = new ArrayList<>();
            for (var subPlan : fork.children()) {
                newSubPlans.add(addImplicitLimit.apply(subPlan, ctx));
            }
            return fork.replaceSubPlans(newSubPlans);
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

    /**
     * Cast string literals in ScalarFunction, EsqlArithmeticOperation, BinaryComparison, In and GroupingFunction to desired data types.
     * For example, the string literals in the following expressions will be cast implicitly to the field data type on the left hand side.
     * <ul>
     * <li>date > "2024-08-21"</li>
     * <li>date in ("2024-08-21", "2024-08-22", "2024-08-23")</li>
     * <li>date = "2024-08-21" + 3 days</li>
     * <li>ip == "127.0.0.1"</li>
     * <li>version != "1.0"</li>
     * <li>bucket(dateField, "1 month")</li>
     * <li>date_trunc("1 minute", dateField)</li>
     * </ul>
     * If the inputs to Coalesce are mixed numeric types, cast the rest of the numeric field or value to the first numeric data type if
     * applicable. For example, implicit casting converts:
     * <ul>
     * <li>Coalesce(Long, Int) to Coalesce(Long, Long)</li>
     * <li>Coalesce(null, Long, Int) to Coalesce(null, Long, Long)</li>
     * <li>Coalesce(Double, Long, Int) to Coalesce(Double, Double, Double)</li>
     * <li>Coalesce(null, Double, Long, Int) to Coalesce(null, Double, Double, Double)</li>
     * </ul>
     * Coalesce(Int, Long) will NOT be converted to Coalesce(Long, Long) or Coalesce(Int, Int).
     */
    private static class ImplicitCasting extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {
        @Override
        public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
            return plan.transformExpressionsUp(
                org.elasticsearch.xpack.esql.core.expression.function.Function.class,
                e -> ImplicitCasting.cast(e, context.functionRegistry().snapshotRegistry())
            );
        }

        private static Expression cast(org.elasticsearch.xpack.esql.core.expression.function.Function f, EsqlFunctionRegistry registry) {
            if (f instanceof In in) {
                return processIn(in);
            }
            if (f instanceof EsqlScalarFunction || f instanceof GroupingFunction) { // exclude AggregateFunction until it is needed
                return processScalarOrGroupingFunction(f, registry);
            }
            if (f instanceof EsqlArithmeticOperation || f instanceof BinaryComparison) {
                return processBinaryOperator((BinaryOperator) f);
            }
            return f;
        }

        private static Expression processScalarOrGroupingFunction(
            org.elasticsearch.xpack.esql.core.expression.function.Function f,
            EsqlFunctionRegistry registry
        ) {
            List<Expression> args = f.arguments();
            List<DataType> targetDataTypes = registry.getDataTypeForStringLiteralConversion(f.getClass());
            if (targetDataTypes == null || targetDataTypes.isEmpty()) {
                return f;
            }
            List<Expression> newChildren = new ArrayList<>(args.size());
            boolean childrenChanged = false;
            DataType targetDataType = DataType.NULL;
            Expression arg;
            DataType targetNumericType = null;
            boolean castNumericArgs = true;
            for (int i = 0; i < args.size(); i++) {
                arg = args.get(i);
                if (arg.resolved()) {
                    var dataType = arg.dataType();
                    if (dataType == KEYWORD) {
                        if (arg.foldable() && ((arg instanceof EsqlScalarFunction) == false)) {
                            if (i < targetDataTypes.size()) {
                                targetDataType = targetDataTypes.get(i);
                            }
                            if (targetDataType != DataType.NULL && targetDataType != DataType.UNSUPPORTED) {
                                Expression e = castStringLiteral(arg, targetDataType);
                                if (e != arg) {
                                    childrenChanged = true;
                                    newChildren.add(e);
                                    continue;
                                }
                            }
                        }
                    } else if (dataType.isNumeric() && canCastMixedNumericTypes(f) && castNumericArgs) {
                        if (targetNumericType == null) {
                            targetNumericType = dataType;  // target data type is the first numeric data type
                        } else if (dataType != targetNumericType) {
                            castNumericArgs = canCastNumeric(dataType, targetNumericType);
                        }
                    }
                }
                newChildren.add(args.get(i));
            }
            Expression resultF = childrenChanged ? f.replaceChildren(newChildren) : f;
            return targetNumericType != null && castNumericArgs
                ? castMixedNumericTypes((EsqlScalarFunction) resultF, targetNumericType)
                : resultF;
        }

        private static Expression processBinaryOperator(BinaryOperator<?, ?, ?, ?> o) {
            Expression left = o.left();
            Expression right = o.right();
            if (left.resolved() == false || right.resolved() == false) {
                return o;
            }
            List<Expression> newChildren = new ArrayList<>(2);
            boolean childrenChanged = false;
            DataType targetDataType = DataType.NULL;
            Expression from = Literal.NULL;

            if (left.dataType() == KEYWORD && left.foldable() && (left instanceof EsqlScalarFunction == false)) {
                if (supportsStringImplicitCasting(right.dataType())) {
                    targetDataType = right.dataType();
                    from = left;
                } else if (supportsImplicitTemporalCasting(right, o)) {
                    targetDataType = DATETIME;
                    from = left;
                }
            }
            if (right.dataType() == KEYWORD && right.foldable() && (right instanceof EsqlScalarFunction == false)) {
                if (supportsStringImplicitCasting(left.dataType())) {
                    targetDataType = left.dataType();
                    from = right;
                } else if (supportsImplicitTemporalCasting(left, o)) {
                    targetDataType = DATETIME;
                    from = right;
                }
            }
            if (from != Literal.NULL) {
                Expression e = castStringLiteral(from, targetDataType);
                newChildren.add(from == left ? e : left);
                newChildren.add(from == right ? e : right);
                childrenChanged = true;
            }
            return childrenChanged ? o.replaceChildren(newChildren) : o;
        }

        private static Expression processIn(In in) {
            Expression left = in.value();
            List<Expression> right = in.list();

            if (left.resolved() == false || supportsStringImplicitCasting(left.dataType()) == false) {
                return in;
            }

            DataType targetDataType = left.dataType();
            List<Expression> newChildren = new ArrayList<>(right.size() + 1);
            boolean childrenChanged = false;

            for (Expression value : right) {
                if (value.resolved() && value.dataType() == KEYWORD && value.foldable()) {
                    Expression e = castStringLiteral(value, targetDataType);
                    newChildren.add(e);
                    childrenChanged = true;
                } else {
                    newChildren.add(value);
                }
            }
            newChildren.add(left);
            return childrenChanged ? in.replaceChildren(newChildren) : in;
        }

        private static boolean canCastMixedNumericTypes(org.elasticsearch.xpack.esql.core.expression.function.Function f) {
            return f instanceof Coalesce || f instanceof Case || f instanceof Greatest || f instanceof Least;
        }

        private static boolean canCastNumeric(DataType from, DataType to) {
            DataType commonType = EsqlDataTypeConverter.commonType(from, to);
            return commonType == to;
        }

        private static Expression castMixedNumericTypes(EsqlScalarFunction f, DataType targetNumericType) {
            List<Expression> newChildren = new ArrayList<>(f.children().size());
            boolean childrenChanged = false;
            DataType childDataType;

            for (Expression e : f.children()) {
                if (e.resolved()) {
                    childDataType = e.dataType();
                    if (childDataType.isNumeric() == false
                        || childDataType == targetNumericType
                        || canCastNumeric(childDataType, targetNumericType) == false) {
                        newChildren.add(e);
                        continue;
                    }
                    childrenChanged = true;
                    // add a casting function
                    switch (targetNumericType) {
                        case INTEGER -> newChildren.add(new ToInteger(e.source(), e));
                        case LONG -> newChildren.add(new ToLong(e.source(), e));
                        case DOUBLE -> newChildren.add(new ToDouble(e.source(), e));
                        case UNSIGNED_LONG -> newChildren.add(new ToUnsignedLong(e.source(), e));
                        default -> throw new EsqlIllegalArgumentException("unexpected data type: " + targetNumericType);
                    }
                } else {
                    newChildren.add(e);
                }
            }
            return childrenChanged ? f.replaceChildren(newChildren) : f;
        }

        private static boolean supportsImplicitTemporalCasting(Expression e, BinaryOperator<?, ?, ?, ?> o) {
            return isTemporalAmount(e.dataType()) && (o instanceof DateTimeArithmeticOperation);
        }

        private static boolean supportsStringImplicitCasting(DataType type) {
            return type == DATETIME || type == DATE_NANOS || type == IP || type == VERSION || type == BOOLEAN;
        }

        private static UnresolvedAttribute unresolvedAttribute(Expression value, String type, Exception e) {
            String message = LoggerMessageFormat.format(
                "Cannot convert string [{}] to [{}], error [{}]",
                value.fold(FoldContext.small() /* TODO remove me */),
                type,
                (e instanceof ParsingException pe) ? pe.getErrorMessage() : e.getMessage()
            );
            return new UnresolvedAttribute(value.source(), String.valueOf(value.fold(FoldContext.small() /* TODO remove me */)), message);
        }

        private static Expression castStringLiteralToTemporalAmount(Expression from) {
            try {
                TemporalAmount result = maybeParseTemporalAmount(from.fold(FoldContext.small() /* TODO remove me */).toString().strip());
                if (result == null) {
                    return from;
                }
                DataType target = result instanceof Duration ? TIME_DURATION : DATE_PERIOD;
                return new Literal(from.source(), result, target);
            } catch (Exception e) {
                return unresolvedAttribute(from, DATE_PERIOD + " or " + TIME_DURATION, e);
            }
        }

        private static Expression castStringLiteral(Expression from, DataType target) {
            assert from.foldable();
            try {
                return isTemporalAmount(target)
                    ? castStringLiteralToTemporalAmount(from)
                    : new Literal(
                        from.source(),
                        EsqlDataTypeConverter.convert(from.fold(FoldContext.small() /* TODO remove me */), target),
                        target
                    );
            } catch (Exception e) {
                return unresolvedAttribute(from, target.toString(), e);
            }
        }
    }

    /**
     * The EsqlIndexResolver will create InvalidMappedField instances for fields that are ambiguous (i.e. have multiple mappings).
     * During {@link ResolveRefs} we do not convert these to UnresolvedAttribute instances, as we want to first determine if they can
     * instead be handled by conversion functions within the query. This rule looks for matching conversion functions and converts
     * those fields into MultiTypeEsField, which encapsulates the knowledge of how to convert these into a single type.
     * This knowledge will be used later in generating the FieldExtractExec with built-in type conversion.
     * Any fields which could not be resolved by conversion functions will be converted to UnresolvedAttribute instances in a later rule
     * (See {@link UnionTypesCleanup} below).
     */
    private static class ResolveUnionTypes extends Rule<LogicalPlan, LogicalPlan> {

        record TypeResolutionKey(String fieldName, DataType fieldType) {}

        private List<FieldAttribute> unionFieldAttributes;

        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            unionFieldAttributes = new ArrayList<>();
            // Collect field attributes from previous runs
            plan.forEachUp(EsRelation.class, rel -> {
                for (Attribute attr : rel.output()) {
                    if (attr instanceof FieldAttribute fa && fa.field() instanceof MultiTypeEsField) {
                        unionFieldAttributes.add(fa);
                    }
                }
            });

            return plan.transformUp(LogicalPlan.class, p -> p.childrenResolved() == false ? p : doRule(p));
        }

        private LogicalPlan doRule(LogicalPlan plan) {
            int alreadyAddedUnionFieldAttributes = unionFieldAttributes.size();
            // See if the eval function has an unresolved MultiTypeEsField field
            // Replace the entire convert function with a new FieldAttribute (containing type conversion knowledge)
            plan = plan.transformExpressionsOnly(e -> {
                if (e instanceof ConvertFunction convert) {
                    return resolveConvertFunction(convert, unionFieldAttributes);
                }
                return e;
            });
            // If no union fields were generated, return the plan as is
            if (unionFieldAttributes.size() == alreadyAddedUnionFieldAttributes) {
                return plan;
            }

            // And add generated fields to EsRelation, so these new attributes will appear in the OutputExec of the Fragment
            // and thereby get used in FieldExtractExec
            plan = plan.transformDown(EsRelation.class, esr -> {
                List<Attribute> missing = new ArrayList<>();
                for (FieldAttribute fa : unionFieldAttributes) {
                    // Using outputSet().contains looks by NameId, resp. uses semanticEquals.
                    if (esr.outputSet().contains(fa) == false) {
                        missing.add(fa);
                    }
                }

                if (missing.isEmpty() == false) {
                    return new EsRelation(
                        esr.source(),
                        esr.indexPattern(),
                        esr.indexMode(),
                        esr.indexNameWithModes(),
                        CollectionUtils.combine(esr.output(), missing)
                    );
                }
                return esr;
            });
            return plan;
        }

        private Expression resolveConvertFunction(ConvertFunction convert, List<FieldAttribute> unionFieldAttributes) {
            Expression convertExpression = (Expression) convert;
            if (convert.field() instanceof FieldAttribute fa && fa.field() instanceof InvalidMappedField imf) {
                HashMap<TypeResolutionKey, Expression> typeResolutions = new HashMap<>();
                Set<DataType> supportedTypes = convert.supportedTypes();
                if (convert instanceof FoldablesConvertFunction fcf) {
                    // FoldablesConvertFunction does not accept fields as inputs, they only accept constants
                    String unresolvedMessage = "argument of ["
                        + fcf.sourceText()
                        + "] must be a constant, received ["
                        + Expressions.name(fa)
                        + "]";
                    Expression ua = new UnresolvedAttribute(fa.source(), fa.name(), unresolvedMessage);
                    return fcf.replaceChildren(Collections.singletonList(ua));
                }
                imf.types().forEach(type -> {
                    if (supportedTypes.contains(type.widenSmallNumeric())) {
                        TypeResolutionKey key = new TypeResolutionKey(fa.name(), type);
                        var concreteConvert = typeSpecificConvert(convert, fa.source(), type, imf);
                        typeResolutions.put(key, concreteConvert);
                    }
                });
                // If all mapped types were resolved, create a new FieldAttribute with the resolved MultiTypeEsField
                if (typeResolutions.size() == imf.getTypesToIndices().size()) {
                    var resolvedField = resolvedMultiTypeEsField(fa, typeResolutions);
                    return createIfDoesNotAlreadyExist(fa, resolvedField, unionFieldAttributes);
                }
            } else if (convert.field() instanceof AbstractConvertFunction subConvert) {
                return convertExpression.replaceChildren(
                    Collections.singletonList(resolveConvertFunction(subConvert, unionFieldAttributes))
                );
            }
            return convertExpression;
        }

        private Expression createIfDoesNotAlreadyExist(
            FieldAttribute fa,
            MultiTypeEsField resolvedField,
            List<FieldAttribute> unionFieldAttributes
        ) {
            // Generate new ID for the field and suffix it with the data type to maintain unique attribute names.
            // NOTE: The name has to start with $$ to not break bwc with 8.15 - in that version, this is how we had to mark this as
            // synthetic to work around a bug.
            String unionTypedFieldName = Attribute.rawTemporaryName(fa.name(), "converted_to", resolvedField.getDataType().typeName());
            FieldAttribute unionFieldAttribute = new FieldAttribute(fa.source(), fa.parentName(), unionTypedFieldName, resolvedField, true);
            int existingIndex = unionFieldAttributes.indexOf(unionFieldAttribute);
            if (existingIndex >= 0) {
                // Do not generate multiple name/type combinations with different IDs
                return unionFieldAttributes.get(existingIndex);
            } else {
                unionFieldAttributes.add(unionFieldAttribute);
                return unionFieldAttribute;
            }
        }

        private MultiTypeEsField resolvedMultiTypeEsField(FieldAttribute fa, HashMap<TypeResolutionKey, Expression> typeResolutions) {
            Map<String, Expression> typesToConversionExpressions = new HashMap<>();
            InvalidMappedField imf = (InvalidMappedField) fa.field();
            imf.getTypesToIndices().forEach((typeName, indexNames) -> {
                DataType type = DataType.fromTypeName(typeName);
                TypeResolutionKey key = new TypeResolutionKey(fa.name(), type);
                if (typeResolutions.containsKey(key)) {
                    typesToConversionExpressions.put(typeName, typeResolutions.get(key));
                }
            });
            return MultiTypeEsField.resolveFrom(imf, typesToConversionExpressions);
        }

        private Expression typeSpecificConvert(ConvertFunction convert, Source source, DataType type, InvalidMappedField mtf) {
            EsField field = new EsField(mtf.getName(), type, mtf.getProperties(), mtf.isAggregatable());
            FieldAttribute originalFieldAttr = (FieldAttribute) convert.field();
            FieldAttribute resolvedAttr = new FieldAttribute(
                source,
                originalFieldAttr.parentName(),
                originalFieldAttr.name(),
                field,
                originalFieldAttr.nullable(),
                originalFieldAttr.id(),
                true
            );
            Expression e = ((Expression) convert).replaceChildren(Collections.singletonList(resolvedAttr));
            /*
             * Resolve surrogates immediately because these type specific conversions are serialized
             * and SurrogateExpressions are expected to be resolved on the coordinating node. At least,
             * TO_IP is expected to be resolved there.
             */
            return SubstituteSurrogateExpressions.rule(e);
        }
    }

    /**
     * {@link ResolveUnionTypes} creates new, synthetic attributes for union types:
     * If there was no {@code AbstractConvertFunction} that resolved multi-type fields in the {@link ResolveUnionTypes} rule,
     * then there could still be some {@code FieldAttribute}s that contain unresolved {@link MultiTypeEsField}s.
     * These need to be converted back to actual {@code UnresolvedAttribute} in order for validation to generate appropriate failures.
     * <p>
     * Finally, if {@code client_ip} is present in 2 indices, once with type {@code ip} and once with type {@code keyword},
     * using {@code EVAL x = to_ip(client_ip)} will create a single attribute @{code $$client_ip$converted_to$ip}.
     * This should not spill into the query output, so we drop such attributes at the end.
     */
    private static class UnionTypesCleanup extends Rule<LogicalPlan, LogicalPlan> {
        public LogicalPlan apply(LogicalPlan plan) {
            LogicalPlan planWithCheckedUnionTypes = plan.transformUp(
                LogicalPlan.class,
                p -> p.transformExpressionsOnly(FieldAttribute.class, UnionTypesCleanup::checkUnresolved)
            );

            // To drop synthetic attributes at the end, we need to compute the plan's output.
            // This is only legal to do if the plan is resolved.
            return planWithCheckedUnionTypes.resolved()
                ? planWithoutSyntheticAttributes(planWithCheckedUnionTypes)
                : planWithCheckedUnionTypes;
        }

        static Attribute checkUnresolved(FieldAttribute fa) {
            if (fa.field() instanceof InvalidMappedField imf) {
                String unresolvedMessage = "Cannot use field [" + fa.name() + "] due to ambiguities being " + imf.errorMessage();
                List<String> types = imf.getTypesToIndices().keySet().stream().toList();
                return new UnsupportedAttribute(
                    fa.source(),
                    fa.name(),
                    new UnsupportedEsField(imf.getName(), types),
                    unresolvedMessage,
                    fa.id()
                );
            }
            return fa;
        }

        private static LogicalPlan planWithoutSyntheticAttributes(LogicalPlan plan) {
            List<Attribute> output = plan.output();
            List<Attribute> newOutput = new ArrayList<>(output.size());

            for (Attribute attr : output) {
                // Do not let the synthetic union type field attributes end up in the final output.
                if (attr.synthetic() && attr instanceof FieldAttribute) {
                    continue;
                }
                newOutput.add(attr);
            }

            return newOutput.size() == output.size() ? plan : new Project(Source.EMPTY, plan, newOutput);
        }
    }
}
