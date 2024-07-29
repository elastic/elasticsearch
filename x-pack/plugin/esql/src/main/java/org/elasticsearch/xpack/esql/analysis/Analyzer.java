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
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules.BaseAnalyzerRule;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules.ParameterizedAnalyzerRule;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.EmptyAttribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedStar;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.index.EsIndex;
import org.elasticsearch.xpack.esql.core.plan.TableIdentifier;
import org.elasticsearch.xpack.esql.core.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.core.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.esql.core.rule.Rule;
import org.elasticsearch.xpack.esql.core.rule.RuleExecutor;
import org.elasticsearch.xpack.esql.core.session.Configuration;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.DateTimeArithmeticOperation;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Lookup;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.Stats;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.stats.FeatureMetric;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.elasticsearch.xpack.esql.type.MultiTypeEsField;

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

import static java.util.Collections.singletonList;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.core.enrich.EnrichPolicy.GEO_MATCH_TYPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.NESTED;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.core.type.DataType.isTemporalAmount;
import static org.elasticsearch.xpack.esql.stats.FeatureMetric.LIMIT;

/**
 * This class is part of the planner. Resolves references (such as variable and index names) and performs implicit casting.
 */
public class Analyzer extends ParameterizedRuleExecutor<LogicalPlan, AnalyzerContext> {
    // marker list of attributes for plans that do not have any concrete fields to return, but have other computed columns to return
    // ie from test | stats c = count(*)
    public static final List<Attribute> NO_FIELDS = List.of(
        new ReferenceAttribute(Source.EMPTY, "<no-fields>", DataType.NULL, null, Nullability.TRUE, null, true)
    );
    private static final Iterable<RuleExecutor.Batch<LogicalPlan>> rules;

    static {
        var init = new Batch<>(
            "Initialize",
            Limiter.ONCE,
            new ResolveTable(),
            new ResolveEnrich(),
            new ResolveLookupTables(),
            new ResolveFunctions()
        );
        var resolution = new Batch<>(
            "Resolution",
            new ResolveRefs(),
            new ResolveUnionTypes(),  // Must be after ResolveRefs, so union types can be found
            new ImplicitCasting()
        );
        var finish = new Batch<>("Finish Analysis", Limiter.ONCE, new AddImplicitLimit(), new UnionTypesCleanup());
        rules = List.of(init, resolution, finish);
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

    private static class ResolveTable extends ParameterizedAnalyzerRule<UnresolvedRelation, AnalyzerContext> {

        @Override
        protected LogicalPlan rule(UnresolvedRelation plan, AnalyzerContext context) {
            if (context.indexResolution().isValid() == false) {
                return plan.unresolvedMessage().equals(context.indexResolution().toString())
                    ? plan
                    : new UnresolvedRelation(
                        plan.source(),
                        plan.table(),
                        plan.frozen(),
                        plan.metadataFields(),
                        plan.indexMode(),
                        context.indexResolution().toString()
                    );
            }
            TableIdentifier table = plan.table();
            if (context.indexResolution().matches(table.index()) == false) {
                // TODO: fix this (and tests), or drop check (seems SQL-inherited, where's also defective)
                new UnresolvedRelation(
                    plan.source(),
                    plan.table(),
                    plan.frozen(),
                    plan.metadataFields(),
                    plan.indexMode(),
                    "invalid [" + table + "] resolution to [" + context.indexResolution() + "]"
                );
            }

            EsIndex esIndex = context.indexResolution().get();
            var attributes = mappingAsAttributes(plan.source(), esIndex.mapping());
            attributes.addAll(plan.metadataFields());
            return new EsRelation(plan.source(), esIndex, attributes.isEmpty() ? NO_FIELDS : attributes, plan.indexMode());
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

    private static void mappingAsAttributes(List<Attribute> list, Source source, FieldAttribute parent, Map<String, EsField> mapping) {
        for (Map.Entry<String, EsField> entry : mapping.entrySet()) {
            String name = entry.getKey();
            EsField t = entry.getValue();

            if (t != null) {
                name = parent == null ? name : parent.fieldName() + "." + name;
                var fieldProperties = t.getProperties();
                var type = t.getDataType().widenSmallNumeric();
                // due to a bug also copy the field since the Attribute hierarchy extracts the data type
                // directly even if the data type is passed explicitly
                if (type != t.getDataType()) {
                    t = new EsField(t.getName(), type, t.getProperties(), t.isAggregatable(), t.isAlias());
                }

                FieldAttribute attribute = t instanceof UnsupportedEsField uef
                    ? new UnsupportedAttribute(source, name, uef)
                    : new FieldAttribute(source, parent, name, t);
                // primitive branch
                if (DataType.isPrimitive(type)) {
                    list.add(attribute);
                }
                // allow compound object even if they are unknown (but not NESTED)
                if (type != NESTED && fieldProperties.isEmpty() == false) {
                    mappingAsAttributes(list, source, attribute, fieldProperties);
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
            final String policyName = (String) plan.policyName().fold();
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
                var policyNameExp = new UnresolvedAttribute(plan.policyName().source(), policyName, null, error);
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
                return new UnresolvedAttribute(source, enrichFieldName, null, msg);
            } else {
                return new ReferenceAttribute(source, enrichFieldName, mappedField.dataType(), null, Nullability.TRUE, null, false);
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
                tableNameExpression = new UnresolvedAttribute(tableNameExpression.source(), tableName, null, message);
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

    public static class ResolveRefs extends BaseAnalyzerRule {
        @Override
        protected LogicalPlan doRule(LogicalPlan plan) {
            if (plan.childrenResolved() == false) {
                return plan;
            }
            final List<Attribute> childrenOutput = new ArrayList<>();

            for (LogicalPlan child : plan.children()) {
                var output = child.output();
                childrenOutput.addAll(output);
            }

            if (plan instanceof Stats stats) {
                return resolveStats(stats, childrenOutput);
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

            if (plan instanceof Lookup l) {
                return resolveLookup(l, childrenOutput);
            }

            return plan.transformExpressionsOnly(UnresolvedAttribute.class, ua -> maybeResolveAttribute(ua, childrenOutput));
        }

        private LogicalPlan resolveStats(Stats stats, List<Attribute> childrenOutput) {
            // if the grouping is resolved but the aggs are not, use the former to resolve the latter
            // e.g. STATS a ... GROUP BY a = x + 1
            Holder<Boolean> changed = new Holder<>(false);
            List<Expression> groupings = stats.groupings();
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
                    stats = stats.with(newGroupings, stats.aggregates());
                    changed.set(false);
                }
            }

            if (stats.expressionsResolved() == false) {
                AttributeMap<Expression> resolved = new AttributeMap<>();
                for (Expression e : groupings) {
                    Attribute attr = Expressions.attribute(e);
                    if (attr != null && attr.resolved()) {
                        resolved.put(attr, attr);
                    }
                }
                List<Attribute> resolvedList = NamedExpressions.mergeOutputAttributes(new ArrayList<>(resolved.keySet()), childrenOutput);
                List<NamedExpression> newAggregates = new ArrayList<>();

                for (NamedExpression aggregate : stats.aggregates()) {
                    var agg = (NamedExpression) aggregate.transformUp(UnresolvedAttribute.class, ua -> {
                        Expression ne = ua;
                        Attribute maybeResolved = maybeResolveAttribute(ua, resolvedList);
                        if (maybeResolved != null) {
                            changed.set(true);
                            ne = maybeResolved;
                        }
                        return ne;
                    });
                    newAggregates.add(agg);
                }

                stats = changed.get() ? stats.with(groupings, newAggregates) : stats;
            }

            return (LogicalPlan) stats;
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
                                    attr.qualifier(),
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
                        String suffix = "only " + Arrays.toString(allowed) + " allowed for type [" + matchType + "]";
                        resolved = ua.withUnresolvedMessage(
                            "Unsupported type [" + resolved.dataType() + "] for enrich matching field [" + ua.name() + "]; " + suffix
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
        UnresolvedAttribute ua = new UnresolvedAttribute(up.source(), up.pattern(), null);
        Predicate<Attribute> matcher = a -> up.match(a.name()) || up.match(a.qualifiedName());
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

    private BitSet gatherPreAnalysisMetrics(LogicalPlan plan, BitSet b) {
        // count only the explicit "limit" the user added, otherwise all queries will have a "limit" and telemetry won't reflect reality
        if (plan.collectFirstChildren(Limit.class::isInstance).isEmpty() == false) {
            b.set(LIMIT.ordinal());
        }
        plan.forEachDown(p -> FeatureMetric.set(p, b));
        return b;
    }

    private static class ImplicitCasting extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {
        @Override
        public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
            return plan.transformExpressionsUp(ScalarFunction.class, e -> ImplicitCasting.cast(e, context.functionRegistry()));
        }

        private static Expression cast(ScalarFunction f, EsqlFunctionRegistry registry) {
            if (f instanceof In in) {
                return processIn(in);
            }
            if (f instanceof EsqlScalarFunction esf) {
                return processScalarFunction(esf, registry);
            }
            if (f instanceof EsqlArithmeticOperation || f instanceof BinaryComparison) {
                return processBinaryOperator((BinaryOperator) f);
            }
            return f;
        }

        private static Expression processScalarFunction(EsqlScalarFunction f, EsqlFunctionRegistry registry) {
            List<Expression> args = f.arguments();
            List<DataType> targetDataTypes = registry.getDataTypeForStringLiteralConversion(f.getClass());
            if (targetDataTypes == null || targetDataTypes.isEmpty()) {
                return f;
            }
            List<Expression> newChildren = new ArrayList<>(args.size());
            boolean childrenChanged = false;
            DataType targetDataType = DataType.NULL;
            Expression arg;
            for (int i = 0; i < args.size(); i++) {
                arg = args.get(i);
                if (arg.resolved() && arg.dataType() == KEYWORD && arg.foldable() && ((arg instanceof EsqlScalarFunction) == false)) {
                    if (i < targetDataTypes.size()) {
                        targetDataType = targetDataTypes.get(i);
                    }
                    if (targetDataType != DataType.NULL && targetDataType != DataType.UNSUPPORTED) {
                        Expression e = castStringLiteral(arg, targetDataType);
                        childrenChanged = true;
                        newChildren.add(e);
                        continue;
                    }
                }
                newChildren.add(args.get(i));
            }
            return childrenChanged ? f.replaceChildren(newChildren) : f;
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
                if (supportsImplicitCasting(right.dataType())) {
                    targetDataType = right.dataType();
                    from = left;
                } else if (supportsImplicitTemporalCasting(right, o)) {
                    targetDataType = DATETIME;
                    from = left;
                }
            }
            if (right.dataType() == KEYWORD && right.foldable() && (right instanceof EsqlScalarFunction == false)) {
                if (supportsImplicitCasting(left.dataType())) {
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

            if (left.resolved() == false || supportsImplicitCasting(left.dataType()) == false) {
                return in;
            }
            List<Expression> newChildren = new ArrayList<>(right.size() + 1);
            boolean childrenChanged = false;

            for (Expression value : right) {
                if (value.dataType() == KEYWORD && value.foldable()) {
                    Expression e = castStringLiteral(value, left.dataType());
                    newChildren.add(e);
                    childrenChanged = true;
                } else {
                    newChildren.add(value);
                }
            }
            newChildren.add(left);
            return childrenChanged ? in.replaceChildren(newChildren) : in;
        }

        private static boolean supportsImplicitTemporalCasting(Expression e, BinaryOperator<?, ?, ?, ?> o) {
            return isTemporalAmount(e.dataType()) && (o instanceof DateTimeArithmeticOperation);
        }

        private static boolean supportsImplicitCasting(DataType type) {
            return type == DATETIME || type == IP || type == VERSION || type == BOOLEAN;
        }

        public static Expression castStringLiteral(Expression from, DataType target) {
            assert from.foldable();
            try {
                Object to = EsqlDataTypeConverter.convert(from.fold(), target);
                return new Literal(from.source(), to, target);
            } catch (Exception e) {
                String message = LoggerMessageFormat.format(
                    "Cannot convert string [{}] to [{}], error [{}]",
                    from.fold(),
                    target,
                    e.getMessage()
                );
                return new UnsupportedAttribute(
                    from.source(),
                    String.valueOf(from.fold()),
                    new UnsupportedEsField(String.valueOf(from.fold()), from.dataType().typeName()),
                    message
                );
            }
        }
    }

    /**
     * The EsqlIndexResolver will create InvalidMappedField instances for fields that are ambiguous (i.e. have multiple mappings).
     * During ResolveRefs we do not convert these to UnresolvedAttribute instances, as we want to first determine if they can
     * instead be handled by conversion functions within the query. This rule looks for matching conversion functions and converts
     * those fields into MultiTypeEsField, which encapsulates the knowledge of how to convert these into a single type.
     * This knowledge will be used later in generating the FieldExtractExec with built-in type conversion.
     * Any fields which could not be resolved by conversion functions will be converted to UnresolvedAttribute instances in a later rule
     * (See UnresolveUnionTypes below).
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

            return plan.transformUp(LogicalPlan.class, p -> p.resolved() || p.childrenResolved() == false ? p : doRule(p));
        }

        private LogicalPlan doRule(LogicalPlan plan) {
            int alreadyAddedUnionFieldAttributes = unionFieldAttributes.size();
            // See if the eval function has an unresolved MultiTypeEsField field
            // Replace the entire convert function with a new FieldAttribute (containing type conversion knowledge)
            plan = plan.transformExpressionsOnly(
                AbstractConvertFunction.class,
                convert -> resolveConvertFunction(convert, unionFieldAttributes)
            );
            // If no union fields were generated, return the plan as is
            if (unionFieldAttributes.size() == alreadyAddedUnionFieldAttributes) {
                return plan;
            }

            // In ResolveRefs the aggregates are resolved from the groupings, which might have an unresolved MultiTypeEsField.
            // Now that we have resolved those, we need to re-resolve the aggregates.
            if (plan instanceof Aggregate agg) {
                // TODO once inlinestats supports expressions in groups we'll likely need the same sort of extraction here
                // If the union-types resolution occurred in a child of the aggregate, we need to check the groupings
                plan = agg.transformExpressionsOnly(FieldAttribute.class, UnionTypesCleanup::checkUnresolved);

                // Aggregates where the grouping key comes from a union-type field need to be resolved against the grouping key
                Map<Attribute, Expression> resolved = new HashMap<>();
                for (Expression e : agg.groupings()) {
                    Attribute attr = Expressions.attribute(e);
                    if (attr != null && attr.resolved()) {
                        resolved.put(attr, e);
                    }
                }
                plan = plan.transformExpressionsOnly(UnresolvedAttribute.class, ua -> resolveAttribute(ua, resolved));
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
                    List<Attribute> newOutput = new ArrayList<>(esr.output());
                    newOutput.addAll(missing);
                    return new EsRelation(esr.source(), esr.index(), newOutput, esr.indexMode(), esr.frozen());
                }
                return esr;
            });
            return plan;
        }

        private Expression resolveAttribute(UnresolvedAttribute ua, Map<Attribute, Expression> resolved) {
            var named = resolveAgainstList(ua, resolved.keySet());
            return switch (named.size()) {
                case 0 -> ua;
                case 1 -> named.get(0).equals(ua) ? ua : resolved.get(named.get(0));
                default -> ua.withUnresolvedMessage("Resolved [" + ua + "] unexpectedly to multiple attributes " + named);
            };
        }

        private Expression resolveConvertFunction(AbstractConvertFunction convert, List<FieldAttribute> unionFieldAttributes) {
            if (convert.field() instanceof FieldAttribute fa && fa.field() instanceof InvalidMappedField imf) {
                HashMap<TypeResolutionKey, Expression> typeResolutions = new HashMap<>();
                Set<DataType> supportedTypes = convert.supportedTypes();
                imf.getTypesToIndices().keySet().forEach(typeName -> {
                    DataType type = DataType.fromTypeName(typeName);
                    if (supportedTypes.contains(type)) {
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
                return convert.replaceChildren(Collections.singletonList(resolveConvertFunction(subConvert, unionFieldAttributes)));
            }
            return convert;
        }

        private Expression createIfDoesNotAlreadyExist(
            FieldAttribute fa,
            MultiTypeEsField resolvedField,
            List<FieldAttribute> unionFieldAttributes
        ) {
            // Generate new ID for the field and suffix it with the data type to maintain unique attribute names.
            String unionTypedFieldName = LogicalPlanOptimizer.rawTemporaryName(
                fa.name(),
                "converted_to",
                resolvedField.getDataType().typeName()
            );
            FieldAttribute unionFieldAttribute = new FieldAttribute(fa.source(), fa.parent(), unionTypedFieldName, resolvedField);
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

        private Expression typeSpecificConvert(AbstractConvertFunction convert, Source source, DataType type, InvalidMappedField mtf) {
            EsField field = new EsField(mtf.getName(), type, mtf.getProperties(), mtf.isAggregatable());
            NameId id = ((FieldAttribute) convert.field()).id();
            FieldAttribute resolvedAttr = new FieldAttribute(source, null, field.getName(), field, null, Nullability.TRUE, id, false);
            return convert.replaceChildren(Collections.singletonList(resolvedAttr));
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
            LogicalPlan planWithCheckedUnionTypes = plan.transformUp(LogicalPlan.class, p -> {
                if (p instanceof EsRelation esRelation) {
                    // Leave esRelation as InvalidMappedField so that UNSUPPORTED fields can still pass through
                    return esRelation;
                }
                return p.transformExpressionsOnly(FieldAttribute.class, UnionTypesCleanup::checkUnresolved);
            });

            // To drop synthetic attributes at the end, we need to compute the plan's output.
            // This is only legal to do if the plan is resolved.
            return planWithCheckedUnionTypes.resolved()
                ? planWithoutSyntheticAttributes(planWithCheckedUnionTypes)
                : planWithCheckedUnionTypes;
        }

        static Attribute checkUnresolved(FieldAttribute fa) {
            if (fa.field() instanceof InvalidMappedField imf) {
                String unresolvedMessage = "Cannot use field [" + fa.name() + "] due to ambiguities being " + imf.errorMessage();
                return new UnresolvedAttribute(fa.source(), fa.name(), fa.qualifier(), fa.id(), unresolvedMessage, null);
            }
            return fa;
        }

        private static LogicalPlan planWithoutSyntheticAttributes(LogicalPlan plan) {
            List<Attribute> output = plan.output();
            List<Attribute> newOutput = new ArrayList<>(output.size());

            for (Attribute attr : output) {
                // TODO: this should really use .synthetic()
                // https://github.com/elastic/elasticsearch/issues/105821
                if (attr.name().startsWith(FieldAttribute.SYNTHETIC_ATTRIBUTE_NAME_PREFIX) == false) {
                    newOutput.add(attr);
                }
            }

            return newOutput.size() == output.size() ? plan : new Project(Source.EMPTY, plan, newOutput);
        }
    }
}
