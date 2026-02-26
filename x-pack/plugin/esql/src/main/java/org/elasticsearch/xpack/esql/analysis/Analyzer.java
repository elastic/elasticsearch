/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules.ParameterizedAnalyzerRule;
import org.elasticsearch.xpack.esql.analysis.rules.ResolveUnmapped;
import org.elasticsearch.xpack.esql.analysis.rules.ResolvedProjects;
import org.elasticsearch.xpack.esql.capabilities.ConfigurationAware;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.EmptyAttribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedMetadataAttributeExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedPattern;
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
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern;
import org.elasticsearch.xpack.esql.expression.function.AggregateMetricDoubleNativeSupport;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Absent;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AbsentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AvgOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MaxOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MinOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Present;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PresentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SumOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SummationMode;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.expression.function.inference.CompletionFunction;
import org.elasticsearch.xpack.esql.expression.function.inference.InferenceFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Greatest;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Least;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ConvertFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FoldablesConvertFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FromAggregateMetricDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToAggregateMetricDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDateNanos;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDenseVector;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToUnsignedLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvCount;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.vector.VectorFunction;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.DateTimeArithmeticOperation;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.ResolvedInference;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SubstituteSurrogateExpressions;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Insist;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Lookup;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.fuse.Fuse;
import org.elasticsearch.xpack.esql.plan.logical.fuse.FuseScoreEval;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinType;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.ResolvingProject;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.core.enrich.EnrichPolicy.GEO_MATCH_TYPE;
import static org.elasticsearch.xpack.esql.capabilities.TranslationAware.translatable;
import static org.elasticsearch.xpack.esql.core.expression.Expressions.toReferenceAttributes;
import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TIME_DURATION;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.core.type.DataType.isTemporalAmount;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.LIMIT;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.STATS;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.maybeParseTemporalAmount;

/**
 * This class is part of the planner. Resolves references (such as variable and index names) and performs implicit casting.
 */
public class Analyzer extends ParameterizedRuleExecutor<LogicalPlan, AnalyzerContext> {
    // marker list of attributes for plans that do not have any concrete fields to return, but have other computed columns to return
    // ie from test | stats c = count(*)
    public static final String NO_FIELDS_NAME = "<no-fields>";
    public static final List<Attribute> NO_FIELDS = List.of(
        new ReferenceAttribute(Source.EMPTY, null, NO_FIELDS_NAME, NULL, Nullability.TRUE, null, true)
    );

    private static final List<Batch<LogicalPlan>> RULES = List.of(
        new Batch<>(
            "Initialize",
            Limiter.ONCE,
            new ResolveConfigurationAware(),
            new ResolveTable(),
            new ResolveExternalRelations(),
            new PruneEmptyUnionAllBranch(),
            new ResolveEnrich(),
            new ResolveLookupTables(),
            new ResolveFunctions(),
            new ResolveInference(),
            new DateMillisToNanosInEsRelation()
        ),
        new Batch<>(
            "Resolution",
            new ResolveRefs(),
            new ImplicitCasting(),
            new ResolveUnionTypes(),  // Must be after ResolveRefs, so union types can be found
            new InsertDefaultInnerTimeSeriesAggregate(),
            new ImplicitCastAggregateMetricDoubles(),
            new InsertFromAggregateMetricDouble(),
            new TimeSeriesGroupByAll(),
            new ResolveUnionTypesInUnionAll(),
            new ResolveUnmapped()
        ),
        new Batch<>(
            "Finish Analysis",
            Limiter.ONCE,
            new ResolvedProjects(),
            new AddImplicitLimit(),
            new AddImplicitTimestampSort(),
            new AddImplicitForkLimit(),
            new UnionTypesCleanup()
        )
    );
    public static final TransportVersion ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION = TransportVersion.fromName(
        "esql_lookup_join_full_text_function"
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
            IndexResolution indexResolution = plan.indexMode().equals(IndexMode.LOOKUP)
                ? context.lookupResolution().get(plan.indexPattern().indexPattern())
                : context.indexResolution().get(plan.indexPattern());
            return resolveIndex(plan, indexResolution, context);
        }

        private LogicalPlan resolveIndex(UnresolvedRelation plan, IndexResolution indexResolution, AnalyzerContext context) {
            List<NamedExpression> metadata = resolveMetadata(plan.metadataFields(), context);
            if (indexResolution == null || indexResolution.isValid() == false) {
                String indexResolutionMessage = indexResolution == null ? "[none specified]" : indexResolution.toString();
                return plan.unresolvedMessage().equals(indexResolutionMessage)
                    ? plan
                    : new UnresolvedRelation(
                        plan.source(),
                        plan.indexPattern(),
                        plan.frozen(),
                        metadata,
                        plan.indexMode(),
                        indexResolutionMessage,
                        plan.telemetryLabel()
                    );
            }
            // assert indexResolution.matches(plan.indexPattern().indexPattern()) : "Expected index resolution to match the index pattern";
            IndexPattern table = plan.indexPattern();
            if (indexResolution.matches(table.indexPattern()) == false) {
                // TODO: fix this (and tests), or drop check (seems SQL-inherited, where's also defective)
                new UnresolvedRelation(
                    plan.source(),
                    plan.indexPattern(),
                    plan.frozen(),
                    metadata,
                    plan.indexMode(),
                    "invalid [" + table + "] resolution to [" + indexResolution + "]",
                    plan.telemetryLabel()
                );
            }

            if (metadata.stream().anyMatch(x -> x.resolved() == false)) {
                return new UnresolvedRelation(
                    plan.source(),
                    plan.indexPattern(),
                    plan.frozen(),
                    metadata,
                    plan.indexMode(),
                    "unresolved metadata fields: " + metadata.stream().filter(x -> x.resolved() == false).toList(),
                    plan.telemetryLabel()
                );
            }

            EsIndex esIndex = indexResolution.get();

            var attributes = mappingAsAttributes(plan.source(), esIndex.mapping());
            attributes.addAll(metadata.stream().map(NamedExpression::toAttribute).toList());
            return new EsRelation(
                plan.source(),
                esIndex.name(),
                plan.indexMode(),
                esIndex.originalIndices(),
                esIndex.concreteIndices(),
                esIndex.indexNameWithModes(),
                attributes.isEmpty() ? NO_FIELDS : attributes
            );
        }

        private List<NamedExpression> resolveMetadata(List<NamedExpression> metadata, AnalyzerContext context) {
            LinkedHashMap<String, NamedExpression> resolved = new LinkedHashMap<>();
            Set<String> allTags = null;
            for (NamedExpression item : metadata) {
                switch (item) {
                    case MetadataAttribute ma -> {
                        resolved.remove(ma.name());
                        resolved.put(ma.name(), ma);
                    }
                    case UnresolvedMetadataAttributeExpression um -> {
                        if (allTags == null) {
                            allTags = context.allowedTags();
                        }
                        List<? extends NamedExpression> resolvedItems = tryResolveMetadata(um, allTags);
                        if (resolvedItems.isEmpty()) {
                            resolved.put(um.pattern(), um); // unresolved
                        } else {
                            for (NamedExpression resolvedItem : resolvedItems) {
                                resolved.remove(resolvedItem.name()); // last one wins
                                resolved.put(resolvedItem.name(), resolvedItem);
                            }
                        }
                    }
                    default -> throw new IllegalStateException("Unexpected metadata type: " + item.getClass().getName());
                }
            }
            return resolved.values().stream().toList();
        }

        private List<NamedExpression> tryResolveMetadata(UnresolvedMetadataAttributeExpression um, Set<String> allowedTags) {
            Pattern pattern = Pattern.compile(StringUtils.wildcardToJavaPattern(um.pattern(), '\\'));
            List<String> matchingMetadata = allowedTags.stream().filter(x -> pattern.matcher(x).matches()).sorted().toList();
            List<NamedExpression> result = new ArrayList<>();
            for (String item : matchingMetadata) {
                // See if it's a known metadata attribute (we know the type there)
                NamedExpression attribute = MetadataAttribute.create(um.source(), item);
                if (attribute instanceof UnresolvedMetadataAttributeExpression) {
                    // we don't know the type here, but for now we only have keywords as custom tags
                    attribute = new MetadataAttribute(um.source(), item, KEYWORD, false);
                }
                result.add(attribute);
            }
            return result;
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
                    t = new EsField(t.getName(), type, t.getProperties(), t.isAggregatable(), t.isAlias(), t.getTimeSeriesFieldType());
                }

                FieldAttribute attribute = t instanceof UnsupportedEsField uef
                    ? new UnsupportedAttribute(source, name, uef)
                    : new FieldAttribute(source, parentName, null, name, t);
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

    /**
     * Resolves UnresolvedExternalRelation nodes using pre-resolved metadata from ExternalSourceResolver.
     * This rule mirrors the ResolveTable pattern but uses ExternalSourceResolution instead of IndexResolution.
     * <p>
     * This rule creates {@link ExternalRelation} nodes from any SourceMetadata,
     * avoiding the need for source-specific logical plan nodes in core ESQL code.
     */
    private static class ResolveExternalRelations extends ParameterizedAnalyzerRule<UnresolvedExternalRelation, AnalyzerContext> {

        @Override
        protected LogicalPlan rule(UnresolvedExternalRelation plan, AnalyzerContext context) {
            // Extract the table path from the expression
            String tablePath = extractTablePath(plan.tablePath());
            if (tablePath == null) {
                // Path is not a simple literal (e.g., it's a parameter reference)
                // Return the plan as-is for now
                return plan;
            }

            // Get pre-resolved source (metadata + file set) from context
            var resolvedSource = context.externalSourceResolution().resolvedSource(tablePath);
            if (resolvedSource == null) {
                // Still unresolved - return as-is to keep the error message
                return plan;
            }

            var metadata = resolvedSource.metadata();
            return new ExternalRelation(plan.source(), tablePath, metadata, metadata.schema(), resolvedSource.fileSet());
        }

        private String extractTablePath(Expression tablePath) {
            if (tablePath instanceof Literal literal && literal.value() != null) {
                Object value = literal.value();
                if (value instanceof org.apache.lucene.util.BytesRef) {
                    return BytesRefs.toString((org.apache.lucene.util.BytesRef) value);
                }
                return value.toString();
            }
            return null;
        }
    }

    private static class ResolveEnrich extends ParameterizedAnalyzerRule<Enrich, AnalyzerContext> {

        @Override
        protected LogicalPlan rule(Enrich plan, AnalyzerContext context) {
            if (plan.policyName().resolved() == false) {
                // the policy does not exist
                return plan;
            }
            final String policyName = BytesRefs.toString(plan.policyName().fold(FoldContext.small() /* TODO remove me */));
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
                return new ReferenceAttribute(source, null, enrichFieldName, mappedField.dataType(), Nullability.TRUE, null, false);
            }
        }
    }

    private static class ResolveLookupTables extends ParameterizedAnalyzerRule<Lookup, AnalyzerContext> {

        @Override
        protected LogicalPlan rule(Lookup lookup, AnalyzerContext context) {
            // the parser passes the string wrapped in a literal
            Source source = lookup.source();
            Expression tableNameExpression = lookup.tableName();
            String tableName = BytesRefs.toString(tableNameExpression.fold(FoldContext.small() /* TODO remove me */));
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
                EsField field = new EsField(name, column.type(), Map.of(), false, false, EsField.TimeSeriesFieldType.UNKNOWN);
                attributes.add(new FieldAttribute(source, null, null, name, field));
                // prepare the block for the supplier
                blocks[i++] = column.values();
            }
            LocalSupplier supplier = LocalSupplier.of(blocks.length > 0 ? new Page(blocks) : new Page(0));
            return new LocalRelation(source, attributes, supplier);
        }
    }

    public static class ResolveRefs extends ParameterizedAnalyzerRule<LogicalPlan, AnalyzerContext> {
        @Override
        protected LogicalPlan rule(LogicalPlan plan, AnalyzerContext context) {
            if (plan.childrenResolved() == false) {
                return plan;
            }
            // TODO: assess if building this list is still required ahead of the switch, or if it can be done per command only where needed
            final List<Attribute> childrenOutput = new ArrayList<>();

            // Gather all the children's output in case of non-unary plans; even for unaries, we need to copy because we may mutate this to
            // simplify resolution of e.g. RENAME.
            for (LogicalPlan child : plan.children()) {
                var output = child.output();
                childrenOutput.addAll(output);
            }

            var resolved = switch (plan) {
                case Aggregate a -> resolveAggregate(a, childrenOutput);
                case Completion c -> resolveCompletion(c, childrenOutput);
                case Drop d -> resolveDrop(d, context.unmappedResolution());
                case Rename r -> resolveRename(r, context.unmappedResolution());
                case Keep k -> resolveKeep(k, context.unmappedResolution());
                case Fork f -> resolveFork(f);
                case Eval p -> resolveEval(p, childrenOutput);
                case Enrich p -> resolveEnrich(p, childrenOutput);
                case MvExpand p -> resolveMvExpand(p, childrenOutput);
                case Lookup l -> resolveLookup(l, childrenOutput);
                case LookupJoin j -> resolveLookupJoin(j, context);
                case Insist i -> resolveInsist(i, childrenOutput, context);
                case Fuse fuse -> resolveFuse(fuse, childrenOutput);
                case Rerank r -> resolveRerank(r, childrenOutput, context);
                case PromqlCommand promql -> resolvePromql(promql, childrenOutput);
                default -> plan.transformExpressionsOnly(UnresolvedAttribute.class, ua -> maybeResolveAttribute(ua, childrenOutput));
            };

            return context.unmappedResolution() == UnmappedResolution.LOAD ? resolvePartiallyMapped(resolved, context) : resolved;
        }

        private LogicalPlan resolveAggregate(Aggregate aggregate, List<Attribute> childrenOutput) {
            // if the grouping is resolved but the aggs are not, use the former to resolve the latter
            // e.g. STATS a ... GROUP BY a = x + 1
            // first resolve groupings since the aggs might refer to them
            // trying to globally resolve unresolved attributes will lead to some being marked as unresolvable
            List<Expression> newGroupings = maybeResolveGroupings(aggregate, childrenOutput);
            List<? extends NamedExpression> newAggregates = maybeResolveAggregates(aggregate, newGroupings, childrenOutput);
            boolean changed = newGroupings != aggregate.groupings() || newAggregates != aggregate.aggregates();
            LogicalPlan maybeNewAggregate = changed ? aggregate.with(aggregate.child(), newGroupings, newAggregates) : aggregate;

            return maybeNewAggregate instanceof TimeSeriesAggregate ts && ts.timestamp() instanceof UnresolvedAttribute unresolvedTimestamp
                ? ts.withTimestamp(maybeResolveAttribute(unresolvedTimestamp, childrenOutput))
                : maybeNewAggregate;

        }

        private List<Expression> maybeResolveGroupings(Aggregate aggregate, List<Attribute> childrenOutput) {
            List<Expression> groupings = aggregate.groupings();

            if (Resolvables.resolved(groupings) == false) {
                Holder<Boolean> changed = new Holder<>(false);
                List<Expression> newGroupings = new ArrayList<>(groupings.size());
                Function<UnresolvedAttribute, Expression> resolve = ua -> maybeResolveAttribute(ua, childrenOutput);
                for (Expression g : groupings) {
                    Expression resolved = g.transformUp(UnresolvedAttribute.class, resolve);
                    if (resolved != g) {
                        changed.set(true);
                    }
                    newGroupings.add(resolved);
                }

                if (changed.get()) {
                    return newGroupings;
                }
            }

            return groupings;
        }

        private List<? extends NamedExpression> maybeResolveAggregates(
            Aggregate aggregate,
            List<Expression> newGroupings,
            List<Attribute> childrenOutput
        ) {
            List<Expression> groupings = aggregate.groupings();
            List<? extends NamedExpression> aggregates = aggregate.aggregates();

            ArrayList<Attribute> resolvedGroupings = new ArrayList<>(newGroupings.size());
            for (Expression e : newGroupings) {
                Attribute attr = Expressions.attribute(e);
                if (attr != null && attr.resolved()) {
                    resolvedGroupings.add(attr);
                }
            }

            boolean allGroupingsResolved = groupings.size() == resolvedGroupings.size();
            if (allGroupingsResolved == false || Resolvables.resolved(aggregates) == false) {
                Holder<Boolean> changed = new Holder<>(false);
                List<Attribute> resolvedList = NamedExpressions.mergeOutputAttributes(resolvedGroupings, childrenOutput);

                List<NamedExpression> newAggregates = new ArrayList<>(aggregates.size());
                // If no groupings are resolved, skip the resolution of the references to groupings in the aggregates, resolve the
                // aggregations that do not reference to groupings, so that the fields/attributes referenced by the aggregations can be
                // resolved, and verifier doesn't report field/reference/column not found errors for them.
                int aggsIndexLimit = resolvedGroupings.isEmpty() ? aggregates.size() - groupings.size() : aggregates.size();
                for (int i = 0; i < aggregates.size(); i++) {
                    NamedExpression maybeResolvedAgg = aggregates.get(i);
                    if (i < aggsIndexLimit) { // Skip resolving references to groupings in the aggs if no groupings are resolved yet.
                        maybeResolvedAgg = (NamedExpression) maybeResolvedAgg.transformUp(UnresolvedAttribute.class, ua -> {
                            Expression ne = ua;
                            Attribute maybeResolved = maybeResolveAttribute(ua, resolvedList);
                            // An item in aggregations can reference to groupings explicitly, if groupings are not resolved yet and
                            // maybeResolved is not resolved, return the original UnresolvedAttribute, so that it has another chance
                            // to get resolved in the next iteration.
                            // For example STATS c = count(emp_no), x = d::int + 1 BY d = (date == "2025-01-01")
                            if (allGroupingsResolved || maybeResolved.resolved()) {
                                changed.set(true);
                                ne = maybeResolved;
                            }
                            return ne;
                        });
                    }
                    newAggregates.add(maybeResolvedAgg);
                }

                if (changed.get()) {
                    return newAggregates;
                }
            }

            return aggregates;
        }

        private LogicalPlan resolveCompletion(Completion p, List<Attribute> childrenOutput) {
            Attribute targetField = p.targetField();
            Expression prompt = p.prompt();

            if (targetField instanceof UnresolvedAttribute ua) {
                targetField = new ReferenceAttribute(ua.source(), null, ua.name(), KEYWORD);
            }

            if (prompt.resolved() == false) {
                prompt = prompt.transformUp(UnresolvedAttribute.class, ua -> maybeResolveAttribute(ua, childrenOutput));
            }

            return new Completion(p.source(), p.child(), p.inferenceId(), p.rowLimit(), prompt, targetField, p.taskSettings());
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
                            resolved.qualifier(),
                            resolved.name(),
                            resolved.dataType(),
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
                                dataTypesOk = joinedAttribute.dataType() == NULL || attr.dataType() == NULL;
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
                                        + "]"
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

        private Expression resolveJoinFiltersAndSwapIfNeeded(
            Expression joinOnCondition,
            AttributeSet leftChildOutput,
            AttributeSet rightChildOutput,
            List<Attribute> leftJoinKeysToPopulate,
            List<Attribute> rightJoinKeysToPopulate,
            AnalyzerContext context
        ) {
            if (joinOnCondition == null) {
                return joinOnCondition;
            }
            List<Expression> filters = Predicates.splitAnd(joinOnCondition);
            List<Attribute> childrenOutput = new ArrayList<>(leftChildOutput);
            childrenOutput.addAll(rightChildOutput);

            List<Expression> resolvedFilters = new ArrayList<>(filters.size());
            for (Expression filter : filters) {
                Expression filterResolved = filter.transformUp(UnresolvedAttribute.class, ua -> maybeResolveAttribute(ua, childrenOutput));
                // Check if the filterResolved contains unresolved attributes, if it does, we cannot process it further
                // and the error message about the unresolved attribute is already appropriate
                if (filterResolved.anyMatch(UnresolvedAttribute.class::isInstance)) {
                    resolvedFilters.add(filterResolved);
                    continue;
                }
                Expression result = resolveAndOrientJoinCondition(
                    filterResolved,
                    leftChildOutput,
                    rightChildOutput,
                    leftJoinKeysToPopulate,
                    rightJoinKeysToPopulate,
                    context
                );
                resolvedFilters.add(result);
            }
            return Predicates.combineAndWithSource(resolvedFilters, joinOnCondition.source());
        }

        /**
         * This function resolves and orients a single join on condition.
         * We support AND of such conditions, here we handle a single child of the AND
         * We support the following 2 cases:
         * 1) Binary comparisons between a left and a right attribute.
         * We resolve all attributes and orient them so that the attribute on the left side of the join
         * is on the left side of the binary comparison
         *  and the attribute from the lookup index is on the right side of the binary comparison
         * 2) A Lucene pushable expression containing only attributes from the lookup side of the join
         * We resolve all attributes in the expression, verify they are from the right side of the join
         * and also verify that the expression is potentially Lucene pushable
         */
        private Expression resolveAndOrientJoinCondition(
            Expression condition,
            AttributeSet leftChildOutput,
            AttributeSet rightChildOutput,
            List<Attribute> leftJoinKeysToPopulate,
            List<Attribute> rightJoinKeysToPopulate,
            AnalyzerContext context
        ) {
            if (condition instanceof EsqlBinaryComparison comp
                && comp.left() instanceof Attribute leftAttr
                && comp.right() instanceof Attribute rightAttr) {

                boolean leftIsFromLeft = leftChildOutput.contains(leftAttr);
                boolean rightIsFromRight = rightChildOutput.contains(rightAttr);

                if (leftIsFromLeft && rightIsFromRight) {
                    leftJoinKeysToPopulate.add(leftAttr);
                    rightJoinKeysToPopulate.add(rightAttr);
                    return comp; // Correct orientation
                }

                boolean leftIsFromRight = rightChildOutput.contains(leftAttr);
                boolean rightIsFromLeft = leftChildOutput.contains(rightAttr);

                if (leftIsFromRight && rightIsFromLeft) {
                    leftJoinKeysToPopulate.add(rightAttr);
                    rightJoinKeysToPopulate.add(leftAttr);
                    return comp.swapLeftAndRight(); // Swapped orientation
                }
            }
            if (context.minimumVersion().supports(ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION) == false) {
                return new UnresolvedAttribute(
                    condition.source(),
                    "unsupported",
                    "Lookup join on condition is not supported on the remote node,"
                        + " consider upgrading the remote node. Unsupported join filter expression:"
                        + condition.sourceText()
                );
            }
            return handleRightOnlyPushableFilter(condition, rightChildOutput);
        }

        private Expression handleRightOnlyPushableFilter(Expression condition, AttributeSet rightChildOutput) {
            if (isCompletelyRightSideAndTranslatable(condition, rightChildOutput)) {
                // The condition is completely on the right side and is translation aware, so it can be (potentially) pushed down
                return condition;
            } else {
                // The condition cannot be used in the join on clause for now
                // It is not a binary comparison between left and right attributes
                // It is not using fields from the right side only and translation aware
                return new UnresolvedAttribute(
                    condition.source(),
                    "unsupported",
                    "Unsupported join filter expression:" + condition.sourceText()
                );
            }
        }

        private Join resolveLookupJoin(LookupJoin join, AnalyzerContext context) {
            JoinConfig config = join.config();
            // for now, support only (LEFT) USING clauses
            JoinType type = config.type();

            // rewrite the join into an equi-join between the field with the same name between left and right
            if (type == JoinTypes.LEFT) {
                // the lookup cannot be resolved, bail out
                if (Expressions.anyMatch(
                    join.references().stream().toList(),
                    c -> c instanceof UnresolvedAttribute ua && ua.customMessage()
                )) {
                    return join;
                }
                List<Attribute> leftKeys = new ArrayList<>();
                List<Attribute> rightKeys = new ArrayList<>();
                Expression joinOnConditions = null;
                if (join.config().joinOnConditions() != null) {
                    joinOnConditions = resolveJoinFiltersAndSwapIfNeeded(
                        join.config().joinOnConditions(),
                        join.left().outputSet(),
                        join.right().outputSet(),
                        leftKeys,
                        rightKeys,
                        context
                    );
                } else {
                    // resolve the using columns against the left and the right side then assemble the new join config
                    leftKeys = resolveUsingColumns(join.config().leftFields(), join.left().output(), "left");
                    rightKeys = resolveUsingColumns(join.config().rightFields(), join.right().output(), "right");
                }
                config = new JoinConfig(type, leftKeys, rightKeys, joinOnConditions);

                return new LookupJoin(join.source(), join.left(), join.right(), config, join.isRemote() || context.includesRemoteIndices());
            } else {
                // everything else is unsupported for now
                UnresolvedAttribute errorAttribute = new UnresolvedAttribute(join.source(), "unsupported", "Unsupported join type");
                // add error message
                return join.withConfig(new JoinConfig(type, singletonList(errorAttribute), emptyList(), null));
            }
        }

        private boolean isCompletelyRightSideAndTranslatable(Expression expression, AttributeSet rightOutputSet) {
            return rightOutputSet.containsAll(expression.references()) && isTranslatable(expression);
        }

        private boolean isTranslatable(Expression expression) {
            // Here we are trying to eliminate cases where the expression is definitely not translatable.
            // We do this early and without access to search stats for the lookup index that are only on the lookup node,
            // so we only eliminate some of the not translatable cases here
            // Later we will do a more thorough check on the lookup node
            return translatable(expression, LucenePushdownPredicates.DEFAULT) != TranslationAware.Translatable.NO;
        }

        private LogicalPlan resolveFork(Fork fork) {
            // we align the outputs of the sub plans such that they have the same columns
            boolean changed = false;
            List<LogicalPlan> newSubPlans = new ArrayList<>();
            List<Attribute> outputUnion = Fork.outputUnion(fork.children());
            List<String> forkColumns = outputUnion.stream().map(Attribute::name).toList();

            for (LogicalPlan logicalPlan : fork.children()) {
                Source source = logicalPlan.source();

                // find the missing columns
                List<Attribute> missing = new ArrayList<>();
                Set<String> currentNames = logicalPlan.outputSet().names();
                for (Attribute attr : outputUnion) {
                    if (currentNames.contains(attr.name()) == false) {
                        missing.add(attr);
                    }
                }

                List<Alias> aliases = missing.stream().map(attr -> {
                    // We cannot assign an alias with an UNSUPPORTED data type, so we use another type that is
                    // supported. This way we can add this missing column containing only null values to the fork branch output.
                    var attrType = attr.dataType() == UNSUPPORTED ? KEYWORD : attr.dataType();
                    if (attrType.isCounter()) {
                        attrType = attrType.noCounter();
                    }
                    // use the current fork branch's source as the source of the alias, instead of the original FieldAttribute's source.
                    return new Alias(source, attr.name(), new Literal(source, null, attrType));
                }).toList();

                // add the missing columns
                if (aliases.size() > 0) {
                    logicalPlan = new Eval(source, logicalPlan, aliases);
                    changed = true;
                }

                List<String> subPlanColumns = logicalPlan.output().stream().map(Attribute::name).toList();
                // We need to add an explicit projection to align the outputs.
                // If the branch already has a Project on top, and the output of the branch is empty,
                // don't add another Project with only NO_FIELDS on top of it,
                // otherwise it will cause an infinite loop in the analyzer, this happens to subquery so far.
                // forkColumns do not contain NO_FIELD because Fork.outputUnion removes it.
                if (logicalPlan instanceof Project == false
                    || (subPlanColumns.equals(forkColumns) == false
                        && subqueryReferencingIndexWithEmptyMapping(fork, logicalPlan, forkColumns) == false)) {
                    changed = true;
                    List<Attribute> newOutput = new ArrayList<>();
                    for (String attrName : forkColumns) {
                        for (Attribute subAttr : logicalPlan.output()) {
                            if (attrName.equals(subAttr.name())) {
                                newOutput.add(subAttr);
                            }
                        }
                    }
                    logicalPlan = resolveKeep(new Keep(logicalPlan.source(), logicalPlan, newOutput), UnmappedResolution.FAIL);
                }

                newSubPlans.add(logicalPlan);
            }

            if (changed == false) {
                return fork;
            }

            return fork.replaceSubPlansAndOutput(newSubPlans, toReferenceAttributes(outputUnion));
        }

        /*
         * Returns true if a subquery references an index with empty mapping.
         */
        private static boolean subqueryReferencingIndexWithEmptyMapping(
            LogicalPlan unionAll,
            LogicalPlan subquery,
            List<String> outputColumns
        ) {
            return unionAll instanceof UnionAll && outputColumns.isEmpty() && subquery.output().equals(NO_FIELDS);
        }

        private LogicalPlan resolveRerank(Rerank rerank, List<Attribute> childrenOutput, AnalyzerContext context) {
            List<Alias> newFields = new ArrayList<>();
            boolean changed = false;

            // Do not need to cast as string if there are multiple rerank fields since it will be converted to YAML.
            boolean castRerankFieldsAsString = rerank.rerankFields().size() < 2;

            // First resolving fields used in expression
            for (Alias field : rerank.rerankFields()) {
                Alias resolved = (Alias) field.transformUp(UnresolvedAttribute.class, ua -> resolveAttribute(ua, childrenOutput));

                if (resolved.resolved()) {
                    if (castRerankFieldsAsString
                        && rerank.isValidRerankField(resolved)
                        && DataType.isString(resolved.dataType()) == false) {
                        resolved = resolved.replaceChild(
                            new ToString(resolved.child().source(), resolved.child(), context.configuration())
                        );
                    }
                }

                newFields.add(resolved);
                changed |= resolved != field;
            }

            if (changed) {
                rerank = rerank.withRerankFields(newFields);
            }

            // Ensure the score attribute is present in the output.
            if (rerank.scoreAttribute() instanceof UnresolvedAttribute ua) {
                Attribute resolved = resolveAttribute(ua, childrenOutput);
                if (resolved.resolved() == false || resolved.dataType() != DOUBLE) {
                    if (ua.name().equals(MetadataAttribute.SCORE)) {
                        resolved = (Attribute) MetadataAttribute.create(Source.EMPTY, MetadataAttribute.SCORE);
                    } else {
                        resolved = new ReferenceAttribute(resolved.source(), null, resolved.name(), DOUBLE);
                    }
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

        private LogicalPlan resolveInsist(Insist insist, List<Attribute> childrenOutput, AnalyzerContext context) {
            List<Attribute> list = new ArrayList<>();
            List<IndexResolution> resolutions = collectIndexResolutions(insist, context);
            for (Attribute a : insist.insistedAttributes()) {
                list.add(resolveInsistAttribute(a, childrenOutput, resolutions));
            }
            return insist.withAttributes(list);
        }

        private static List<IndexResolution> collectIndexResolutions(LogicalPlan plan, AnalyzerContext context) {
            List<IndexResolution> resolutions = new ArrayList<>();
            plan.forEachDown(EsRelation.class, e -> {
                var resolution = context.indexResolution().get(new IndexPattern(e.source(), e.indexPattern()));
                if (resolution != null) {
                    resolutions.add(resolution);
                }
            });
            return resolutions;
        }

        private Attribute resolveInsistAttribute(Attribute attribute, List<Attribute> childrenOutput, List<IndexResolution> indices) {
            Attribute resolvedCol = maybeResolveAttribute((UnresolvedAttribute) attribute, childrenOutput);
            // Field isn't mapped anywhere.
            if (resolvedCol instanceof UnresolvedAttribute) {
                return insistKeyword(attribute);
            }

            // Field is partially unmapped.
            // TODO: Should the check for partially unmapped fields be done specific to each sub-query in a fork?
            if (resolvedCol instanceof FieldAttribute fa && indices.stream().anyMatch(r -> r.get().isPartiallyUnmappedField(fa.name()))) {
                return fa.dataType() == KEYWORD ? insistKeyword(fa) : invalidInsistAttribute(fa);
            }

            // Either the field is mapped everywhere and we can just use the resolved column, or the INSIST clause isn't on top of a FROM
            // clause—for example, it might be on top of a ROW clause—so the verifier will catch it and fail.
            return resolvedCol;
        }

        private static FieldAttribute invalidInsistAttribute(FieldAttribute fa) {
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
            return new FieldAttribute(fa.source(), null, fa.qualifier(), name, field);
        }

        public static FieldAttribute insistKeyword(Attribute attribute) {
            return new FieldAttribute(
                attribute.source(),
                null,
                attribute.qualifier(),
                attribute.name(),
                new PotentiallyUnmappedKeywordEsField(attribute.name())
            );
        }

        /**
         * This will inspect current node/{@code plan}'s expressions and check if any of the {@code FieldAttribute}s refer to fields that
         * are partially unmapped across the indices involved in the plan fragment. If so, replace their field with an "insisted" EsField.
         */
        private static LogicalPlan resolvePartiallyMapped(LogicalPlan plan, AnalyzerContext context) {
            var indexResolutions = collectIndexResolutions(plan, context);
            Map<FieldAttribute, FieldAttribute> insistedMap = new HashMap<>();
            var transformed = plan.transformExpressionsOnly(FieldAttribute.class, fa -> {
                var esField = fa.field();
                var isInsisted = esField instanceof PotentiallyUnmappedKeywordEsField || esField instanceof InvalidMappedField;
                if (isInsisted == false) {
                    var existing = insistedMap.get(fa);
                    if (existing != null) { // field shows up multiple times in the node; return first processing
                        return existing;
                    }
                    // Field is partially unmapped.
                    if (indexResolutions.stream().anyMatch(r -> r.get().isPartiallyUnmappedField(fa.name()))) {
                        FieldAttribute newFA = fa.dataType() == KEYWORD ? insistKeyword(fa) : invalidInsistAttribute(fa);
                        insistedMap.put(fa, newFA);
                        return newFA;
                    }
                }
                return fa;
            });
            return insistedMap.isEmpty() ? transformed : propagateInsistedFields(transformed, insistedMap);
        }

        /**
         * Push only those fields from the {@code insistedMap} into {@code EsRelation}s in the {@code plan} that wrap a
         * {@code PotentiallyUnmappedKeywordEsField}.
         */
        private static LogicalPlan propagateInsistedFields(LogicalPlan plan, Map<? extends Attribute, FieldAttribute> insistedMap) {
            return plan.transformUp(EsRelation.class, esr -> {
                var newOutput = new ArrayList<Attribute>();
                boolean updated = false;
                for (Attribute attr : esr.output()) {
                    var newFA = insistedMap.get(attr);
                    if (newFA != null && newFA.field() instanceof PotentiallyUnmappedKeywordEsField) {
                        newOutput.add(newFA);
                        updated = true;
                    } else {
                        newOutput.add(attr);
                    }
                }
                return updated ? esr.withAttributes(newOutput) : esr;
            });
        }

        private LogicalPlan resolveFuse(Fuse fuse, List<Attribute> childrenOutput) {
            Source source = fuse.source();
            Attribute score = fuse.score();
            if (score instanceof UnresolvedAttribute) {
                score = maybeResolveAttribute((UnresolvedAttribute) score, childrenOutput);
            }
            if (score instanceof UnresolvedAttribute ua && score.name().equals(MetadataAttribute.SCORE)) {
                score = ua.withUnresolvedMessage(
                    "FUSE requires a score column, default [" + MetadataAttribute.SCORE + "] column not found."
                );
            }

            Attribute discriminator = fuse.discriminator();
            if (discriminator instanceof UnresolvedAttribute) {
                discriminator = maybeResolveAttribute((UnresolvedAttribute) discriminator, childrenOutput);
            }
            if (discriminator instanceof UnresolvedAttribute ua && discriminator.name().equals(Fork.FORK_FIELD)) {
                discriminator = ua.withUnresolvedMessage(
                    "FUSE requires a column to group by, default [" + Fork.FORK_FIELD + "] column not found."
                );
            }

            List<NamedExpression> keys = fuse.keys().stream().map(attr -> {
                if (attr.resolved()) {
                    return attr;
                }
                attr = maybeResolveAttribute((UnresolvedAttribute) attr, childrenOutput);

                if (attr instanceof UnresolvedAttribute ua && ua.name().equals(IdFieldMapper.NAME)) {
                    return ua.withUnresolvedMessage("FUSE requires a key column, default [" + IdFieldMapper.NAME + "] column not found");
                }

                if (attr instanceof UnresolvedAttribute ua && ua.name().equals(MetadataAttribute.INDEX)) {
                    return ua.withUnresolvedMessage(
                        "FUSE requires a key column, default [" + MetadataAttribute.INDEX + "] column not found"
                    );
                }

                return attr;
            }).toList();

            // some attributes were unresolved or the wrong type
            // we return Fuse here so that the Verifier can raise an error message
            if (score instanceof UnresolvedAttribute
                || (score.resolved() && score.dataType() != DOUBLE)
                || discriminator instanceof UnresolvedAttribute
                || (discriminator.resolved() && DataType.isString(discriminator.dataType()) == false)
                || keys.stream().allMatch(attr -> attr.resolved() && DataType.isString(attr.dataType())) == false) {
                return new Fuse(fuse.source(), fuse.child(), score, discriminator, keys, fuse.fuseType(), fuse.options());
            }

            LogicalPlan scoreEval = new FuseScoreEval(source, fuse.child(), score, discriminator, fuse.fuseType(), fuse.options());

            // create aggregations
            Expression aggFilter = new Literal(source, true, DataType.BOOLEAN);

            List<NamedExpression> aggregates = new ArrayList<>();
            aggregates.add(
                new Alias(
                    source,
                    score.name(),
                    new Sum(source, score, aggFilter, AggregateFunction.NO_WINDOW, SummationMode.COMPENSATED_LITERAL)
                )
            );

            for (Attribute attr : childrenOutput) {
                if (attr.name().equals(score.name())) {
                    continue;
                }
                var valuesAgg = new Values(source, attr, aggFilter, AggregateFunction.NO_WINDOW);
                // Use VALUES only on supported fields.
                // FuseScoreEval will check that the input contains only columns with supported data types
                // and will fail with an appropriate error message if it doesn't.
                if (valuesAgg.resolved()) {
                    aggregates.add(new Alias(source, attr.name(), valuesAgg));
                }
            }

            return resolveAggregate(new Aggregate(source, scoreEval, new ArrayList<>(keys), aggregates), childrenOutput);
        }

        private LogicalPlan resolvePromql(PromqlCommand promql, List<Attribute> childrenOutput) {
            LogicalPlan promqlPlan = promql.promqlPlan();
            Function<UnresolvedAttribute, Expression> lambda = ua -> maybeResolveAttribute(ua, childrenOutput);
            // resolve the nested plan
            return promql.withPromqlPlan(promqlPlan.transformExpressionsDown(UnresolvedAttribute.class, lambda))
                // but also any unresolved expressions
                .transformExpressionsOnly(UnresolvedAttribute.class, lambda);
        }

        private Attribute maybeResolveAttribute(UnresolvedAttribute ua, List<Attribute> childrenOutput) {
            return maybeResolveAttribute(ua, childrenOutput, log);
        }

        private static Attribute maybeResolveAttribute(UnresolvedAttribute ua, List<Attribute> childrenOutput, Logger logger) {
            // if we already tried and failed to resolve this attribute, don't try again
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
            List<Attribute> named = resolveAgainstList(ua, childrenOutput);
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
        private static LogicalPlan resolveKeep(Keep keep, UnmappedResolution unmappedResolution) {
            return unmappedResolution == UnmappedResolution.FAIL
                ? new Project(keep.source(), keep.child(), keepResolver(keep.projections(), keep.child().output()))
                : new ResolvingProject(keep.source(), keep.child(), inputAttributes -> keepResolver(keep.projections(), inputAttributes));
        }

        private static List<NamedExpression> keepResolver(List<? extends NamedExpression> projections, List<Attribute> childOutput) {
            List<NamedExpression> resolvedProjections;
            // start with projections

            // no projection specified or just *
            if (projections.isEmpty() || (projections.size() == 1 && projections.getFirst() instanceof UnresolvedStar)) {
                resolvedProjections = new ArrayList<>(childOutput);
            }
            // otherwise resolve them
            else {
                Map<NamedExpression, Integer> priorities = new LinkedHashMap<>();
                for (var proj : projections) {
                    final List<Attribute> resolved;
                    final int priority;
                    if (proj instanceof UnresolvedStar) {
                        resolved = childOutput;
                        priority = 4;
                    } else if (proj instanceof UnresolvedNamePattern up) {
                        resolved = resolveAgainstList(up, childOutput);
                        priority = 3;
                    } else if (proj instanceof UnsupportedAttribute) {
                        resolved = List.of(proj.toAttribute());
                        priority = 2;
                    } else if (proj instanceof UnresolvedAttribute ua) {
                        resolved = resolveAgainstList(ua, childOutput);
                        priority = 1;
                    } else if (proj.resolved()) {
                        resolved = List.of(proj.toAttribute());
                        priority = 0;
                    } else {
                        throw new EsqlIllegalArgumentException("unexpected projection: " + proj);
                    }
                    for (var attr : resolved) {
                        Integer previousPrio = priorities.get(attr);
                        if (previousPrio == null || previousPrio >= priority) {
                            priorities.remove(attr);
                            priorities.put(attr, priority);
                        }
                    }
                }
                resolvedProjections = new ArrayList<>(priorities.keySet());
            }

            return resolvedProjections;
        }

        private static LogicalPlan resolveDrop(Drop drop, UnmappedResolution unmappedResolution) {
            return unmappedResolution == UnmappedResolution.FAIL
                ? new Project(drop.source(), drop.child(), dropResolver(drop.removals(), drop.output()))
                : new ResolvingProject(drop.source(), drop.child(), inputAttributes -> dropResolver(drop.removals(), inputAttributes));
        }

        private static List<NamedExpression> dropResolver(List<NamedExpression> removals, List<Attribute> childOutput) {
            List<NamedExpression> resolvedProjections = new ArrayList<>(childOutput);

            for (NamedExpression ne : removals) {
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

            return resolvedProjections;
        }

        private LogicalPlan resolveRename(Rename rename, UnmappedResolution unmappedResolution) {
            return unmappedResolution == UnmappedResolution.FAIL
                ? new Project(rename.source(), rename.child(), projectionsForRename(rename, rename.child().output(), log))
                : new ResolvingProject(
                    rename.source(),
                    rename.child(),
                    inputAttributes -> projectionsForRename(rename, inputAttributes, log)
                );
        }

        /**
         * This will compute the projections for a {@link Rename}.
         */
        public static List<NamedExpression> projectionsForRename(Rename rename, List<Attribute> inputAttributes, Logger logger) {
            List<Attribute> childrenOutput = new ArrayList<>(inputAttributes);
            List<NamedExpression> projections = new ArrayList<>(inputAttributes);

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
                            unresolved.add(alias.replaceChild(u)); // keep the alias around for potential later resolution
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
        UnresolvedAttribute ua = new UnresolvedPattern(up.source(), up.pattern());
        Predicate<Attribute> matcher = a -> up.match(a.name());
        var matches = AnalyzerRules.maybeResolveAgainstList(matcher, () -> ua, attrList, true, a -> Analyzer.handleSpecialFields(ua, a));
        return potentialCandidatesIfNoMatchesFound(ua, matches, attrList, list -> UnresolvedNamePattern.errorMessage(up.pattern(), list));
    }

    private static List<Attribute> resolveAgainstList(UnresolvedAttribute ua, Collection<Attribute> attrList) {
        var matches = AnalyzerRules.maybeResolveAgainstList(ua, attrList, a -> Analyzer.handleSpecialFields(ua, a));
        return potentialCandidatesIfNoMatchesFound(ua, matches, attrList, ua::defaultUnresolvedMessage);
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

    private static class ResolveConfigurationAware extends ParameterizedAnalyzerRule<LogicalPlan, AnalyzerContext> {

        @Override
        protected boolean skipResolved() {
            return false;
        }

        @Override
        protected LogicalPlan rule(LogicalPlan plan, AnalyzerContext context) {
            return plan.transformExpressionsUp(
                Expression.class,
                expression -> resolveConfigurationAware(expression, context.configuration())
            );
        }

        private static Expression resolveConfigurationAware(Expression expression, Configuration configuration) {
            if (expression instanceof ConfigurationAware ca && ca.configuration() == ConfigurationAware.CONFIGURATION_MARKER) {
                return ca.withConfiguration(configuration);
            }
            return expression;
        }
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

    private static class ResolveInference extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

        @Override
        public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
            return plan.transformExpressionsOnly(InferenceFunction.class, f -> resolveInferenceFunction(f, context))
                .transformDown(InferencePlan.class, p -> resolveInferencePlan(p, context));
        }

        private LogicalPlan resolveInferencePlan(InferencePlan<?> plan, AnalyzerContext context) {
            assert plan.inferenceId().resolved() && plan.inferenceId().foldable();

            String inferenceId = BytesRefs.toString(plan.inferenceId().fold(FoldContext.small()));
            ResolvedInference resolvedInference = context.inferenceResolution().getResolvedInference(inferenceId);

            if (resolvedInference == null) {
                String error = context.inferenceResolution().getError(inferenceId);
                return plan.withInferenceResolutionError(inferenceId, error);
            }

            if (resolvedInference.taskType() != plan.taskType()) {
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
            }

            if (plan.isFoldable()) {
                // Transform foldable InferencePlan to Eval with function call
                return transformToEval(plan, inferenceId);
            }

            return plan;
        }

        /**
         * Transforms a foldable InferencePlan to an Eval with the appropriate function call.
         */
        private LogicalPlan transformToEval(InferencePlan<?> plan, String inferenceId) {
            Expression inferenceIdLiteral = Literal.keyword(plan.inferenceId().source(), inferenceId);
            Source source = plan.source();
            LogicalPlan child = plan.child();

            if (plan instanceof Completion completion) {
                CompletionFunction completionFunction = new CompletionFunction(
                    source,
                    completion.prompt(),
                    inferenceIdLiteral,
                    completion.taskSettings()
                );
                Alias alias = new Alias(source, completion.targetField().name(), completionFunction, completion.targetField().id());
                return new Eval(source, child, List.of(alias));
            }

            return plan;
        }

        private InferenceFunction<?> resolveInferenceFunction(InferenceFunction<?> inferenceFunction, AnalyzerContext context) {
            if (inferenceFunction.inferenceId().resolved()
                && inferenceFunction.inferenceId().foldable()
                && DataType.isString(inferenceFunction.inferenceId().dataType())) {

                String inferenceId = BytesRefs.toString(inferenceFunction.inferenceId().fold(FoldContext.small()));
                ResolvedInference resolvedInference = context.inferenceResolution().getResolvedInference(inferenceId);

                if (resolvedInference == null) {
                    String error = context.inferenceResolution().getError(inferenceId);
                    return inferenceFunction.withInferenceResolutionError(inferenceId, error);
                }

                if (resolvedInference.taskType() != inferenceFunction.taskType()) {
                    String error = "cannot use inference endpoint ["
                        + inferenceId
                        + "] with task type ["
                        + resolvedInference.taskType()
                        + "] within a "
                        + context.functionRegistry().snapshotRegistry().functionName(inferenceFunction.getClass())
                        + " function. Only inference endpoints with the task type ["
                        + inferenceFunction.taskType()
                        + "] are supported.";
                    return inferenceFunction.withInferenceResolutionError(inferenceId, error);
                }
            }

            return inferenceFunction;
        }
    }

    private static class AddImplicitLimit extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {
        @Override
        public LogicalPlan apply(LogicalPlan logicalPlan, AnalyzerContext context) {
            List<LogicalPlan> limits = logicalPlan.collectFirstChildren(Limit.class::isInstance);
            // We check whether the query contains a TimeSeriesAggregate to determine if we should apply
            // the default limit for TS queries or for non-TS queries.
            // NOTE: PromqlCommand is translated to TimeSeriesAggregate during optimization.
            boolean isTsAggregate = logicalPlan.collectFirstChildren(lp -> lp instanceof TimeSeriesAggregate || lp instanceof PromqlCommand)
                .isEmpty() == false;

            int limit;
            if (limits.isEmpty()) {
                limit = context.configuration().resultTruncationDefaultSize(isTsAggregate); // user provided no limit: cap to a
                // default
                if (isTsAggregate == false) {
                    HeaderWarning.addWarning("No limit defined, adding default limit of [{}]", limit);
                }
            } else {
                limit = context.configuration().resultTruncationMaxSize(isTsAggregate); // user provided a limit: cap result
                                                                                        // entries to the max
            }
            var source = logicalPlan.source();
            return new Limit(source, new Literal(source, limit, DataType.INTEGER), logicalPlan);
        }
    }

    private static class AddImplicitForkLimit extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {
        private final AddImplicitLimit addImplicitLimit = new AddImplicitLimit();

        @Override
        public LogicalPlan apply(LogicalPlan logicalPlan, AnalyzerContext context) {
            if (context.configuration().pragmas().forkImplicitLimit()) {
                return logicalPlan.transformUp(Fork.class, fork -> addImplicitLimitToForkSubQueries(fork, context));
            }
            return logicalPlan;
        }

        private LogicalPlan addImplicitLimitToForkSubQueries(Fork fork, AnalyzerContext ctx) {
            // do not append an implicit limit to subqueries below a UnionAll
            if (fork instanceof UnionAll) {
                return fork;
            }
            List<LogicalPlan> newSubPlans = new ArrayList<>();
            for (var subPlan : fork.children()) {
                newSubPlans.add(addImplicitLimit.apply(subPlan, ctx));
            }
            return fork.replaceSubPlans(newSubPlans);
        }
    }

    /**
     * For TS queries without explicit SORT or STATS, inject an implicit SORT by @timestamp DESC
     * so that the most recent points are returned first, instead of physical index order.
     */
    private static class AddImplicitTimestampSort extends Rule<LogicalPlan, LogicalPlan> {
        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            if (plan instanceof Limit limit) {
                return injectTimestampSort(limit);
            }
            throw new IllegalStateException(
                "Rule " + AddImplicitTimestampSort.class.getName() + " should run after " + AddImplicitLimit.class.getName()
            );
        }

        private LogicalPlan injectTimestampSort(Limit limit) {
            LogicalPlan child = limit.child();

            boolean hasExplicitSortOrAggregate = child.collectFirstChildren(lp -> lp instanceof OrderBy || lp instanceof Aggregate)
                .isEmpty() == false;

            if (hasExplicitSortOrAggregate) {
                return limit;
            }

            boolean hasTimeSeries = child.collect(EsRelation.class, r -> r.indexMode() == IndexMode.TIME_SERIES).isEmpty() == false;
            if (hasTimeSeries == false) {
                return limit;
            }

            // Inject the OrderBy below each (to handle FORK) innermost Limit.
            return limit.transformDown(Limit.class, l -> {
                if (l.child().collect(Limit.class).isEmpty()) {
                    var localChild = l.child();
                    var localTimestampAttr = localChild.collect(EsRelation.class, r -> r.indexMode() == IndexMode.TIME_SERIES)
                        .stream()
                        .findFirst()
                        .flatMap(r -> r.output().stream().filter(a -> MetadataAttribute.TIMESTAMP_FIELD.equals(a.name())).findFirst())
                        .flatMap(ts -> localChild.output().stream().filter(a -> a.id().equals(ts.id())).findFirst());

                    if (localTimestampAttr.isPresent()) {
                        var source = l.source();
                        Order order = new Order(source, localTimestampAttr.get(), Order.OrderDirection.DESC, Order.NullsPosition.LAST);
                        return l.replaceChild(new OrderBy(source, localChild, List.of(order)));
                    }
                }
                return l;
            });
        }
    }

    private BitSet gatherPreAnalysisMetrics(LogicalPlan plan, BitSet b) {
        // count only the explicit "limit" the user added, otherwise all queries will have a "limit" and telemetry won't reflect reality
        if (plan.collectFirstChildren(Limit.class::isInstance).isEmpty() == false) {
            b.set(LIMIT.ordinal());
        }

        // count only the Aggregate (STATS command) that is "standalone" not also the one that is part of an INLINE STATS command
        if (plan instanceof Aggregate) {
            b.set(STATS.ordinal());
        } else {
            plan.forEachDownMayReturnEarly((p, breakEarly) -> {
                if (p instanceof InlineStats) {
                    return;
                }
                for (var c : p.children()) {
                    if (c instanceof Aggregate) {
                        b.set(STATS.ordinal());
                        breakEarly.set(true);
                        return;
                    }
                }
            });
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
     * applicable, the same applies to Case, Greatest, Least. For example, implicit casting converts:
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
            // do implicit casting for named parameters
            return plan.transformExpressionsUp(
                org.elasticsearch.xpack.esql.core.expression.function.Function.class,
                e -> ImplicitCasting.cast(e, context.functionRegistry().snapshotRegistry(), context.configuration())
            );
        }

        private static Expression cast(
            org.elasticsearch.xpack.esql.core.expression.function.Function f,
            EsqlFunctionRegistry registry,
            Configuration configuration
        ) {
            if (f instanceof In in) {
                return processIn(in, configuration);
            }
            if (f instanceof VectorFunction) {
                return processVectorFunction(f, registry, configuration);
            }
            if (f instanceof EsqlScalarFunction || f instanceof GroupingFunction) { // exclude AggregateFunction until it is needed
                return processScalarOrGroupingFunction(f, registry, configuration);
            }
            if (f instanceof EsqlArithmeticOperation || f instanceof BinaryComparison) {
                return processBinaryOperator((BinaryOperator) f, configuration);
            }
            return f;
        }

        private static Expression processScalarOrGroupingFunction(
            org.elasticsearch.xpack.esql.core.expression.function.Function f,
            EsqlFunctionRegistry registry,
            Configuration configuration
        ) {
            List<Expression> args = f.arguments();
            List<DataType> targetDataTypes = registry.getDataTypeForStringLiteralConversion(f.getClass());
            if (targetDataTypes == null || targetDataTypes.isEmpty()) {
                return f;
            }
            List<Expression> newChildren = new ArrayList<>(args.size());
            boolean childrenChanged = false;
            DataType targetDataType = NULL;
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
                            } // else the last type applies to all elements in a possible list (variadic)
                            if (targetDataType != NULL && targetDataType != UNSUPPORTED) {
                                Expression e = castStringLiteral(arg, targetDataType, configuration);
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

        private static Expression processBinaryOperator(BinaryOperator<?, ?, ?, ?> o, Configuration configuration) {
            Expression left = o.left();
            Expression right = o.right();
            if (left.resolved() == false || right.resolved() == false) {
                return o;
            }
            List<Expression> newChildren = new ArrayList<>(2);
            boolean childrenChanged = false;
            DataType targetDataType = NULL;
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
                Expression e = castStringLiteral(from, targetDataType, configuration);
                newChildren.add(from == left ? e : left);
                newChildren.add(from == right ? e : right);
                childrenChanged = true;
            }
            return childrenChanged ? o.replaceChildren(newChildren) : o;
        }

        private static Expression processIn(In in, Configuration configuration) {
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
                    Expression e = castStringLiteral(value, targetDataType, configuration);
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
            String name = BytesRefs.toString(value.fold(FoldContext.small()) /* TODO remove me */);
            String message = LoggerMessageFormat.format(
                null,
                "Cannot convert string [{}] to [{}], error [{}]",
                name,
                type,
                (e instanceof ParsingException pe) ? pe.getErrorMessage() : e.getMessage()
            );
            return new UnresolvedAttribute(value.source(), name, message);
        }

        private static Expression castStringLiteralToTemporalAmount(Expression from) {
            try {
                TemporalAmount result = maybeParseTemporalAmount(
                    BytesRefs.toString(from.fold(FoldContext.small() /* TODO remove me */)).strip()
                );
                if (result == null) {
                    return from;
                }
                DataType target = result instanceof Duration ? TIME_DURATION : DATE_PERIOD;
                return new Literal(from.source(), result, target);
            } catch (Exception e) {
                return unresolvedAttribute(from, DATE_PERIOD + " or " + TIME_DURATION, e);
            }
        }

        private static Expression castStringLiteral(Expression from, DataType target, Configuration configuration) {
            assert from.foldable();
            try {
                return isTemporalAmount(target)
                    ? castStringLiteralToTemporalAmount(from)
                    : new Literal(
                        from.source(),
                        EsqlDataTypeConverter.convert(from.fold(FoldContext.small() /* TODO remove me */), target, configuration),
                        target
                    );
            } catch (Exception e) {
                return unresolvedAttribute(from, target.toString(), e);
            }
        }

        @SuppressWarnings("unchecked")
        private static Expression processVectorFunction(
            org.elasticsearch.xpack.esql.core.expression.function.Function vectorFunction,
            EsqlFunctionRegistry registry,
            Configuration configuration
        ) {
            // Perform implicit casting for dense_vector from numeric and keyword values
            List<Expression> args = vectorFunction.arguments();
            List<DataType> targetDataTypes = registry.getDataTypeForStringLiteralConversion(vectorFunction.getClass());
            List<Expression> newArgs = new ArrayList<>();
            for (int i = 0; i < args.size(); i++) {
                Expression arg = args.get(i);
                if (targetDataTypes.get(i) == DENSE_VECTOR && arg.resolved()) {
                    var dataType = arg.dataType();
                    if (dataType == KEYWORD) {
                        if (arg.foldable()) {
                            Expression exp = castStringLiteral(arg, DENSE_VECTOR, configuration);
                            if (exp != arg) {
                                newArgs.add(exp);
                                continue;
                            }
                        }
                    } else if (dataType.isNumeric()) {
                        newArgs.add(new ToDenseVector(vectorFunction.source(), arg));
                        continue;
                    }
                }
                newArgs.add(arg);
            }

            return vectorFunction.replaceChildren(newArgs);
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

        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            List<Attribute.IdIgnoringWrapper> unionFieldAttributes = new ArrayList<>();
            return plan.transformUp(LogicalPlan.class, p -> p.childrenResolved() == false ? p : doRule(p, unionFieldAttributes));
        }

        private LogicalPlan doRule(LogicalPlan plan, List<Attribute.IdIgnoringWrapper> unionFieldAttributes) {
            Holder<Integer> alreadyAddedUnionFieldAttributes = new Holder<>(unionFieldAttributes.size());
            // Collect field attributes from previous runs
            if (plan instanceof EsRelation rel) {
                unionFieldAttributes.clear();
                for (Attribute attr : rel.output()) {
                    if (attr instanceof FieldAttribute fa && fa.field() instanceof MultiTypeEsField && fa.synthetic()) {
                        unionFieldAttributes.add(fa.ignoreId());
                    }
                }
            }

            // See if the eval function has an unresolved MultiTypeEsField field
            // Replace the entire convert function with a new FieldAttribute (containing type conversion knowledge)
            plan = plan.transformExpressionsOnly(e -> {
                if (e instanceof ConvertFunction convert) {
                    return resolveConvertFunction(convert, unionFieldAttributes);
                }
                return e;
            });

            // If no union fields were generated, return the plan as is
            if (unionFieldAttributes.size() == alreadyAddedUnionFieldAttributes.get()) {
                return plan;
            }

            return addGeneratedFieldsToEsRelations(plan, unionFieldAttributes.stream().map(attr -> (FieldAttribute) attr.inner()).toList());
        }

        /**
         * Add generated fields to EsRelation, so these new attributes will appear in the OutputExec of the Fragment
         * and thereby get used in FieldExtractExec
         */
        private static LogicalPlan addGeneratedFieldsToEsRelations(LogicalPlan plan, List<FieldAttribute> unionFieldAttributes) {
            var res = plan.transformDown(EsRelation.class, esr -> {
                List<Attribute> missing = new ArrayList<>();
                for (FieldAttribute fa : unionFieldAttributes) {
                    // Using outputSet().contains looks by NameId, resp. uses semanticEquals.
                    if (esr.outputSet().contains(fa) == false) {
                        missing.add(fa);
                    }
                }

                if (missing.isEmpty() == false) {
                    return esr.withAttributes(CollectionUtils.combine(esr.output(), missing));
                }
                return esr;
            });
            if (res.equals(plan) == false) {
                res = res.transformUp(Project.class, p -> {
                    List<Attribute> syntheticAttributesToCarryOver = new ArrayList<>();
                    for (Attribute attr : p.inputSet()) {
                        if (attr.synthetic() && p.outputSet().contains(attr) == false) {
                            syntheticAttributesToCarryOver.add(attr);
                        }
                    }

                    if (syntheticAttributesToCarryOver.isEmpty()) {
                        return p;
                    }

                    List<NamedExpression> newProjections = new ArrayList<>(p.projections());
                    newProjections.addAll(syntheticAttributesToCarryOver);
                    return new Project(p.source(), p.child(), newProjections);
                });
            }
            return res;
        }

        private Expression resolveConvertFunction(ConvertFunction convert, List<Attribute.IdIgnoringWrapper> unionFieldAttributes) {
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
                        typeResolutions(fa, convert, type, imf, typeResolutions);
                    }
                });
                // If all mapped types were resolved, create a new FieldAttribute with the resolved MultiTypeEsField
                if (typeResolutions.size() == imf.getTypesToIndices().size()) {
                    var resolvedField = resolvedMultiTypeEsField(fa, typeResolutions);
                    return createIfDoesNotAlreadyExist(fa, resolvedField, unionFieldAttributes);
                }
            } else if (convert.field() instanceof FieldAttribute fa
                && fa.synthetic() == false // MultiTypeEsField in EsRelation created by DateMillisToNanosInEsRelation has synthetic = false
                && fa.field() instanceof MultiTypeEsField mtf) {
                    // This is an explicit casting of a union typed field that has been converted to MultiTypeEsField in EsRelation by
                    // DateMillisToNanosInEsRelation, it is not necessary to cast it again to the same type, replace the implicit casting
                    // with explicit casting. However, it is useful to differentiate implicit and explicit casting in some cases, for
                    // example, an expression like multiTypeEsField(synthetic=false, date_nanos)::date_nanos::datetime is rewritten to
                    // multiTypeEsField(synthetic=true, date_nanos)::datetime, the implicit casting is overwritten by explicit casting and
                    // the multiTypeEsField is not casted to datetime directly.
                    if (((Expression) convert).dataType() == mtf.getDataType()) {
                        return createIfDoesNotAlreadyExist(fa, mtf, unionFieldAttributes);
                    }

                    // Data type is different between implicit(date_nanos) and explicit casting, if the conversion is supported, create a
                    // new MultiTypeEsField with explicit casting type, and add it to unionFieldAttributes.
                    Set<DataType> supportedTypes = convert.supportedTypes();
                    if (supportedTypes.contains(fa.dataType()) && canConvertOriginalTypes(mtf, supportedTypes)) {
                        // Build the mapping between index name and conversion expressions
                        Map<String, Expression> indexToConversionExpressions = new HashMap<>();
                        for (Map.Entry<String, Expression> entry : mtf.getIndexToConversionExpressions().entrySet()) {
                            String indexName = entry.getKey();
                            AbstractConvertFunction originalConversionFunction = (AbstractConvertFunction) entry.getValue();
                            Expression originalField = originalConversionFunction.field();
                            Expression newConvertFunction = convertExpression.replaceChildren(Collections.singletonList(originalField));
                            indexToConversionExpressions.put(indexName, newConvertFunction);
                        }
                        MultiTypeEsField multiTypeEsField = new MultiTypeEsField(
                            fa.fieldName().string(),
                            convertExpression.dataType(),
                            false,
                            indexToConversionExpressions,
                            fa.field().getTimeSeriesFieldType()
                        );
                        return createIfDoesNotAlreadyExist(fa, multiTypeEsField, unionFieldAttributes);
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
            List<Attribute.IdIgnoringWrapper> unionFieldAttributes
        ) {
            // Generate new ID for the field and suffix it with the data type to maintain unique attribute names.
            // NOTE: The name has to start with $$ to not break bwc with 8.15 - in that version, this is how we had to mark this as
            // synthetic to work around a bug.
            String unionTypedFieldName = Attribute.rawTemporaryName(fa.name(), "converted_to", resolvedField.getDataType().typeName());
            FieldAttribute unionFieldAttribute = new FieldAttribute(
                fa.source(),
                fa.parentName(),
                fa.qualifier(),
                unionTypedFieldName,
                resolvedField,
                true
            );
            var nonSemanticUnionFieldAttribute = unionFieldAttribute.ignoreId();

            int existingIndex = unionFieldAttributes.indexOf(nonSemanticUnionFieldAttribute);
            if (existingIndex >= 0) {
                // Do not generate multiple name/type combinations with different IDs
                return unionFieldAttributes.get(existingIndex).inner();
            } else {
                unionFieldAttributes.add(nonSemanticUnionFieldAttribute);
                return nonSemanticUnionFieldAttribute.inner();
            }
        }

        private static MultiTypeEsField resolvedMultiTypeEsField(
            FieldAttribute fa,
            HashMap<TypeResolutionKey, Expression> typeResolutions
        ) {
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

        private static boolean canConvertOriginalTypes(MultiTypeEsField multiTypeEsField, Set<DataType> supportedTypes) {
            return multiTypeEsField.getIndexToConversionExpressions()
                .values()
                .stream()
                .allMatch(
                    e -> e instanceof AbstractConvertFunction convertFunction
                        && supportedTypes.contains(convertFunction.field().dataType().widenSmallNumeric())
                );
        }

        private static Expression typeSpecificConvert(ConvertFunction convert, Source source, DataType type, InvalidMappedField mtf) {
            EsField field = new EsField(mtf.getName(), type, mtf.getProperties(), mtf.isAggregatable(), mtf.getTimeSeriesFieldType());
            FieldAttribute originalFieldAttr = (FieldAttribute) convert.field();
            FieldAttribute resolvedAttr = new FieldAttribute(
                source,
                originalFieldAttr.parentName(),
                originalFieldAttr.qualifier(),
                originalFieldAttr.name(),
                field,
                originalFieldAttr.nullable(),
                originalFieldAttr.id(),
                true
            );
            Expression fn = (Expression) convert;
            List<Expression> children = new ArrayList<>(fn.children());
            children.set(0, resolvedAttr);
            Expression e = ((Expression) convert).replaceChildren(children);
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

            // We start by dropping synthetic attributes if the plan is resolved
            LogicalPlan cleanPlan = plan.resolved() ? planWithoutSyntheticAttributes(plan) : plan;

            // If not, we apply checkUnresolved to the field attributes of the original plan, resulting in unsupported attributes
            // This removes attributes such as converted types if they are aliased, but retains them otherwise, while also guaranteeing that
            // unsupported / unresolved fields can be explicitly retained
            return cleanPlan.transformUp(
                LogicalPlan.class,
                p -> p.transformExpressionsOnly(FieldAttribute.class, UnionTypesCleanup::checkUnresolved)
            );
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
                if (attr.synthetic() && attr != NO_FIELDS.getFirst()) {
                    continue;
                }
                newOutput.add(attr);
            }

            return newOutput.size() == output.size() ? plan : new Project(Source.EMPTY, plan, newOutput);
        }
    }

    /**
     * Cast the union typed fields in EsRelation to date_nanos if they are mixed date and date_nanos types.
     */
    private static class DateMillisToNanosInEsRelation extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

        @Override
        public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
            return plan.transformUp(EsRelation.class, relation -> {
                if (relation.indexMode() == IndexMode.LOOKUP) {
                    return relation;
                }
                return relation.transformExpressionsUp(FieldAttribute.class, f -> {
                    if (f.field() instanceof InvalidMappedField imf && imf.types().stream().allMatch(DataType::isDate)) {
                        HashMap<ResolveUnionTypes.TypeResolutionKey, Expression> typeResolutions = new HashMap<>();
                        var convert = new ToDateNanos(f.source(), f, context.configuration());
                        imf.types().forEach(type -> typeResolutions(f, convert, type, imf, typeResolutions));
                        var resolvedField = ResolveUnionTypes.resolvedMultiTypeEsField(f, typeResolutions);
                        return new FieldAttribute(
                            f.source(),
                            f.parentName(),
                            f.qualifier(),
                            f.name(),
                            resolvedField,
                            f.nullable(),
                            f.id(),
                            f.synthetic()
                        );
                    }
                    return f;
                });
            });
        }
    }

    private static void typeResolutions(
        FieldAttribute fieldAttribute,
        ConvertFunction convert,
        DataType type,
        InvalidMappedField imf,
        HashMap<ResolveUnionTypes.TypeResolutionKey, Expression> typeResolutions
    ) {
        ResolveUnionTypes.TypeResolutionKey key = new ResolveUnionTypes.TypeResolutionKey(fieldAttribute.name(), type);
        var concreteConvert = ResolveUnionTypes.typeSpecificConvert(convert, fieldAttribute.source(), type, imf);
        typeResolutions.put(key, concreteConvert);
    }

    /**
     * Take InvalidMappedFields in specific aggregations (min, max, sum, count, and avg) and if all original data types
     * are aggregate metric double + any combination of numerics, implicitly cast them to the same type: aggregate metric
     * double for count, and double for min, max, and sum. Avg gets replaced with its surrogate (Div(Sum, Count))
     */
    private static class ImplicitCastAggregateMetricDoubles extends Rule<LogicalPlan, LogicalPlan> {

        private boolean isTimeSeries = false;

        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            Holder<IndexMode> indexMode = new Holder<>(IndexMode.STANDARD);
            plan.forEachUp(EsRelation.class, esRelation -> { indexMode.set(esRelation.indexMode()); });
            isTimeSeries = indexMode.get() == IndexMode.TIME_SERIES;
            return plan.transformUp(Aggregate.class, p -> p.childrenResolved() == false ? p : doRule(p));
        }

        private LogicalPlan doRule(Aggregate plan) {
            Map<String, FieldAttribute> unionFields = new HashMap<>();
            Holder<Boolean> aborted = new Holder<>(Boolean.FALSE);
            var newPlan = plan.transformExpressionsOnly(AggregateFunction.class, aggFunc -> {
                Expression child;
                if (aggFunc.field() instanceof ToAggregateMetricDouble toAMD) {
                    child = tryToTransformFunction(aggFunc, toAMD.field(), aborted, unionFields);
                } else {
                    child = tryToTransformFunction(aggFunc, aggFunc.field(), aborted, unionFields);
                }
                return child;
            });
            if (unionFields.isEmpty() || aborted.get()) {
                return plan;
            }
            return ResolveUnionTypes.addGeneratedFieldsToEsRelations(newPlan, unionFields.values().stream().toList());
        }

        private Expression tryToTransformFunction(
            AggregateFunction aggFunc,
            Expression field,
            Holder<Boolean> aborted,
            Map<String, FieldAttribute> unionFields
        ) {
            if (field instanceof FieldAttribute fa && fa.field() instanceof InvalidMappedField imf) {
                if (imf.types().contains(AGGREGATE_METRIC_DOUBLE) == false
                    || imf.types().stream().allMatch(f -> f == AGGREGATE_METRIC_DOUBLE || f.isNumeric()) == false) {
                    aborted.set(Boolean.TRUE);
                    return aggFunc;
                }

                // break down Avg and AvgOverTime so we grab the correct submetrics
                if (aggFunc instanceof Avg avg) {
                    return new Div(
                        aggFunc.source(),
                        new Sum(aggFunc.source(), field, aggFunc.filter(), aggFunc.window(), avg.summationMode()),
                        new Count(aggFunc.source(), field, aggFunc.filter(), aggFunc.window())
                    );
                }
                if (aggFunc instanceof AvgOverTime) {
                    return new Div(
                        aggFunc.source(),
                        new SumOverTime(aggFunc.source(), field, aggFunc.filter(), aggFunc.window()),
                        new CountOverTime(aggFunc.source(), field, aggFunc.filter(), aggFunc.window())
                    );
                }

                Map<String, Expression> typeConverters = typeConverters(aggFunc, fa, imf);
                var newField = unionFields.computeIfAbsent(
                    Attribute.rawTemporaryName(fa.name(), aggFunc.functionName(), aggFunc.sourceText()),
                    newName -> new FieldAttribute(
                        fa.source(),
                        fa.parentName(),
                        fa.qualifier(),
                        newName,
                        MultiTypeEsField.resolveFrom(imf, typeConverters),
                        fa.nullable(),
                        null,
                        true
                    )
                );
                List<Expression> children = new ArrayList<>(aggFunc.children());
                children.set(0, newField);
                // break down Count so we compute the sum of the count submetrics, rather than the number of documents present
                if (aggFunc instanceof Count) {
                    return new Sum(aggFunc.source(), children.getFirst());
                }
                if (aggFunc instanceof CountOverTime) {
                    return new SumOverTime(aggFunc.source(), children.getFirst(), aggFunc.filter(), aggFunc.window());
                }
                return aggFunc.replaceChildren(children);
            }
            return aggFunc;
        }

        private Map<String, Expression> typeConverters(AggregateFunction aggFunc, FieldAttribute fa, InvalidMappedField mtf) {
            var metric = getMetric(aggFunc, isTimeSeries);
            Map<String, Expression> typeConverter = new HashMap<>();
            for (DataType type : mtf.types()) {
                final ConvertFunction convert;
                if (type == AGGREGATE_METRIC_DOUBLE) {
                    convert = FromAggregateMetricDouble.withMetric(aggFunc.source(), fa, metric);
                } else if (metric == AggregateMetricDoubleBlockBuilder.Metric.COUNT) {
                    // we have a numeric on hand so calculate MvCount on it so we can plug it into Sum(metric.count)
                    var tempConvert = new MvCount(aggFunc.source(), fa);
                    typeConverter.put(type.typeName(), countConvert(tempConvert, fa.source(), type, mtf));
                    continue;
                } else {
                    convert = new ToDouble(fa.source(), fa);
                }
                Expression expression = ResolveUnionTypes.typeSpecificConvert(convert, fa.source(), type, mtf);
                typeConverter.put(type.typeName(), expression);
            }
            return typeConverter;
        }

        private Expression countConvert(UnaryScalarFunction convert, Source source, DataType type, InvalidMappedField imf) {
            EsField field = new EsField(imf.getName(), type, imf.getProperties(), imf.isAggregatable(), imf.getTimeSeriesFieldType());
            FieldAttribute originalFieldAttr = (FieldAttribute) convert.field();
            FieldAttribute resolvedAttr = new FieldAttribute(
                source,
                originalFieldAttr.parentName(),
                originalFieldAttr.qualifier(),
                originalFieldAttr.name(),
                field,
                originalFieldAttr.nullable(),
                originalFieldAttr.id(),
                true
            );
            List<Expression> children = new ArrayList<>(convert.children());
            children.set(0, resolvedAttr);
            return convert.replaceChildren(children);
        }

        private static boolean hasNativeSupport(AggregateFunction aggFunc, boolean isTimeSeries) {
            return aggFunc instanceof AggregateMetricDoubleNativeSupport
                && (isTimeSeries == false || aggFunc instanceof TimeSeriesAggregateFunction);
        }

        private static AggregateMetricDoubleBlockBuilder.Metric getMetric(AggregateFunction aggFunc, boolean isTimeSeries) {
            if (hasNativeSupport(aggFunc, isTimeSeries) == false) {
                return AggregateMetricDoubleBlockBuilder.Metric.DEFAULT;
            }
            if (aggFunc instanceof Max || aggFunc instanceof MaxOverTime) {
                return AggregateMetricDoubleBlockBuilder.Metric.MAX;
            }
            if (aggFunc instanceof Min || aggFunc instanceof MinOverTime) {
                return AggregateMetricDoubleBlockBuilder.Metric.MIN;
            }
            if (aggFunc instanceof Sum || aggFunc instanceof SumOverTime) {
                return AggregateMetricDoubleBlockBuilder.Metric.SUM;
            }
            if (aggFunc instanceof Count || aggFunc instanceof CountOverTime) {
                return AggregateMetricDoubleBlockBuilder.Metric.COUNT;
            }
            if (aggFunc instanceof Present || aggFunc instanceof PresentOverTime) {
                return AggregateMetricDoubleBlockBuilder.Metric.COUNT;
            }
            if (aggFunc instanceof Absent || aggFunc instanceof AbsentOverTime) {
                return AggregateMetricDoubleBlockBuilder.Metric.COUNT;
            }
            return AggregateMetricDoubleBlockBuilder.Metric.DEFAULT;
        }
    }

    /**
     * Takes aggregation functions that don't natively support AggregateMetricDouble (i.e. aggregations other than
     * min, max, sum, count, avg) that receive an AggregateMetricDouble as input, and inserts a call to
     * FROM_AGGREGATE_METRIC_DOUBLE to fetch the DEFAULT metric.
     */
    private static class InsertFromAggregateMetricDouble extends Rule<LogicalPlan, LogicalPlan> {
        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            return plan.transformUp(Aggregate.class, p -> p.childrenResolved() == false ? p : doRule(p));
        }

        private LogicalPlan doRule(Aggregate plan) {
            Holder<IndexMode> indexMode = new Holder<>(IndexMode.STANDARD);
            plan.forEachUp(EsRelation.class, esRelation -> { indexMode.set(esRelation.indexMode()); });
            final boolean isTimeSeries = indexMode.get() == IndexMode.TIME_SERIES;
            return plan.transformExpressionsOnly(AggregateFunction.class, aggFunc -> {
                if (ImplicitCastAggregateMetricDoubles.hasNativeSupport(aggFunc, isTimeSeries)) {
                    return aggFunc;
                }
                if (aggFunc.field() instanceof FieldAttribute fa && fa.field().getDataType() == AGGREGATE_METRIC_DOUBLE) {
                    Expression newField = FromAggregateMetricDouble.withMetric(
                        fa.source(),
                        fa,
                        AggregateMetricDoubleBlockBuilder.Metric.DEFAULT
                    );
                    List<Expression> children = new ArrayList<>(aggFunc.children());
                    children.set(0, newField);
                    return aggFunc.replaceChildren(children);
                }
                return aggFunc;
            });
        }
    }

    /**
     * Handle union types in UnionAll:
     * <ol>
     * <li>Push down explicit conversion functions into the UnionAll branches</li>
     * <li>Replace the explicit conversion functions with the corresponding attributes in the UnionAll output</li>
     * <li>Implicitly cast the outputs of the UnionAll branches to the common type, this applies to date and date_nanos types only</li>
     * <li>Update the attributes referencing the updated UnionAll output</li>
     * </ol>
     */
    private static class ResolveUnionTypesInUnionAll extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

        @Override
        public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
            // The mapping between explicit conversion functions and the corresponding attributes in the UnionAll output,
            // if the conversion functions in the main query are pushed down into the UnionAll branches, a new ReferenceAttribute
            // is created for the corresponding output of UnionAll, the value is the new ReferenceAttribute
            Map<AbstractConvertFunction, Attribute> convertFunctionsToAttributes = new HashMap<>();

            // The list of attributes in the UnionAll output that have been updated.
            // The parent plans that reference these attributes need to be updated accordingly.
            List<Attribute> updatedUnionAllOutput = new ArrayList<>();

            // First push down the conversion functions into the UnionAll branches
            LogicalPlan planWithConvertFunctionsPushedDown = plan.transformUp(
                UnionAll.class,
                unionAll -> unionAll.childrenResolved()
                    ? maybePushDownConvertFunctions(unionAll, plan, convertFunctionsToAttributes)
                    : unionAll
            );

            // Then replace the conversion functions with the corresponding attributes in the UnionAll output
            LogicalPlan planWithConvertFunctionsReplaced = replaceConvertFunctions(
                planWithConvertFunctionsPushedDown,
                convertFunctionsToAttributes
            );

            // Next implicitly cast the outputs of the UnionAll branches to the common type, this applies to date and date_nanos types only
            LogicalPlan planWithImplicitCasting = planWithConvertFunctionsReplaced.transformUp(
                UnionAll.class,
                unionAll -> unionAll.resolved()
                    ? implicitCastingUnionAllOutput(
                        unionAll,
                        planWithConvertFunctionsReplaced,
                        updatedUnionAllOutput,
                        context.configuration()
                    )
                    : unionAll
            );

            // Finally update the attributes referencing the updated UnionAll output
            return updatedUnionAllOutput.isEmpty()
                ? planWithImplicitCasting
                : updateAttributesReferencingUpdatedUnionAllOutput(planWithImplicitCasting, updatedUnionAllOutput);
        }

        /**
         * Push down the explicit conversion functions into the UnionAll branches
         */
        private static LogicalPlan maybePushDownConvertFunctions(
            UnionAll unionAll,
            LogicalPlan plan,
            Map<AbstractConvertFunction, Attribute> convertFunctionsToAttributes
        ) {
            // Collect all conversion functions that convert the UnionAll outputs to a different type
            Map<String, Set<AbstractConvertFunction>> oldOutputToConvertFunctions = collectConvertFunctions(unionAll, plan);

            if (oldOutputToConvertFunctions.isEmpty()) { // nothing to push down
                return unionAll;
            }

            // push down the conversion functions into the unionAll branches
            List<LogicalPlan> newChildren = new ArrayList<>(unionAll.children().size());
            Map<String, AbstractConvertFunction> newOutputToConvertFunctions = new HashMap<>();
            boolean outputChanged = false;
            for (LogicalPlan child : unionAll.children()) {
                List<Attribute> childOutput = child.output();
                List<Alias> newAliases = new ArrayList<>();
                List<Attribute> newChildOutput = new ArrayList<>(childOutput.size());
                for (Attribute oldAttr : childOutput) {
                    newChildOutput.add(oldAttr);
                    Set<AbstractConvertFunction> converts = oldOutputToConvertFunctions.get(oldAttr.name());
                    if (converts != null) {
                        // create a new alias for each conversion function and add it to the new aliases list
                        for (AbstractConvertFunction convert : converts) {
                            // create a new alias for the conversion function
                            String newAliasName = Attribute.rawTemporaryName(oldAttr.name(), "converted_to", convert.dataType().typeName());
                            Alias newAlias = new Alias(
                                oldAttr.source(),
                                newAliasName, // oldAttrName$$converted_to$$targetType
                                convert.replaceChildren(Collections.singletonList(oldAttr)),
                                null, // generate a new id
                                true // this'll be used to Project the synthetic attributes out when finishing analysis
                            );
                            newAliases.add(newAlias);
                            newChildOutput.add(newAlias.toAttribute());
                            outputChanged = true;
                            newOutputToConvertFunctions.putIfAbsent(newAliasName, convert);
                        }
                    }
                }
                newChildren.add(maybePushDownConvertFunctionsToChild(child, newAliases, newChildOutput));
            }

            // Populate convertFunctionsToAttributes. The values of convertFunctionsToAttributes are the new ReferenceAttributes
            // in the new UnionAll outputs created for the updated unionAll output after pushing down the conversion functions.
            return outputChanged
                ? rebuildUnionAll(unionAll, newChildren, newOutputToConvertFunctions, convertFunctionsToAttributes)
                : unionAll;
        }

        /**
         * Collect all conversion functions in the plan that convert the unionAll outputs to a different type,
         * the keys are the name of the old/existing attributes in the unionAll output, the values are all the conversion functions.
         */
        private static Map<String, Set<AbstractConvertFunction>> collectConvertFunctions(UnionAll unionAll, LogicalPlan plan) {
            Map<String, Set<AbstractConvertFunction>> convertFunctions = new HashMap<>();
            plan.forEachExpressionDown(AbstractConvertFunction.class, f -> {
                if (f.field() instanceof Attribute attr) {
                    // get the attribute from the UnionAll output by name and id
                    unionAll.output()
                        .stream()
                        .filter(a -> a.name().equals(attr.name()) && a.id() == attr.id())
                        .findFirst()
                        .ifPresent(unionAllAttr -> convertFunctions.computeIfAbsent(attr.name(), k -> new HashSet<>()).add(f));
                }
            });
            return convertFunctions;
        }

        /**
         * Push down the conversion functions into the child plan by adding an Eval with the new aliases on top of the child plan.
         */
        private static LogicalPlan maybePushDownConvertFunctionsToChild(LogicalPlan child, List<Alias> aliases, List<Attribute> output) {
            // Fork/UnionAll adds a projection on top of each child plan during resolveFork, check this pattern before pushing down
            // If the pattern doesn't match, something unexpected happened, just return the child as is
            if (aliases.isEmpty() == false && child instanceof Project project) {
                LogicalPlan childOfProject = project.child();
                Eval eval = new Eval(childOfProject.source(), childOfProject, aliases);
                return new Project(project.source(), eval, output);
            }
            return child;
        }

        /**
         * Rebuild the UnionAll with the new children and the new output after pushing down the conversion functions,
         * and populate convertFunctionsToAttributes with the mapping between conversion functions and the
         * new ReferenceAttributes in the new UnionAll output.
         */
        private static LogicalPlan rebuildUnionAll(
            UnionAll unionAll,
            List<LogicalPlan> newChildren,
            Map<String, AbstractConvertFunction> newOutputToConvertFunctions,
            Map<AbstractConvertFunction, Attribute> convertFunctionsToAttributes
        ) {
            // check if the new children has the same number of outputs, it could be different from the original unionAll output
            // if there are multiple explicit conversion functions on the same unionAll output attribute
            List<String> newChildrenOutputNames = newChildren.getFirst().output().stream().map(Attribute::name).toList();
            Holder<Boolean> childrenMatch = new Holder<>(true);
            newChildren.stream().skip(1).forEach(childPlan -> {
                List<String> names = childPlan.output().stream().map(Attribute::name).toList();
                if (names.equals(newChildrenOutputNames) == false) {
                    childrenMatch.set(false);
                }
            });
            if (childrenMatch.get() == false) {
                // new UnionAll children outputs do not match after pushing down convert functions,
                // cannot move on, return the original UnionAll
                return unionAll;
            }

            // rebuild the unionAll output according to its new children's output, and populate convertFunctionsToAttributes
            List<Attribute> newOutput = new ArrayList<>(newChildrenOutputNames.size());
            List<Attribute> oldOutput = unionAll.output();
            for (String attrName : newChildrenOutputNames) {
                // find the old attribute by name
                Attribute oldAttr = null;
                for (Attribute attr : oldOutput) {
                    if (attr.name().equals(attrName)) {
                        oldAttr = attr;
                        break;
                    }
                }
                if (oldAttr != null) { // keep the old UnionAll output unchanged
                    newOutput.add(oldAttr);
                } else { // this is a new attribute created by pushing down convert functions find the corresponding convert function
                    AbstractConvertFunction convert = newOutputToConvertFunctions.get(attrName);
                    if (convert != null) {
                        ReferenceAttribute newAttr = new ReferenceAttribute(
                            convert.source(),
                            null,
                            attrName,
                            convert.dataType(),
                            convert.nullable(),
                            null,
                            true
                        );
                        newOutput.add(newAttr);
                        convertFunctionsToAttributes.putIfAbsent(convert, newAttr);
                    } else {
                        // something unexpected happened, the attribute is neither the old attribute nor created by a convert function,
                        // return the original UnionAll
                        return unionAll;
                    }
                }
            }
            return new UnionAll(unionAll.source(), newChildren, newOutput);
        }

        /**
         * Replace the conversion functions with the corresponding attributes in the UnionAll output
         */
        private static LogicalPlan replaceConvertFunctions(
            LogicalPlan plan,
            Map<AbstractConvertFunction, Attribute> convertFunctionsToAttributes
        ) {
            if (convertFunctionsToAttributes.isEmpty()) {
                return plan;
            }
            return plan.transformExpressionsUp(AbstractConvertFunction.class, convertFunction -> {
                if (convertFunction.field() instanceof Attribute attr) {
                    for (Map.Entry<AbstractConvertFunction, Attribute> entry : convertFunctionsToAttributes.entrySet()) {
                        AbstractConvertFunction candidate = entry.getKey();
                        Attribute replacement = entry.getValue();
                        if (candidate == convertFunction
                            && candidate.field() instanceof Attribute candidateAttr
                            && candidateAttr.id() == attr.id()) {
                            // Make sure to match by attribute id, as ReferenceAttribute with the same name
                            // but with different id might be considered equal
                            return replacement;
                        }
                    }
                }
                return convertFunction;
            });
        }

        /**
         * Implicitly cast the outputs of the UnionAll branches to the common type, this applies to date and date_nanos types only
         */
        private static LogicalPlan implicitCastingUnionAllOutput(
            UnionAll unionAll,
            LogicalPlan plan,
            List<Attribute> updatedUnionAllOutput,
            Configuration configuration
        ) {
            // build a map of UnionAll output to a list of LogicalPlan that reference this output
            Map<Attribute, List<LogicalPlan>> outputToPlans = outputToPlans(unionAll, plan);

            List<List<Attribute>> outputs = unionAll.children().stream().map(LogicalPlan::output).toList();
            // only do implicit casting for date and date_nanos types for now, to be consistent with queries without subqueries
            List<DataType> commonTypes = commonTypes(outputs);

            Map<Integer, DataType> indexToCommonType = new HashMap<>();

            // Cast each branch's output to the common type
            List<LogicalPlan> newChildren = new ArrayList<>(unionAll.children().size());
            boolean outputChanged = false;
            for (LogicalPlan child : unionAll.children()) {
                List<Alias> newAliases = new ArrayList<>();
                List<Attribute> oldChildOutput = child.output();
                List<Attribute> newChildOutput = new ArrayList<>(oldChildOutput.size());
                for (int i = 0; i < oldChildOutput.size(); i++) {
                    Attribute oldOutput = oldChildOutput.get(i);
                    DataType targetType = commonTypes.get(i);
                    Attribute resolved = resolveAttribute(
                        oldOutput,
                        targetType,
                        i,
                        outputs,
                        unionAll,
                        outputToPlans,
                        newAliases,
                        indexToCommonType,
                        configuration
                    );
                    newChildOutput.add(resolved);
                    if (resolved != oldOutput) {
                        outputChanged = true;
                    }
                }
                // create a new eval for the casting expressions, and push it down under the projection
                newChildren.add(maybePushDownConvertFunctionsToChild(child, newAliases, newChildOutput));
            }

            // Update common types with overrides
            indexToCommonType.forEach(commonTypes::set);

            return outputChanged ? rebuildUnionAllOutput(unionAll, newChildren, commonTypes, updatedUnionAllOutput) : unionAll;
        }

        /**
         * Build a map of UnionAll output to a list of LogicalPlan that reference this output
         */
        private static Map<Attribute, List<LogicalPlan>> outputToPlans(UnionAll unionAll, LogicalPlan plan) {
            Map<Attribute, List<LogicalPlan>> outputToPlans = new HashMap<>();
            plan.forEachDown(p -> p.forEachExpression(Attribute.class, attr -> {
                if (p instanceof UnionAll == false && p instanceof Project == false) {
                    // get the attribute from the UnionAll output by name and id
                    unionAll.output()
                        .stream()
                        .filter(a -> a.name().equals(attr.name()) && a.id() == attr.id())
                        .findFirst()
                        .ifPresent(unionAllAttr -> outputToPlans.computeIfAbsent(attr, k -> new ArrayList<>()).add(p));
                }
            }));
            return outputToPlans;
        }

        private static List<DataType> commonTypes(List<List<Attribute>> outputs) {
            int columnCount = outputs.get(0).size();
            List<DataType> commonTypes = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                DataType type = outputs.get(0).get(i).dataType();
                for (List<Attribute> out : outputs) {
                    type = commonType(type, out.get(i).dataType());
                }
                commonTypes.add(type);
            }
            return commonTypes;
        }

        private static DataType commonType(DataType t1, DataType t2) {
            if (t1 == null || t2 == null) {
                return null;
            }
            t1 = t1.isCounter() ? t1.noCounter() : t1;
            t2 = t2.isCounter() ? t2.noCounter() : t2;
            if (t1 == t2) {
                return t1;
            }
            if (t1.isDate() && t2.isDate()) {
                return DATE_NANOS;
            }
            return null;
        }

        /**
         * Resolve the attribute to the target type, if target type is null, create:
         * an UnsupportedAttribute if the attribute is referenced in the parent plans,
         * a Null alias with keyword type if the attribute is not referenced in the parent plans.
         */
        private static Attribute resolveAttribute(
            Attribute oldAttr,
            DataType targetType,
            int columnIndex,
            List<List<Attribute>> outputs,
            UnionAll unionAll,
            Map<Attribute, List<LogicalPlan>> outputToPlans,
            List<Alias> newAliases,
            Map<Integer, DataType> indexToCommonType,
            Configuration configuration
        ) {
            if (targetType == null) {
                return createUnsupportedOrNull(oldAttr, columnIndex, outputs, unionAll, outputToPlans, newAliases, indexToCommonType);
            }

            if (targetType != NULL && oldAttr.dataType() != targetType) {
                var converterFactory = EsqlDataTypeConverter.converterFunctionFactory(targetType);
                if (converterFactory != null) {
                    var converter = converterFactory.apply(oldAttr.source(), oldAttr, configuration);
                    if (converter != null) {
                        Alias alias = new Alias(oldAttr.source(), oldAttr.name(), converter);
                        newAliases.add(alias);
                        return alias.toAttribute();
                    }
                }
            }
            return oldAttr;
        }

        private static Attribute createUnsupportedOrNull(
            Attribute oldAttr,
            int columnIndex,
            List<List<Attribute>> outputs,
            UnionAll unionAll,
            Map<Attribute, List<LogicalPlan>> outputToPlans,
            List<Alias> newAliases,
            Map<Integer, DataType> indexToCommonType
        ) {
            Attribute unionAttr = unionAll.output().get(columnIndex);

            if (outputToPlans.containsKey(unionAttr)) {
                // Unsupported attribute
                List<String> dataTypes = collectIncompatibleTypes(columnIndex, outputs);
                UnsupportedAttribute unsupported = new UnsupportedAttribute(
                    oldAttr.source(),
                    oldAttr.name(),
                    new UnsupportedEsField(oldAttr.name(), dataTypes),
                    "Column [" + oldAttr.name() + "] has conflicting data types in subqueries: " + dataTypes,
                    oldAttr.id()
                );
                newAliases.add(new Alias(oldAttr.source(), oldAttr.name(), unsupported));
                indexToCommonType.putIfAbsent(columnIndex, UNSUPPORTED);
                return unsupported;
            } else {
                // Null alias with keyword type
                Alias nullAlias = new Alias(oldAttr.source(), oldAttr.name(), new Literal(oldAttr.source(), null, KEYWORD));
                newAliases.add(nullAlias);
                indexToCommonType.putIfAbsent(columnIndex, KEYWORD);
                return nullAlias.toAttribute();
            }
        }

        private static List<String> collectIncompatibleTypes(int columnIndex, List<List<Attribute>> outputs) {
            List<String> dataTypes = new ArrayList<>();
            for (List<Attribute> out : outputs) {
                Attribute attr = out.get(columnIndex);
                if (attr instanceof FieldAttribute fa && fa.field() instanceof InvalidMappedField imf) {
                    dataTypes.addAll(imf.types().stream().map(DataType::typeName).toList());
                } else {
                    dataTypes.add(attr.dataType().typeName());
                }
            }
            return dataTypes;
        }

        /**
         * Rebuild the UnionAll with the new children and the new output after implicit casting date and date_nanos types,
         * and populate updatedUnionAllOutput with the list of attributes in the UnionAll output that have been updated.
         */
        private static UnionAll rebuildUnionAllOutput(
            UnionAll unionAll,
            List<LogicalPlan> newChildren,
            List<DataType> commonTypes,
            List<Attribute> updatedUnionAllOutput
        ) {
            // Rebuild the newUnionAll's output to ensure the correct attributes are used
            List<Attribute> oldOutput = unionAll.output();
            List<Attribute> newOutput = new ArrayList<>(oldOutput.size());

            for (int i = 0; i < oldOutput.size(); i++) {
                Attribute oldAttr = oldOutput.get(i);
                DataType commonType = commonTypes.get(i);

                if (oldAttr.dataType() != commonType) {
                    // keep the id unchanged, otherwise the downstream operators won't recognize the attribute
                    ReferenceAttribute newAttr = new ReferenceAttribute(
                        oldAttr.source(),
                        null,
                        oldAttr.name(),
                        commonType,
                        oldAttr.nullable(),
                        oldAttr.id(),
                        oldAttr.synthetic()
                    );
                    newOutput.add(newAttr);
                    updatedUnionAllOutput.add(newAttr);
                } else {
                    newOutput.add(oldAttr);
                }
            }
            return new UnionAll(unionAll.source(), newChildren, newOutput);
        }

        /**
         * Update the attributes referencing the updated UnionAll output.
         */
        private static LogicalPlan updateAttributesReferencingUpdatedUnionAllOutput(
            LogicalPlan plan,
            List<Attribute> updatedUnionAllOutput
        ) {
            Map<NameId, Attribute> idToUpdatedAttr = updatedUnionAllOutput.stream().collect(Collectors.toMap(Attribute::id, attr -> attr));
            return plan.transformExpressionsUp(Attribute.class, expr -> {
                Attribute updated = idToUpdatedAttr.get(expr.id());
                return (updated != null && expr.dataType() != updated.dataType()) ? updated : expr;
            });
        }
    }

    /**
     * Prune branches of a UnionAll that resolve to empty subqueries.
     * For example, given the following plan, the index resolution of 'remote:missingIndex' is EMPTY_SUBQUERY:
     * <pre>
     * UnionAll[[]]
     * |_EsRelation[test][...]
     * |_Subquery[]
     * | \_UnresolvedRelation[remote:missingIndex]
     * \_Subquery[]
     *   \_EsRelation[sample_data][...]
     * </pre>
     *
     * The branch with EMPTY_SUBQUERY index resolution is pruned in the plan after the rule is applied:
     * <pre>
     * UnionAll[[]]
     * |_EsRelation[test][...]
     * \_Subquery[]
     *   \_EsRelation[sample_data][...]
     * </pre>
     */
    private static class PruneEmptyUnionAllBranch extends ParameterizedAnalyzerRule<UnionAll, AnalyzerContext> {

        @Override
        protected LogicalPlan rule(UnionAll unionAll, AnalyzerContext context) {
            Map<IndexPattern, IndexResolution> indexResolutions = context.indexResolution();
            // check if any child is an UnresolvedRelation that resolves to an empty subquery
            List<LogicalPlan> newChildren = new ArrayList<>(unionAll.children().size());
            for (LogicalPlan child : unionAll.children()) {
                // check for UnresolvedRelation in the child plan tree
                Holder<Boolean> isEmptySubquery = new Holder<>(Boolean.FALSE);
                child.forEachUp(UnresolvedRelation.class, ur -> {
                    IndexResolution resolution = indexResolutions.get(ur.indexPattern());
                    if (resolution == IndexResolution.EMPTY_SUBQUERY) {
                        isEmptySubquery.set(Boolean.TRUE);
                    }
                });
                if (isEmptySubquery.get() == false) {
                    newChildren.add(child);
                }
            }
            return newChildren.size() == unionAll.children().size() ? unionAll : unionAll.replaceChildren(newChildren);
        }
    }
}
