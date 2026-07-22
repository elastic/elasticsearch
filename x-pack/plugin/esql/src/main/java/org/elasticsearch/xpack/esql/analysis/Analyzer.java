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
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.iplocation.api.DatabaseProperty;
import org.elasticsearch.iplocation.api.IpDataLookupInfo;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules.ParameterizedAnalyzerRule;
import org.elasticsearch.xpack.esql.analysis.rules.ResolveFunctions;
import org.elasticsearch.xpack.esql.analysis.rules.ResolvePromqlFunctions;
import org.elasticsearch.xpack.esql.analysis.rules.ResolveUnmapped;
import org.elasticsearch.xpack.esql.analysis.rules.ResolvedProjects;
import org.elasticsearch.xpack.esql.capabilities.ConfigurationAware;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.EmptyAttribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.ExternalMetadataAttribute;
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
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.expression.VirtualAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.CompactMultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedTsField;
import org.elasticsearch.xpack.esql.core.type.MissingEsField;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedSingleTypeEsField;
import org.elasticsearch.xpack.esql.core.type.TypeConflictedField;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.datasources.ExternalMetadataColumns;
import org.elasticsearch.xpack.esql.datasources.FileMetadataColumns;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern;
import org.elasticsearch.xpack.esql.expression.function.AggregateMetricDoubleNativeSupport;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.TimestampBoundsAware;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Absent;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AbsentOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AvgOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.First;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGauge;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToUnsignedLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvCount;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.vector.VectorFunction;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.DateTimeArithmeticOperation;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.ResolvedInference;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ApplyWindowFilter;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.SubstituteSurrogateExpressions;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TranslateTimeSeriesAggregate;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.TranslateTimeSeriesWithout;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslatePromqlToEsqlPlan;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.promql.TranslateTimeSeriesCollapse;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.DatasetShadowRelation;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.IpLocation;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Lookup;
import org.elasticsearch.xpack.esql.plan.logical.MMR;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesCollapse;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedIpLocation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewShadowRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;
import org.elasticsearch.xpack.esql.plan.logical.fuse.Fuse;
import org.elasticsearch.xpack.esql.plan.logical.fuse.FuseScoreEval;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.AntiJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinType;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.MarkJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;
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
import org.elasticsearch.xpack.esql.view.ViewCompaction;

import java.time.Duration;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SequencedMap;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.core.enrich.EnrichPolicy.GEO_MATCH_TYPE;
import static org.elasticsearch.xpack.esql.capabilities.TranslationAware.translatable;
import static org.elasticsearch.xpack.esql.core.expression.Expressions.toReferenceAttributesPreservingIds;
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
            new ResolveViewShadow(),
            new ViewCompactionPostIndexResolution(),
            new ResolveDatasetShadow(),
            new StripDatasetShadowRelations(),
            new ResolveExternalRelations(),
            new PruneEmptyUnionAllBranch(),
            new ResolveEnrich(),
            new ResolveIpLocation(),
            new ResolveLookupTables(),
            new ResolveFunctions(),
            new ResolvePromqlFunctions(),
            new ResolveTimestampBoundsAware(),
            new ResolveInference(),
            new DateMillisToNanosInEsRelation(),
            new ResolveTwoLeggedPunksInEsRelation(),
            // Must happen before Translating PromQL plan to ESQL plan
            new ResolveAndVerifyPromqlRefs(),
            // Populates the TS_COLLAPSE wrapping a PromqlCommand with dimensions and bounds drawn from the
            // PromqlCommand. The wrapped PromqlCommand is left in place and translated to ESQL nodes by the next rule.
            new TranslateTimeSeriesCollapse(),
            // translate PromQL plan to ESQL. It should run before TranslateTimeSeriesAggregate and implicit casting
            new TranslatePromqlToEsqlPlan()
        ),
        new Batch<>(
            "Resolution",
            new ResolveRefs(),
            new ImplicitCasting(),
            new ResolveUnionTypes(),  // Must be after ResolveRefs, so union types can be found
            new ResolveUnionTypesInUnionAll(),
            new ResolveUnmapped(),
            new InsertDefaultInnerTimeSeriesAggregate(),
            new ImplicitCastAggregateMetricDoubles(),
            new InsertFromAggregateMetricDouble()
        ),
        new Batch<>(
            "Finish Analysis",
            Limiter.ONCE,
            new ResolveImplicitTimeSeriesIdentityGrouping(),
            new ResolvedProjects(),
            new AddImplicitLimit(),
            new AddImplicitTimestampSort(),
            new VerifyTimeSeries(),
            // Replace TimeSeriesWithout grouping nodes with TimeSeriesMetadataAttribute carrying the excluded dimensions.
            // Must run before TranslateTimeSeriesAggregate which expects the lowered attribute form.
            new TranslateTimeSeriesWithout(),
            // translate metric aggregates early before they are converted to nested expressions
            new TranslateTimeSeriesAggregate(),
            new ApplyWindowFilter(),
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
        LogicalPlan analyzed = execute(plan);
        LogicalPlan verified = verify(analyzed, gatherPreAnalysisMetrics(plan, partialMetrics));
        // verify throws on failure, so we only reach here once the plan is valid: flush the warnings deferred during analysis.
        context().deferredHeaderWarnings().forEach(HeaderWarning::addWarning);
        return verified;
    }

    public LogicalPlan verify(LogicalPlan plan, BitSet partialMetrics) {
        Collection<Failure> failures = verifier.verify(plan, partialMetrics, context());
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
                t = t.withWidenedSmallNumeric();
                var type = t.getDataType();

                FieldAttribute attribute;
                if (t instanceof UnsupportedEsField uef) {
                    attribute = new UnsupportedAttribute(source, name, uef);
                } else if (t instanceof InvalidMappedTsField imtf) {
                    // Convert the TS role conflict directly to an UnsupportedAttribute with a meaningful message.
                    // The original types don't matter. We pass a custom error message, anyway, which will fail the query in the verifier.
                    var carrier = new UnsupportedEsField(imtf.getName(), List.of(), null, imtf.getProperties());
                    attribute = new UnsupportedAttribute(source, name, carrier, imtf.errorMessage());
                } else {
                    attribute = new FieldAttribute(source, parentName, null, name, t);
                }
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
     * Resolves {@link ViewShadowRelation} nodes against {@link AnalyzerContext#linkedResolution()}.
     * <p>
     * Each {@code ViewShadowRelation} represents a "if a remote project has an index with this
     * view's name, treat it as if the user wrote a remote index reference at this position"
     * lookup. The lenient field-caps integration (deferred to a follow-up PR) populates
     * {@code linkedResolution}, keyed by the shadow's {@link ViewShadowRelation#linkedIndexPattern()}
     * (view name + applicable exclusions). The full pattern is the lookup key — different
     * exclusion lists at the same view name produce distinct {@code ViewShadowRelation}
     * instances and may resolve differently (e.g. one comes back empty because of the
     * exclusions, the other resolves to a remote index). This rule:
     * <ul>
     *   <li>If a valid {@link IndexResolution} is present for the shadow's
     *       {@link ViewShadowRelation#linkedIndexPattern()}, replaces the shadow with an
     *       {@link EsRelation} built from the resolved {@link EsIndex} (same shape as
     *       {@link ResolveTable}'s {@code resolveIndex} for a strict UR).</li>
     *   <li>Otherwise leaves the shadow unresolved. {@link ViewCompactionPostIndexResolution}
     *       (which runs immediately after this rule) strips any unresolved shadow.</li>
     * </ul>
     */
    private static class ResolveViewShadow extends ParameterizedAnalyzerRule<ViewShadowRelation, AnalyzerContext> {

        @Override
        protected LogicalPlan rule(ViewShadowRelation shadow, AnalyzerContext context) {
            IndexResolution resolution = context.linkedResolution().get(shadow.linkedIndexPattern());
            if (resolution == null || resolution.isValid() == false) {
                // No remote index found (or lookup didn't run yet) — leave the shadow alone for
                // ViewCompactionPostIndexResolution to strip.
                return shadow;
            }
            EsIndex esIndex = resolution.get();
            var attributes = mappingAsAttributes(shadow.source(), esIndex.mapping());
            return new EsRelation(
                shadow.source(),
                esIndex.name(),
                IndexMode.STANDARD,
                esIndex.originalIndices(),
                esIndex.concreteIndices(),
                esIndex.indexNameWithModes(),
                attributes.isEmpty() ? NO_FIELDS : attributes
            );
        }
    }

    /**
     * Phase 2 of view compaction. Runs in the Initialize batch right after {@link ResolveTable},
     * once all reachable {@link UnresolvedRelation}s have been replaced with {@code EsRelation}s
     * (and once CPS's lenient field-caps rule has rewritten any matched {@code ViewShadowRelation}s).
     * Strips remaining unresolved shadows, flattens nested {@code ViewUnionAll} structures, and
     * unwraps remaining {@code NamedSubquery} wrappers. See {@link ViewCompaction} for the rationale
     * behind splitting compaction across the analyzer boundary.
     */
    private static class ViewCompactionPostIndexResolution extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            return ViewCompaction.postIndexResolution(plan);
        }
    }

    /**
     * Resolves {@link DatasetShadowRelation} nodes against {@link AnalyzerContext#linkedResolution()}.
     * The dataset analog of {@link ResolveViewShadow}.
     * <p>
     * Each {@code DatasetShadowRelation} represents a "if a linked project has an index with this
     * dataset's name, treat it as if the user wrote a remote index reference at this position" lookup.
     * {@code EsqlSession.preAnalyzeLinkedIndices} populates {@code linkedResolution}, keyed by the shadow's
     * {@link DatasetShadowRelation#linkedIndexPattern()} (dataset name + applicable exclusions). A linked
     * dataset/view of the same name has already failed the query on the detect rail before this rule runs;
     * a linked index of the same name produces a valid resolution here. This rule:
     * <ul>
     *   <li>If a valid {@link IndexResolution} is present for the shadow's
     *       {@link DatasetShadowRelation#linkedIndexPattern()}, replaces the shadow with an
     *       {@link EsRelation} built from the resolved {@link EsIndex} (same shape as
     *       {@link ResolveTable}'s {@code resolveIndex} for a strict UR).</li>
     *   <li>Otherwise leaves the shadow unresolved. {@link StripDatasetShadowRelations} (which runs
     *       immediately after this rule) strips any unresolved shadow.</li>
     * </ul>
     */
    private static class ResolveDatasetShadow extends ParameterizedAnalyzerRule<DatasetShadowRelation, AnalyzerContext> {

        @Override
        protected LogicalPlan rule(DatasetShadowRelation shadow, AnalyzerContext context) {
            IndexResolution resolution = context.linkedResolution().get(shadow.linkedIndexPattern());
            if (resolution == null || resolution.isValid() == false) {
                // No linked index found (or lookup didn't run yet) — leave the shadow alone for
                // StripDatasetShadowRelations to remove.
                return shadow;
            }
            EsIndex esIndex = resolution.get();
            var attributes = mappingAsAttributes(shadow.source(), esIndex.mapping());
            return new EsRelation(
                shadow.source(),
                esIndex.name(),
                IndexMode.STANDARD,
                esIndex.originalIndices(),
                esIndex.concreteIndices(),
                esIndex.indexNameWithModes(),
                attributes.isEmpty() ? NO_FIELDS : attributes
            );
        }
    }

    /**
     * Strips any {@link DatasetShadowRelation} that {@link ResolveDatasetShadow} did not fold into a
     * sibling {@code EsRelation}. The dataset analog of {@code ViewCompaction.stripViewShadowRelations},
     * but over the plain {@link UnionAll} the {@code DatasetRewriter} builds rather than a
     * {@link org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll}. Runs right after
     * {@link ResolveDatasetShadow} in the Initialize batch.
     * <p>
     * Delegates to {@link UnionAll#pruneEmptyBranches(java.util.function.Predicate)} so a matched shadow
     * (now an {@code EsRelation}) survives alongside the dataset's external relation as separate
     * {@code UnionAll} branches (Strategy A — no merging into a single combined relation). A single-survivor
     * union collapses to its lone child, so {@code FROM ds} with no remote match returns to exactly the bare
     * {@code UnresolvedExternalRelation} shape the non-CPS path produces.
     */
    private static class StripDatasetShadowRelations extends Rule<LogicalPlan, LogicalPlan> {

        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            return plan.transformDown(UnionAll.class, unionAll -> {
                // Plain UnionAll only — ViewUnionAll shadows are ViewCompaction's, and its single-child
                // wrappers must not collapse.
                if (unionAll instanceof ViewUnionAll) {
                    return unionAll;
                }
                LogicalPlan pruned = unionAll.pruneEmptyBranches(child -> child instanceof DatasetShadowRelation);
                if (pruned instanceof UnionAll prunedUnion
                    && prunedUnion instanceof ViewUnionAll == false
                    && prunedUnion.children().size() == 1) {
                    return prunedUnion.children().getFirst();
                }
                return pruned;
            });
        }
    }

    /**
     * Resolves UnresolvedExternalRelation nodes using pre-resolved metadata from ExternalSourceResolver.
     * This rule mirrors the ResolveTable pattern but uses ExternalSourceResolution instead of IndexResolution.
     * <p>
     * This rule creates {@link ExternalRelation} nodes from any SourceMetadata,
     * avoiding the need for source-specific logical plan nodes in core ESQL code.
     * <p>
     * Binds the user's {@code METADATA ...} clause. Every name in
     * {@link MetadataAttribute#ATTRIBUTES_MAP} (standard names like {@code _id}/{@code _index}/...)
     * and every name in {@link org.elasticsearch.xpack.esql.datasources.FileMetadataColumns#COLUMNS}
     * ({@code _file.path}, {@code _file.name}, ...) becomes an {@link ExternalMetadataAttribute} of
     * the registered type. Unknown names propagate as-is for the verifier to flag with the existing
     * "Unknown column" diagnostic. Names already present in the source's natural schema are skipped
     * — the source's own column wins.
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
            // Partition columns are path-derived and appear in the schema as plain ReferenceAttributes (indistinguishable
            // from data columns by type), so pass their names explicitly: _id.path pointing at a partition column must be
            // rejected loudly (the reader stamps _id per row from a data column, not from a path-derived constant).
            PartitionMetadata partitionMetadata = resolvedSource.fileList() != null ? resolvedSource.fileList().partitionMetadata() : null;
            Set<String> partitionColumnNames = partitionMetadata != null && partitionMetadata.isEmpty() == false
                ? partitionMetadata.partitionColumns().keySet()
                : Set.of();
            MetadataBindResult bindResult = bindMetadataFields(plan, metadata.schema(), partitionColumnNames);
            ExternalRelation relation = new ExternalRelation(
                plan.source(),
                tablePath,
                metadata,
                bindResult.schema(),
                resolvedSource.fileList(),
                resolvedSource.schemaMap(),
                plan.datasetName(),
                bindResult.unresolvedMetadata(),
                resolvedSource.declaredReadSpec()
            );
            return relation;
        }

        /**
         * Result of {@link #bindMetadataFields}: the enriched schema (resolved standard /
         * {@code _file.*} names appended to the base schema) and the list of metadata expressions the
         * bind could not resolve. The unresolved list is threaded
         * through to {@link ExternalRelation#metadataFields()} so the verifier's
         * {@code checkUnresolvedAttributes} walk fires the indexed-equivalent
         * {@code "Unresolved metadata pattern [...]"} error.
         */
        private record MetadataBindResult(List<Attribute> schema, List<? extends NamedExpression> unresolvedMetadata) {}

        /**
         * Walks the user's METADATA clause. Names registered in
         * {@link MetadataAttribute#ATTRIBUTES_MAP} or
         * {@link org.elasticsearch.xpack.esql.datasources.FileMetadataColumns#COLUMNS} are bound
         * to an {@link ExternalMetadataAttribute} appended to the source's natural schema. Names
         * registered in neither stay as {@code UnresolvedMetadataAttributeExpression} in the
         * returned {@code unresolvedMetadata} list — the verifier picks them up via the relation's
         * expression walk and fires its native {@code "Unresolved metadata pattern [...]"} error,
         * matching the diagnostic indexed {@code FROM x METADATA _typo} produces. Names already
         * present in the source's natural schema are skipped (the source's own column takes
         * precedence).
         */
        private static MetadataBindResult bindMetadataFields(
            UnresolvedExternalRelation plan,
            List<Attribute> baseSchema,
            Set<String> partitionColumnNames
        ) {
            if (plan.metadataFields().isEmpty()) {
                return new MetadataBindResult(baseSchema, List.of());
            }
            Set<String> existing = new LinkedHashSet<>();
            for (Attribute a : baseSchema) {
                existing.add(a.name());
            }
            List<Attribute> enriched = null;
            List<NamedExpression> unresolved = null;
            for (NamedExpression requested : plan.metadataFields()) {
                // FROM's parser threads non-standard names through UnresolvedMetadataAttributeExpression
                // (whose name() throws); EXTERNAL's parser threads plain UnresolvedAttribute. Resolve
                // the textual name from either shape without invoking the throwing accessor.
                String name = requested instanceof UnresolvedMetadataAttributeExpression unr ? unr.pattern() : requested.name();
                if (existing.contains(name)) {
                    continue;
                }
                // _id.path names the column the reader stamps _id from. If the dataset declares one but the resolved
                // schema has no such DATA column — a typo, the files lost it, or it is a partition/virtual column the
                // reader never materializes per row — reject the _id request loudly rather than returning silently-null
                // ids. Fires only when _id is actually asked for — a bad _id.path on a query that never reads _id is
                // moot, like any other unread column.
                if (ExternalMetadataColumns.ID.equals(name)) {
                    String idPath = declaredIdPath(plan);
                    if (idPath != null) {
                        Attribute idSource = null;
                        for (Attribute a : baseSchema) {
                            if (a.name().equals(idPath)) {
                                idSource = a;
                                break;
                            }
                        }
                        if (idSource == null) {
                            throw new IllegalArgumentException(
                                "[_id] is declared to come from column ["
                                    + idPath
                                    + "] (mappings._id.path), but no such column exists in the dataset's schema"
                            );
                        }
                        // A partition column is a path-derived constant surfaced as a plain ReferenceAttribute (not a
                        // Virtual/ExternalMetadata attribute), so it slips the type checks above; the reader classifies
                        // it in the partition branch and never stamps _id from it (silent null id). Reject it here.
                        if (idSource instanceof VirtualAttribute
                            || idSource instanceof ExternalMetadataAttribute
                            || partitionColumnNames.contains(idPath)) {
                            throw new IllegalArgumentException(
                                "[_id] is declared to come from ["
                                    + idPath
                                    + "] (mappings._id.path), which is not a data column of the files; _id must come from a "
                                    + "column the reader materializes per row"
                            );
                        }
                    }
                }
                DataType type = MetadataAttribute.dataType(name);
                if (type == null) {
                    type = FileMetadataColumns.COLUMNS.get(name);
                }
                if (type == null) {
                    // Unknown name — keep the unresolved expression so the verifier picks it up via
                    // ExternalRelation#metadataFields() and fires its native unresolved-pattern error.
                    if (unresolved == null) {
                        unresolved = new ArrayList<>();
                    }
                    unresolved.add(requested);
                    continue;
                }
                if (enriched == null) {
                    enriched = new ArrayList<>(baseSchema);
                }
                enriched.add(new ExternalMetadataAttribute(plan.source(), name, type));
                existing.add(name);
            }
            List<Attribute> resolvedSchema = enriched == null ? baseSchema : List.copyOf(enriched);
            List<? extends NamedExpression> unresolvedList = unresolved == null ? List.of() : List.copyOf(unresolved);
            return new MetadataBindResult(resolvedSchema, unresolvedList);
        }

        /** The declared {@code mappings._id.path}, or {@code null} when the dataset does not set {@code _id} from a column. */
        private static String declaredIdPath(UnresolvedExternalRelation plan) {
            var mapping = plan.mapping();
            return mapping != null && mapping.mappings() != null ? mapping.mappings().idPath() : null;
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

    /**
     * Resolves the transient {@link UnresolvedIpLocation} node produced by the parser into a fully-typed {@link IpLocation} node.
     * The output columns depend on the IP database schema, which is read from the {@link IpLocationResolution} carried by the
     * {@link AnalyzerContext}. That metadata is pre-fetched on the coordinator before analysis, so this rule never touches the
     * IP location service itself. If the service was unavailable, the database is unknown, or a requested property is invalid, the
     * node is left unresolved with a specific message, which the verifier turns into a failure.
     */
    private static class ResolveIpLocation extends ParameterizedAnalyzerRule<UnresolvedIpLocation, AnalyzerContext> {

        @Override
        protected LogicalPlan rule(UnresolvedIpLocation plan, AnalyzerContext context) {
            IpLocationResolution ipLocationResolution = context.ipLocationResolution();
            if (ipLocationResolution.serviceAvailable() == false) {
                return plan.withUnresolvedMessage("IP_LOCATION command requires the IP location service to be available");
            }
            IpDataLookupInfo info = ipLocationResolution.databaseInfo(plan.databaseFile());
            if (info == null) {
                return plan.withUnresolvedMessage(
                    Strings.format(
                        "IP location database [%s] is not recognized. Use a bundled MaxMind/ipinfo filename "
                            + "(e.g. GeoLite2-City.mmdb, GeoIP2-City.mmdb, asn.mmdb) or register the file via the Manage IP Geolocation "
                            + "Database API.",
                        plan.databaseFile()
                    )
                );
            }

            SequencedMap<String, Class<?>> filteredOutputFields;
            List<String> properties = plan.properties();
            if (properties == null) {
                filteredOutputFields = info.getDefaultFields();
            } else {
                Set<DatabaseProperty> validProperties = DatabaseProperty.buildValidSet(info.getFields().keySet());
                filteredOutputFields = new LinkedHashMap<>();
                for (String property : properties) {
                    DatabaseProperty dp;
                    try {
                        dp = DatabaseProperty.parseProperty(validProperties, property);
                    } catch (IllegalArgumentException e) {
                        return plan.withUnresolvedMessage(e.getMessage());
                    }
                    Class<?> type = info.getFields().get(dp.fieldName());
                    assert type != null : "valid property [" + dp.fieldName() + "] has no type in the database fields map";
                    filteredOutputFields.put(dp.fieldName(), type);
                }
            }

            return IpLocation.createInitialInstance(
                plan.source(),
                plan.child(),
                plan.input(),
                plan.outputPrefix(),
                plan.databaseFile(),
                plan.firstOnly(),
                filteredOutputFields
            );
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
            final var resolved = context.enrichResolution().getResolvedPolicy(plan.source());
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
                String error = context.enrichResolution().getError(plan.source());
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

    private static class VerifyTimeSeries extends ParameterizedAnalyzerRule<TimeSeriesAggregate, AnalyzerContext> {

        @Override
        protected boolean skipResolved() {
            return false;
        }

        @Override
        protected LogicalPlan rule(TimeSeriesAggregate plan, AnalyzerContext context) {
            if (plan.childrenResolved() == false) {
                return plan;
            }
            Failures failures = new Failures();
            plan.verify(failures);
            if (failures.hasFailures()) {
                throw new VerificationException(failures);
            }
            return plan;
        }
    }

    private static class ResolveAndVerifyPromqlRefs extends ParameterizedAnalyzerRule<LogicalPlan, AnalyzerContext> {
        @Override
        protected LogicalPlan rule(LogicalPlan plan, AnalyzerContext context) {
            if (plan.childrenResolved() == false) {
                return plan;
            }
            final List<Attribute> childrenOutput = new ArrayList<>();
            for (LogicalPlan child : plan.children()) {
                var output = child.output();
                childrenOutput.addAll(output);
            }
            if (plan instanceof TimeSeriesCollapse tsc) {
                Failures failures = new Failures();
                tsc.verify(failures);
                if (failures.hasFailures()) {
                    throw new VerificationException(failures);
                }
                return tsc;
            }
            if (plan instanceof PromqlCommand promql) {
                return resolvePromql(promql, childrenOutput).transformDown(PromqlCommand.class, p -> {
                    Failures failures = new Failures();
                    p.verify(failures);
                    if (failures.hasFailures()) {
                        throw new VerificationException(failures);
                    }
                    return p;
                });
            }
            return plan;
        }

        private LogicalPlan resolvePromql(PromqlCommand promql, List<Attribute> childrenOutput) {
            LogicalPlan promqlPlan = promql.promqlPlan();
            Function<UnresolvedAttribute, Expression> lambda = ua -> ResolveRefs.maybeResolveAttribute(ua, childrenOutput, log);
            // resolve the nested plan
            return promql.withPromqlPlan(promqlPlan.transformExpressionsDown(UnresolvedAttribute.class, lambda))
                // but also any unresolved expressions
                .transformExpressionsOnly(UnresolvedAttribute.class, lambda);
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
                case Fork f -> resolveFork(f, context.unmappedResolution());
                case Eval p -> resolveEval(p, childrenOutput);
                case Enrich p -> resolveEnrich(p, childrenOutput);
                case MvExpand p -> resolveMvExpand(p, childrenOutput);
                case Lookup l -> resolveLookup(l, childrenOutput);
                case LookupJoin j -> resolveLookupJoin(j, context);
                case AbstractSubqueryJoin sj -> resolveSubqueryJoin(sj);
                case Fuse fuse -> resolveFuse(fuse, childrenOutput);
                case Rerank r -> resolveRerank(r, childrenOutput, context);
                case Row row -> resolveRow(row);
                case MMR mmr -> resolveMMR(mmr, childrenOutput);
                default -> plan.transformExpressionsOnly(UnresolvedAttribute.class, ua -> maybeResolveAttribute(ua, childrenOutput));
            };

            return resolved;
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
            Set<String> unresolvedGroupingNames = new HashSet<>(newGroupings.size());
            for (Expression e : newGroupings) {
                Attribute attr = Expressions.attribute(e);
                if (attr != null) {
                    if (attr.resolved()) {
                        resolvedGroupings.add(attr);
                    } else {
                        unresolvedGroupingNames.add(attr.name());
                    }
                }
            }

            boolean allGroupingsResolved = groupings.size() == resolvedGroupings.size();
            if (allGroupingsResolved == false || Resolvables.resolved(aggregates) == false) {
                Holder<Boolean> changed = new Holder<>(false);
                var inputAttributes = new ArrayList<>(childrenOutput);
                // Remove input attributes with the same name as unresolved groupings: could be shadowed by not yet resolved renamed groups.
                // E.g. for
                // SET unmapped_fields="nullify"; ROW x = 1, language_code = 2
                // | STATS c = max(language_code) BY language_code = does_not_exist
                // max(language_code) should not be resolved to the input attribute language_code.
                inputAttributes.removeIf(a -> unresolvedGroupingNames.contains(a.name()));
                List<Attribute> resolvedList = NamedExpressions.mergeOutputAttributes(resolvedGroupings, inputAttributes);

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
                    // resolve each side independently — skip sides that are already resolved
                    leftKeys = Resolvables.resolved(config.leftFields())
                        ? config.leftFields()
                        : resolveUsingColumns(config.leftFields(), join.left().output(), "left");
                    rightKeys = Resolvables.resolved(config.rightFields())
                        ? config.rightFields()
                        : resolveUsingColumns(config.rightFields(), join.right().output(), "right");
                }
                config = new JoinConfig(type, leftKeys, rightKeys, joinOnConditions);
                boolean hasRemoteIndices = join.left().anyMatch(node -> node instanceof EsRelation relation && hasRemoteIndices(relation));
                var newLookupJoinMode = newLookupJoinMode(join.executesOn(), hasRemoteIndices);
                return new LookupJoin(join.source(), join.left(), join.right(), config, newLookupJoinMode);
            } else {
                // everything else is unsupported for now
                UnresolvedAttribute errorAttribute = new UnresolvedAttribute(join.source(), "unsupported", "Unsupported join type");
                // add error message
                return join.withConfig(new JoinConfig(type, singletonList(errorAttribute), emptyList(), null));
            }
        }

        private static ExecuteLocation newLookupJoinMode(ExecuteLocation mode, boolean hasRemoteIndices) {
            if (mode == ExecuteLocation.COORDINATOR) {
                return ExecuteLocation.COORDINATOR;
            } else if (mode == ExecuteLocation.REMOTE || hasRemoteIndices) {
                return ExecuteLocation.REMOTE;
            } else {
                return ExecuteLocation.ANY;
            }
        }

        private static boolean hasRemoteIndices(EsRelation relation) {
            return switch (relation.concreteIndices().size()) {
                case 0 -> false;// row
                case 1 -> relation.concreteIndices().containsKey(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY) == false;
                default -> true;
            };
        }

        /**
         * Resolves both sides of a SEMI/ANTI join created by {@link InSubqueryResolver}:
         * <ul>
         *   <li>Left fields: resolved against the left child's output using standard attribute resolution.</li>
         *   <li>Right fields: set to the subquery's single output column, or an error attribute if the
         *       subquery returns zero or more than one column, or references an index with empty
         *       mapping (right output is {@link #NO_FIELDS}). Stale right fields (e.g. after
         *       {@link ImplicitCasting} recreated the right subtree) are re-resolved.</li>
         *   <li>Right child: when the subquery returns exactly one resolved field and the top of the
         *       right plan is not already a {@link Project}, an explicit {@code Project[rightField]}
         *       is inserted. The data-node fragment optimizer prunes source attributes down to
         *       {@code _doc}, and only re-extracts fields that are referenced by an upstream
         *       operator inside the fragment; without an explicit Project the {@code id}-style
         *       single-field outputs would collapse to {@code _doc} and trip the post-optimization
         *       output verifier.</li>
         * </ul>
         */
        private AbstractSubqueryJoin resolveSubqueryJoin(AbstractSubqueryJoin subqueryJoin) {
            // Resolve left fields. Skip when every leftField is either already resolved or is an
            // UnresolvedAttribute that already carries a custom message: resolveUsingColumns
            // appends a " in left side of join" suffix on every call, and UnresolvedAttribute
            // equality includes the message, so re-processing an already-customized message would
            // loop forever in the rule executor. Mirrors the customMessage() bail-out in
            // resolveLookupJoin.
            List<Attribute> leftFields = subqueryJoin.config().leftFields();
            boolean leftNeedsResolution = leftFields.stream()
                .anyMatch(a -> a instanceof UnresolvedAttribute ua && ua.customMessage() == false);
            List<Attribute> leftKeys = leftNeedsResolution
                ? resolveUsingColumns(leftFields, subqueryJoin.left().output(), "left")
                : leftFields;

            // resolve right fields
            List<Attribute> rightFields = resolveRightFields(subqueryJoin);

            // Wrap the right side in an explicit Project on the single right field when the
            // subquery plan does not already contain a Project or Aggregate anywhere. Both nodes
            // pin the field for InsertFieldExtraction on the data node: Project explicitly lists
            // the kept attributes; Aggregate produces the grouping/aggregate aliases that drive
            // field extraction. When neither is present (e.g. plain {@code FROM ids}, or
            // {@code FROM ids | LIMIT 5} / {@code FROM ids | WHERE id > 0}), the local fragment
            // would otherwise collapse the right output to {@code _doc} after
            // {@code ReplaceSourceAttributes} and trip the post-optimization output verifier.
            // Skip when the right field failed to resolve (multi-column subquery, empty mapping)
            // since we have no concrete attribute to project.
            LogicalPlan right = subqueryJoin.right();
            if (rightFields.size() == 1
                && rightFields.get(0).resolved()
                && right.anyMatch(p -> p instanceof Project || p instanceof Aggregate) == false) {
                right = new Project(subqueryJoin.source(), right, rightFields);
            }

            JoinConfig joinConfig = new JoinConfig(
                subqueryJoin.config().type(),
                leftKeys,
                rightFields,
                subqueryJoin.config().joinOnConditions()
            );

            if (subqueryJoin instanceof MarkJoin markJoin) {
                return new MarkJoin(markJoin.source(), markJoin.left(), right, joinConfig, markJoin.markAttribute());
            }
            return subqueryJoin instanceof AntiJoin
                ? new AntiJoin(subqueryJoin.source(), subqueryJoin.left(), right, joinConfig)
                : new SemiJoin(subqueryJoin.source(), subqueryJoin.left(), right, joinConfig);
        }

        private static List<Attribute> resolveRightFields(AbstractSubqueryJoin semiJoin) {
            List<Attribute> rightFields = semiJoin.config().rightFields();
            if (rightFields.isEmpty() == false) {
                // Bail out if rightFields already carries an analyzer-supplied custom error message
                // (e.g. NO_FIELDS placeholder, multi-column subquery): re-creating a fresh
                // UnresolvedAttribute every iteration mints a new NameId and would never converge.
                // Mirrors the customMessage() bail-out used for left fields.
                if (rightFields.stream().anyMatch(a -> a instanceof UnresolvedAttribute ua && ua.customMessage())) {
                    return rightFields;
                }
                // Re-resolve rightFields if they became stale (e.g. after ImplicitCasting recreated the right subtree)
                if (rightFields.stream().anyMatch(a -> a.resolved() == false)) {
                    List<Attribute> rightOutput = semiJoin.right().output();
                    if (rightOutput.size() == 1) {
                        return singletonList(resolveSingleRightField(semiJoin, rightOutput.get(0)));
                    }
                }
                return rightFields;
            }
            List<Attribute> rightOutput = semiJoin.right().output();
            if (rightOutput.size() != 1) {
                return singletonList(
                    new UnresolvedAttribute(
                        semiJoin.source(),
                        "*",
                        "IN subquery must return exactly one column, found ["
                            + rightOutput.stream().map(Attribute::name).collect(Collectors.joining(", "))
                            + "]"
                    )
                );
            }
            return singletonList(resolveSingleRightField(semiJoin, rightOutput.get(0)));
        }

        /**
         * If the lone right-side output is the {@link #NO_FIELDS} placeholder (meaning the
         * subquery references an index with empty mapping and no projected/computed columns),
         * surface a clear analyzer error instead of letting the type-compatibility check fail
         * later with an obscure {@code [NULL]}-typed message. Otherwise return the attribute
         * unchanged.
         */
        private static Attribute resolveSingleRightField(AbstractSubqueryJoin semiJoin, Attribute rightAttr) {
            if (NO_FIELDS_NAME.equals(rightAttr.name())) {
                return new UnresolvedAttribute(semiJoin.source(), "*", "IN subquery cannot reference an index with empty mapping");
            }
            return rightAttr;
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

        private LogicalPlan resolveFork(Fork fork, UnmappedResolution unmappedResolution) {
            // we align the outputs of the sub plans such that they have the same columns
            boolean changed = false;
            List<LogicalPlan> newSubPlans = new ArrayList<>();
            // FORK branches share one source index, so align across them; subqueries/views (UnionAll) read independent
            // sources and are handled in ResolveUnmapped. See #142033.
            boolean alignUnmappedAcrossBranches = switch (unmappedResolution) {
                case LOAD, NULLIFY -> fork instanceof UnionAll == false;
                case DEFAULT -> false;
            };
            List<Attribute> outputUnion = Fork.outputUnion(fork.children());
            // DROP of an unmapped field in a branch is a mention: the field is materialized in that branch's source but dropped from its
            // output, so Fork.outputUnion misses it. Surface it as a FORK column when a sibling branch can surface it (the dropping branch
            // then null-fills it). Skip it when no branch can surface it (e.g. dropped in every branch), else it would be null everywhere
            // and isn't a real column.
            if (alignUnmappedAcrossBranches && fork.children().stream().anyMatch(ResolveRefs::branchCanSurfaceLoadedField)) {
                addDroppedUnmappedFieldsMissingFromUnion(outputUnion, unmappedFieldsDroppedByProjection(fork));
            }
            List<String> forkColumns = outputUnion.stream().map(Attribute::name).toList();
            Set<String> forkMaterializedUnmappedFieldNames = alignUnmappedAcrossBranches ? materializedUnmappedFieldNames(fork) : Set.of();

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

                List<Alias> aliases = new ArrayList<>(missing.size());
                List<FieldAttribute> toLoad = new ArrayList<>();
                for (Attribute attr : missing) {
                    // An unmapped field materialized in a sibling branch is materialized here too (rather than null-filled), unless this
                    // branch can't surface it: loaded from _source under load, null-typed under nullify. This keeps the branches' source
                    // relations symmetric. Matched by name so a sibling's generating command (EVAL/MV_EXPAND/...) doesn't hide it. #142033
                    if (alignUnmappedAcrossBranches
                        && forkMaterializedUnmappedFieldNames.contains(attr.name())
                        && branchCanSurfaceLoadedField(logicalPlan)) {
                        toLoad.add(unmappedResolution == UnmappedResolution.LOAD ? unmappedKeyword(attr) : nullifyField(attr));
                        continue;
                    }
                    // We cannot assign an alias with an UNSUPPORTED data type, so we use another type that is
                    // supported. This way we can add this missing column containing only null values to the fork branch output.
                    var attrType = alignmentDataType(attr);
                    attrType = attrType == UNSUPPORTED ? KEYWORD : attrType;
                    if (attrType.isCounter()) {
                        attrType = attrType.noCounter();
                    }
                    // use the current fork branch's source as the source of the alias, instead of the original FieldAttribute's source.
                    aliases.add(new Alias(source, attr.name(), new Literal(source, null, attrType)));
                }

                // materialize the unmapped fields in this branch's own source relation so they surface in its output
                if (toLoad.isEmpty() == false) {
                    LogicalPlan withLoaded = logicalPlan.transformUp(EsRelation.class, esr -> {
                        if (esr.indexMode() == IndexMode.LOOKUP) {
                            return esr;
                        }
                        Set<String> existingNames = esr.outputSet().names();
                        List<Attribute> newFields = new ArrayList<>(toLoad.size());
                        for (FieldAttribute field : toLoad) {
                            if (existingNames.contains(field.name()) == false) {
                                newFields.add(field);
                            }
                        }
                        return esr.withAdditionalAttributes(newFields);
                    });
                    // mark changed only if the relation gained fields, else the fixed-point iteration never terminates
                    if (withLoaded != logicalPlan) {
                        logicalPlan = withLoaded;
                        changed = true;
                    }
                }

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
                    if (forkColumns.isEmpty()) {
                        // When forkColumns is empty (all branches only have no-fields), resolveKeep with empty
                        // projections would resolve to all child output including no-fields. Create a Project with
                        // empty output directly so the no-fields marker doesn't leak into the fork branch output.
                        logicalPlan = new Project(logicalPlan.source(), logicalPlan, List.of());
                    } else {
                        // FORK alignment is structural, not user-named: emit a Project directly rather than
                        // routing through resolveKeep. A Keep on this path would falsely register every
                        // virtual attribute in the alignment projection (e.g. EXTERNAL's shim-injected
                        // _file.* family) as "the user explicitly KEEP'd it", which planWithoutSyntheticAttributes
                        // then refuses to strip — leaking the columns to the output. The projections here are
                        // already pre-resolved Attributes drawn from Fork.outputUnion, so keepResolver would
                        // be a no-op anyway (no wildcards, no UnresolvedNamePattern). A user-named KEEP _file.path
                        // upstream of the FORK still survives via its own Keep node in the branch's plan tree.
                        logicalPlan = new Project(logicalPlan.source(), logicalPlan, new ArrayList<>(newOutput));
                    }
                }

                newSubPlans.add(logicalPlan);
            }

            if (changed == false) {
                return fork;
            }

            return fork.replaceSubPlansAndOutput(newSubPlans, toReferenceAttributesPreservingIds(outputUnion, fork.output()));
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

        /**
         * Names of unmapped fields materialized by any FORK branch's {@link EsRelation}: {@link PotentiallyUnmappedKeywordEsField} under
         * {@code load}, {@link MissingEsField} under {@code nullify}. Scans the relations, not branch outputs, so a referencing generating
         * command (EVAL/MV_EXPAND/...) can't hide the origin.
         */
        private static Set<String> materializedUnmappedFieldNames(Fork fork) {
            Set<String> names = new HashSet<>();
            for (LogicalPlan branch : fork.children()) {
                branch.forEachDown(EsRelation.class, esr -> {
                    if (esr.indexMode() == IndexMode.LOOKUP) {
                        return;
                    }
                    for (Attribute attr : esr.output()) {
                        if (attr instanceof FieldAttribute fa
                            && (fa.field() instanceof PotentiallyUnmappedKeywordEsField || fa.field() instanceof MissingEsField)) {
                            names.add(fa.name());
                        }
                    }
                });
            }
            return names;
        }

        /**
         * Unmapped fields a {@link Project} in a FORK branch drops outright, in the projection input but neither surfaced nor referenced
         * (a plain {@code DROP}, not a {@code RENAME}). Detects both materialization markers: {@link PotentiallyUnmappedKeywordEsField}
         * under {@code load} and {@link MissingEsField} under {@code nullify}. Keyed by name, first occurrence wins. A field consumed by an
         * {@link Aggregate} (e.g., {@code STATS ... BY f}) is excluded: it was never a branch output column, so it must not become a
         * {@code FORK} column.
         */
        private static Map<String, FieldAttribute> unmappedFieldsDroppedByProjection(Fork fork) {
            Map<String, FieldAttribute> byName = new LinkedHashMap<>();
            for (LogicalPlan branch : fork.children()) {
                branch.forEachDown(Project.class, project -> {
                    Set<String> survivingNames = project.outputSet().names();
                    Set<String> referencedNames = project.references().names();
                    for (Attribute attr : project.child().output()) {
                        if (attr instanceof FieldAttribute fa
                            // We can ignore PUNKs here since they are by definition mapped in some indices (whereas
                            // PotentiallyUnmappedKeywordEsField can be entirely unmapped).
                            && (fa.field() instanceof PotentiallyUnmappedKeywordEsField || fa.field() instanceof MissingEsField)
                            && survivingNames.contains(fa.name()) == false
                            && referencedNames.contains(fa.name()) == false) {
                            byName.putIfAbsent(fa.name(), fa);
                        }
                    }
                });
            }
            return byName;
        }

        /**
         * Mutates {@code outputUnion} in place, inserting a loader for each dropped unmapped fields missing from it right before the
         * {@code _fork} discriminator, so a {@code DROP}-mentioned field lands where a {@code WHERE}/{@code KEEP}-mentioned one would and
         * {@code _fork} stays last.
         */
        private static void addDroppedUnmappedFieldsMissingFromUnion(
            List<Attribute> outputUnion,
            Map<String, FieldAttribute> droppedUnmappedFields
        ) {
            if (droppedUnmappedFields.isEmpty()) {
                return;
            }
            Set<String> unionNames = new HashSet<>(Expressions.names(outputUnion));
            List<Attribute> loaders = new ArrayList<>();
            for (Map.Entry<String, FieldAttribute> entry : droppedUnmappedFields.entrySet()) {
                if (unionNames.contains(entry.getKey()) == false) {
                    FieldAttribute dropped = entry.getValue();
                    // Match how the field was materialized: a nullified MissingEsField under nullify, else an insisted keyword under load.
                    loaders.add(dropped.field() instanceof MissingEsField ? nullifyField(dropped) : unmappedKeyword(dropped));
                }
            }
            if (loaders.isEmpty()) {
                return;
            }
            int forkFieldIndex = Iterables.indexOf(outputUnion, a -> a.name().equals(Fork.FORK_FIELD));
            if (forkFieldIndex < 0) {
                forkFieldIndex = outputUnion.size();
            }
            outputUnion.addAll(forkFieldIndex, loaders);
        }

        /**
         * Whether an unmapped field materialized at this branch's source would reach the branch output: true only if walking
         * column-preserving unary plans from the root reaches a non-LOOKUP {@link EsRelation} (a Project/Aggregate in the way drops it).
         */
        private static boolean branchCanSurfaceLoadedField(LogicalPlan plan) {
            if (plan instanceof EsRelation esRelation) {
                return esRelation.indexMode() != IndexMode.LOOKUP;
            }
            if (plan instanceof Project || plan instanceof Aggregate) {
                return false;
            }
            if (plan instanceof Join join && join.config().type() == JoinTypes.LEFT) {
                return branchCanSurfaceLoadedField(join.left());
            } else if (plan instanceof UnaryPlan unaryPlan) {
                return branchCanSurfaceLoadedField(unaryPlan.child());
            } else {
                return false;
            }
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
                    // Multi-key LOOKUP JOIN re-entry after ResolveUnmapped: prior-pass-resolved keys pass through.
                    resolved.add(col);
                }
            }
            return resolved;
        }

        public static FieldAttribute unmappedKeyword(Attribute attribute) {
            String name = attribute.name();
            int lastDot = name.lastIndexOf('.');
            String parentName = lastDot < 0 ? null : name.substring(0, lastDot);
            String leafName = lastDot < 0 ? name : name.substring(lastDot + 1);
            return new FieldAttribute(
                attribute.source(),
                parentName,
                attribute.qualifier(),
                name,
                new PotentiallyUnmappedKeywordEsField(leafName)
            );
        }

        /**
         * A {@link FieldAttribute} backed by a {@link MissingEsField} of type {@link DataType#NULL}, i.e., the
         * {@code unmapped_fields="nullify"} marker.
         */
        public static FieldAttribute nullifyField(Attribute attribute) {
            return new FieldAttribute(
                attribute.source(),
                null,
                attribute.qualifier(),
                attribute.name(),
                new MissingEsField(attribute.name())
            );
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
                // _fork differs per branch for the same document, use VALUES to collect all
                // branch names into a multi-value field
                // All other columns come from the same document in every branch, use
                // FIRST(col, NULL), as "any value"
                Expression agg = attr.name().equals(discriminator.name())
                    ? new Values(source, attr, aggFilter, AggregateFunction.NO_WINDOW)
                    : new First(source, attr, Literal.NULL).withFilter(new IsNotNull(source, attr));
                if (agg.resolved()) {
                    aggregates.add(new Alias(source, attr.name(), agg));
                }
            }

            return resolveAggregate(new Aggregate(source, scoreEval, new ArrayList<>(keys), aggregates), childrenOutput);
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
            var resolved = resolveFields(eval.fields(), childOutput);
            return resolved != null ? new Eval(eval.source(), eval.child(), resolved) : eval;
        }

        /**
         * Resolve Row fields, allowing later fields to reference earlier ones using attribute references.
         * Field deduplication (shadowing) is handled by {@link Row#output()} via mergeOutputAttributes.
         */
        private LogicalPlan resolveRow(Row row) {
            var resolved = resolveFields(row.fields(), List.of());
            return resolved != null ? new Row(row.source(), resolved) : row;
        }

        private List<Alias> resolveFields(List<Alias> fields, List<Attribute> initialInputs) {
            List<Attribute> allResolvedInputs = new ArrayList<>(initialInputs);
            List<Alias> newFields = new ArrayList<>();
            boolean changed = false;
            for (Alias field : fields) {
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
            return changed ? newFields : null;
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
            if (unmappedResolution != UnmappedResolution.DEFAULT) {
                return new ResolvingProject(
                    keep.source(),
                    keep.child(),
                    inputAttributes -> keepResolver(keep.projections(), inputAttributes)
                );
            }
            List<NamedExpression> resolved = keepResolver(keep.projections(), keep.child().output());
            // Provenance for the external-metadata surfacing rule: when an explicit KEEP names an
            // engine-synthesized virtual column (external metadata: _file.*, _index, ...), keep the
            // result as a Keep node — NOT a bare Project — so planWithoutSyntheticAttributes can tell
            // "the user kept this virtual column" apart from "a DROP carried it forward". A DROP
            // resolves to a plain Project (resolveDrop) and a KEEP * routes through
            // excludeExternalMetadata, so neither produces a Keep that lists a VirtualAttribute.
            //
            // This Keep node is emitted ONLY when a virtual column was explicitly kept; every other
            // KEEP (the overwhelmingly common regular-index case) still resolves to a plain Project,
            // so the regular-index plan shape — and its golden snapshots — are unchanged.
            boolean keptVirtual = false;
            for (NamedExpression ne : resolved) {
                if (ne instanceof VirtualAttribute) {
                    keptVirtual = true;
                    break;
                }
            }
            return keptVirtual ? new Keep(keep.source(), keep.child(), resolved) : new Project(keep.source(), keep.child(), resolved);
        }

        // Engine-synthesized columns (today: {@code _file.*}) are never expanded by {@code KEEP *}
        // or implicit projections — users must request them by name. Identification is type-based
        // through the {@link VirtualAttribute} marker so future virtual attributes opt in by
        // class hierarchy rather than name convention.
        private static <T extends NamedExpression> List<T> excludeExternalMetadata(List<T> attributes) {
            List<T> filtered = new ArrayList<>(attributes.size());
            for (T attr : attributes) {
                if (attr instanceof VirtualAttribute == false) {
                    filtered.add(attr);
                }
            }
            return filtered;
        }

        private static List<NamedExpression> keepResolver(List<? extends NamedExpression> projections, List<Attribute> childOutput) {
            List<NamedExpression> resolvedProjections;
            // start with projections

            // no projection specified or just *
            if (projections.isEmpty() || (projections.size() == 1 && projections.getFirst() instanceof UnresolvedStar)) {
                // Widen List<Attribute> to List<NamedExpression> via copy; safe because every
                // Attribute is a NamedExpression and the result is a fresh, mutable list.
                resolvedProjections = new ArrayList<>(excludeExternalMetadata(childOutput));
            }
            // otherwise resolve them
            else {
                Map<NamedExpression, Integer> priorities = new LinkedHashMap<>();
                for (var proj : projections) {
                    final List<Attribute> resolved;
                    final int priority;
                    if (proj instanceof UnresolvedStar) {
                        resolved = excludeExternalMetadata(childOutput);
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
            return unmappedResolution != UnmappedResolution.DEFAULT
                ? new ResolvingProject(drop.source(), drop.child(), inputAttributes -> dropResolver(drop.removals(), inputAttributes, true))
                : new Project(drop.source(), drop.child(), dropResolver(drop.removals(), drop.output(), false));
        }

        private static List<NamedExpression> dropResolver(
            List<NamedExpression> removals,
            List<Attribute> childOutput,
            boolean ignoreUnmatchedPatterns
        ) {
            // DROP must operate over the full childOutput — including any external metadata
            // (`_file.*`, partition columns) the user already pulled in via KEEP — so it can
            // remove a data column without silently stripping previously-kept virtual columns.
            // Wildcard / default-output filtering is handled in keepResolver and
            // planWithoutSyntheticAttributes, not here.
            List<NamedExpression> resolvedProjections = new ArrayList<>(childOutput);

            for (NamedExpression ne : removals) {
                List<? extends NamedExpression> resolved;

                if (ne instanceof UnresolvedNamePattern np) {
                    resolved = resolveAgainstList(np, childOutput);
                    // A wildcard that matches no field resolves to a single unresolved UnresolvedPattern.
                    if (ignoreUnmatchedPatterns && resolved.size() == 1 && resolved.getFirst() instanceof UnresolvedAttribute) {
                        continue;
                    }
                } else if (ne instanceof UnresolvedAttribute ua) {
                    resolved = resolveAgainstList(ua, childOutput);
                } else {
                    resolved = singletonList(ne);
                }

                // the return list might contain either resolved elements or unresolved ones.
                // if things are resolved, remove them - if not add them to the list to trip the Verifier;
                // thus make sure to remove the intersection but add the unresolved difference (if any).
                // so, remove things that are in common
                Set<? extends NamedExpression> resolvedSet = new HashSet<>(resolved);
                resolvedProjections.removeIf(resolvedSet::contains);
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
            return unmappedResolution == UnmappedResolution.DEFAULT
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
                // For type-conflicted fields, defer to ResolveUnionTypes and ultimately UnionTypesCleanup (which produces the "Cannot use
                // field ... due to ambiguities" error for incompatible ones). Reading dataType() off an TypeConflictedField returns
                // UNSUPPORTED, which would otherwise produce a misleading error here.
                boolean deferToUnionTypes = resolved instanceof FieldAttribute fa && fa.hasTypeConflicts();
                if (deferToUnionTypes == false && resolved.resolved() && resolved.dataType() != NULL && enrich.policy() != null) {
                    String matchType = enrich.policy().getType();
                    List<DataType> allowed = allowedEnrichTypes(matchType);
                    if (allowed.contains(resolved.dataType()) == false) {
                        resolved = ua.withUnresolvedMessage(
                            Strings.format(
                                "Unsupported type [%s] for enrich matching field [%s]; only [%s] allowed for type [%s]",
                                resolved.dataType().typeName(),
                                ua.name(),
                                allowed.stream().map(DataType::typeName).collect(Collectors.joining(", ")),
                                matchType
                            )
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

        private LogicalPlan resolveMMR(MMR mmr, List<Attribute> childrenOutput) {
            MMR resolved = (MMR) mmr.transformExpressionsOnly(UnresolvedAttribute.class, ua -> maybeResolveAttribute(ua, childrenOutput));

            Expression queryVector = resolved.queryVector();

            if (queryVector != null && (queryVector.dataType().isNumeric() || queryVector.dataType() == KEYWORD)) {
                return new MMR(
                    resolved.source(),
                    resolved.child(),
                    resolved.diversifyField(),
                    resolved.limit(),
                    new ToDenseVector(resolved.queryVector().source(), resolved.queryVector()),
                    resolved.options()
                );
            }

            return resolved;
        }

        private static final List<DataType> GEO_TYPES = List.of(GEO_POINT, GEO_SHAPE);
        private static final List<DataType> NON_GEO_TYPES = List.of(KEYWORD, TEXT, IP, LONG, INTEGER, FLOAT, DOUBLE, DATETIME);

        private List<DataType> allowedEnrichTypes(String matchType) {
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

    private static class ResolveTimestampBoundsAware extends ParameterizedAnalyzerRule<LogicalPlan, AnalyzerContext> {

        @Override
        protected boolean skipResolved() {
            return false;
        }

        @Override
        protected LogicalPlan rule(LogicalPlan plan, AnalyzerContext context) {
            var bounds = context.timestampBounds();
            if (bounds == null) {
                return plan;
            }
            if (plan instanceof TimestampBoundsAware.OfLogicalPlan tba && tba.needsTimestampBounds()) {
                plan = tba.withTimestampBounds(
                    Literal.dateTime(plan.source(), bounds.start()),
                    Literal.dateTime(plan.source(), bounds.end())
                );
            }
            return plan.transformExpressionsUp(Expression.class, expression -> {
                if (expression instanceof TimestampBoundsAware.OfExpression tba && tba.needsTimestampBounds()) {
                    return tba.withTimestampBounds(
                        Literal.dateTime(expression.source(), bounds.start()),
                        Literal.dateTime(expression.source(), bounds.end())
                    );
                }
                return expression;
            });
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
                    completion.taskSettings(),
                    completion.timeout()
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
            // TimeSeriesCollapse is included because const-folded PromQL plans (e.g. literals on empty indices)
            // replace PromqlCommand with a LocalRelation but retain the TimeSeriesCollapse wrapper.
            boolean isTsAggregate = logicalPlan.collectFirstChildren(
                lp -> lp instanceof TimeSeriesAggregate || lp instanceof PromqlCommand || lp instanceof TimeSeriesCollapse
            ).isEmpty() == false;

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

            boolean hasTimeSeries = child.collect(EsRelation.class, r -> r.indexMode().isTsdb()).isEmpty() == false;
            if (hasTimeSeries == false) {
                return limit;
            }

            // Inject the OrderBy below each (to handle FORK) innermost Limit.
            return limit.transformDown(Limit.class, l -> {
                if (l.child().collect(Limit.class).isEmpty()) {
                    var localChild = l.child();
                    var localTimestampAttr = localChild.collect(EsRelation.class, r -> r.indexMode().isTsdb())
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
     * The EsqlIndexResolver will create TypeConflictedField instances for fields that are ambiguous (i.e. have multiple mappings).
     * During {@link ResolveRefs} we do not convert these to UnresolvedAttribute instances, as we want to first determine if they can
     * instead be handled by conversion functions within the query. This rule looks for matching conversion functions and converts
     * those fields into UnionTypeEsField, which encapsulates the knowledge of how to convert these into a single type.
     * This knowledge will be used later in generating the FieldExtractExec with built-in type conversion.
     * Any fields which could not be resolved by conversion functions will be converted to UnresolvedAttribute instances in a later rule
     * (See {@link UnionTypesCleanup} below).
     */
    private static class ResolveUnionTypes extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

        record TypeResolutionKey(String fieldName, DataType fieldType) {}

        @Override
        public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
            UnionTypeResolutionState state = new UnionTypeResolutionState();
            return plan.transformUp(LogicalPlan.class, p -> p.childrenResolved() == false ? p : doRule(p, state, context));
        }

        private static class UnionTypeResolutionState {
            private final List<Attribute.IdIgnoringWrapper> unionFieldAttributes = new ArrayList<>();
            private boolean isAfterAggregate = false;
        }

        private static LogicalPlan doRule(LogicalPlan plan, UnionTypeResolutionState state, AnalyzerContext context) {
            // Collect field attributes from previous runs
            if (plan instanceof EsRelation rel) {
                state.unionFieldAttributes.clear();
                // A new source relation may belong to a sibling FORK/UnionAll branch; Aggregate context is branch-local.
                state.isAfterAggregate = false;
                for (Attribute attr : rel.output()) {
                    if (attr instanceof FieldAttribute fa && fa.field() instanceof UnionTypeEsField && fa.synthetic()) {
                        state.unionFieldAttributes.add(fa.ignoreId());
                    }
                }
            }

            if (state.isAfterAggregate) {
                return plan;
            }

            int alreadyAddedUnionFieldAttributes = state.unionFieldAttributes.size();
            // See if the eval function has an unresolved UnionTypeEsField field
            // Replace the entire convert function with a new FieldAttribute (containing type conversion knowledge)
            plan = plan.transformExpressionsOnly(e -> {
                if (e instanceof ConvertFunction convert) {
                    return resolveConvertFunction(convert, state.unionFieldAttributes, context);
                }
                return e;
            });

            boolean generatedUnionFields = state.unionFieldAttributes.size() > alreadyAddedUnionFieldAttributes;
            if (generatedUnionFields == false && plan instanceof Aggregate == false) {
                return plan;
            }

            if (generatedUnionFields) {
                plan = addGeneratedFieldsToEsRelations(
                    plan,
                    state.unionFieldAttributes.stream().map(attr -> (FieldAttribute) attr.inner()).toList()
                );
            }

            if (plan instanceof Aggregate) {
                // Parent plans see aggregate output, not source fields, even when a grouping key preserves the same name and id.
                state.unionFieldAttributes.clear();
                state.isAfterAggregate = true;
            }
            return plan;
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
                res = carryOverSyntheticAttributesThroughProjects(res);
            }
            return res;
        }

        /**
         * This method decides how to handle a convert function (e.g., {@code TO_INTEGER(foo)}, {@code foo::double}) when applied to a field
         * that has different types across indices. It doesn't discover or collect type information; that's already done during index
         * resolution.
         *
         * <p>There are three cases to handle.
         * <ol>
         *   <li>If the field has unresolved type conflicts ({@link TypeConflictedField}), we try to build a {@link UnionTypeEsField} with
         *   per-type conversions.</li>
         *   <li>If the field was already implicitly cast to a union type ({@link UnionTypeEsField}), rewrap with the explicit cast.</li>
         *   <li>If the convert's input is itself a convert, e.g.: {@code foo::long::double}, recurse and resolve the inner one first.</li>
         * </ol>
         *
         * @return The resolved expression
         */
        private static Expression resolveConvertFunction(
            ConvertFunction convert,
            List<Attribute.IdIgnoringWrapper> unionFieldAttributes,
            AnalyzerContext context
        ) {
            Expression convertExpression = (Expression) convert;
            if (convert.field() instanceof FieldAttribute fa && fa.field() instanceof TypeConflictedField tcf) {
                // The field has an unresolved type conflict (TypeConflictedField), so we attempt to create UnionTypeEsField with
                // index-specific conversions
                Map<TypeResolutionKey, Expression> typeResolutions = new HashMap<>();
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
                // TO_GAUGE is a no-op when every branch is already a non-counter type (including aggregate_metric_double).
                // Strip it so union resolution can defer to implicit aggregate_metric_double casting in aggregations.
                if (convert instanceof ToGauge && ToGauge.isNoOpOnAllUnionTypes(tcf)) {
                    return fa;
                }
                tcf.types().forEach(type -> {
                    if (supportedTypes.contains(type.widenSmallNumeric())) {
                        typeResolutions(convert, type, fa, tcf, typeResolutions);
                    }
                });

                // If all mapped types were resolved, create a new FieldAttribute with the resolved UnionTypeEsField
                if (typeResolutions.size() == tcf.getTypesToIndices().size()) {
                    boolean loadUnmappedFields = context.unmappedResolution() == UnmappedResolution.LOAD;
                    if (skipMultiTypeForPotentiallyUnmappedKeyword(loadUnmappedFields, tcf, supportedTypes)) {
                        return convertExpression;
                    }

                    Expression potentiallyUnmappedConversion = tcf.isPotentiallyUnmapped()
                        ? ResolveUnionTypes.typeSpecificConvert(convert, fa.source(), KEYWORD, tcf)
                        : null;
                    EsField resolvedField = resolvedUnionTypeFields(fa, tcf, typeResolutions, potentiallyUnmappedConversion, context);
                    return createIfDoesNotAlreadyExist(fa, resolvedField, unionFieldAttributes);
                }
            } else if (convert.field() instanceof FieldAttribute fa
                && fa.synthetic() == false // UnionTypeEsField in EsRelation created by DateMillisToNanosInEsRelation or
                                           // ResolveTwoLeggedPunksInEsRelation has synthetic = false
                && fa.field() instanceof UnionTypeEsField unionTypeEsField) {
                    // This is an explicit casting of a union typed field that has been converted to UnionTypeEsField in EsRelation by
                    // DateMillisToNanosInEsRelation or ResolveTwoLeggedPunksInEsRelation, it is not necessary to cast it again to the same
                    // type, replace the implicit casting
                    // with explicit casting. However, it is useful to differentiate implicit and explicit casting in some cases, for
                    // example, an expression like multiTypeEsField(synthetic=false, date_nanos)::date_nanos::datetime is rewritten to
                    // multiTypeEsField(synthetic=true, date_nanos)::datetime, the implicit casting is overwritten by explicit casting and
                    // the multiTypeEsField is not casted to datetime directly.
                    if (convert.isNoop()
                        && (unionTypeEsField.getUnmappedConversionExpression() == null || convert.supportedTypes().contains(KEYWORD))) {
                        return createIfDoesNotAlreadyExist(fa, fa.field(), unionFieldAttributes);
                    }

                    Set<DataType> supportedTypes = convert.supportedTypes();
                    if (areMappedTypesSupported(unionTypeEsField, supportedTypes)) {
                        Expression unmappedExpr = unionTypeEsField.getUnmappedConversionExpression();
                        // Resolve surrogates immediately, since expressions stored in UnionTypeEsField are serialized
                        // to data nodes, and SurrogateExpressions cannot be serialized.
                        Expression resolvedConvertExpression = SubstituteSurrogateExpressions.rule(convertExpression);
                        UnionTypeEsField rewrapped = unionTypeEsField.rewrapWithCast(resolvedConvertExpression);

                        if (unmappedExpr instanceof AbstractConvertFunction existingConvert) {
                            if (supportedTypes.contains(KEYWORD)) {
                                Expression keywordField = existingConvert.field();
                                Expression rewrappedUnmapped = resolvedConvertExpression.replaceChildren(singletonList(keywordField));
                                rewrapped = rewrapped.withPotentiallyUnmappedExpression(rewrappedUnmapped);
                            } else {
                                // At the moment this path is exercised by TO_DEGREES/TO_RADIANS for single-type PUNKs under LOAD.
                                // Function cannot consume keyword, so keep mapped branches and nullify unmapped ones. See #150378.
                                rewrapped = rewrapped.withPotentiallyUnmappedExpression(null);
                            }
                        } else if (unmappedExpr != null) {
                            throw new IllegalStateException("Unexpected potentially unmapped expression for [" + fa.fieldName() + "]");
                        }

                        return createIfDoesNotAlreadyExist(fa, rewrapped, unionFieldAttributes);
                    } else if (unionTypeEsField.getUnmappedConversionExpression() != null) {
                        String msg = supportedTypes.contains(KEYWORD)
                            ? "One or more mapped types of partially unmapped field [%s] cannot be accepted in [%s]"
                            : "[%s] is loaded as [KEYWORD] where unmapped, but [%s] does not accept [KEYWORD]";

                        msg = String.format(Locale.ROOT, msg, fa.name(), convertExpression.sourceText());

                        return new UnresolvedAttribute(fa.source(), fa.name(), msg);
                    }
                } else if (convert.field() instanceof AbstractConvertFunction subConvert) {
                    return convertExpression.replaceChildren(
                        singletonList(resolveConvertFunction(subConvert, unionFieldAttributes, context))
                    );
                }
            return convertExpression;
        }

        private static boolean skipMultiTypeForPotentiallyUnmappedKeyword(
            boolean loadUnmappedFields,
            TypeConflictedField tcf,
            Set<DataType> supportedTypes
        ) {
            return loadUnmappedFields && tcf.isPotentiallyUnmapped() && supportedTypes.contains(KEYWORD) == false;
        }

        private static Expression createIfDoesNotAlreadyExist(
            FieldAttribute fa,
            EsField resolvedField,
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

        private static EsField resolvedUnionTypeFields(
            FieldAttribute fa,
            TypeConflictedField tcf,
            Map<TypeResolutionKey, Expression> typeResolutions,
            @Nullable Expression potentiallyUnmappedConversion,
            AnalyzerContext context
        ) {
            Map<String, Expression> typesToConversionExpressions = new HashMap<>();
            tcf.getTypesToIndices().forEach((typeName, indexNames) -> {
                DataType type = DataType.fromTypeName(typeName);
                TypeResolutionKey key = new TypeResolutionKey(fa.name(), type);
                if (typeResolutions.containsKey(key)) {
                    typesToConversionExpressions.put(typeName, typeResolutions.get(key));
                }
            });
            return buildUnionTypeField(tcf, typesToConversionExpressions, potentiallyUnmappedConversion, context);
        }

        private static UnionTypeEsField buildUnionTypeField(
            TypeConflictedField tcf,
            Map<String, Expression> typesToConversionExpressions,
            @Nullable Expression unmappedConversionExpression,
            AnalyzerContext context
        ) {
            return context.minimumVersion().supports(CompactMultiTypeEsField.CompactMultiTypeEsField)
                ? CompactMultiTypeEsField.resolveFrom(tcf, typesToConversionExpressions, unmappedConversionExpression)
                : MultiTypeEsField.resolveFrom(tcf, typesToConversionExpressions)
                    .withPotentiallyUnmappedExpression(unmappedConversionExpression);
        }

        /**
         * Check if all the original mapped types in the {@code UnionTypeEsField} are supported by the convert function.
         * If the field is partially unmapped and the function cannot consume {@code KEYWORD}, the unmapped branches are nullified
         * outside this function.
         */
        private static boolean areMappedTypesSupported(UnionTypeEsField unionTypeEsField, Set<DataType> supportedTypes) {
            return unionTypeEsField.getConversionExpressions()
                .stream()
                .allMatch(
                    e -> e instanceof AbstractConvertFunction convertFunction
                        && supportedTypes.contains(convertFunction.field().dataType().widenSmallNumeric())
                );
        }

        private static Expression typeSpecificConvert(ConvertFunction convert, Source source, DataType type, TypeConflictedField tcf) {
            FieldAttribute originalFieldAttr = (FieldAttribute) convert.field();
            FieldAttribute resolvedAttr = new FieldAttribute(
                source,
                originalFieldAttr.parentName(),
                originalFieldAttr.qualifier(),
                originalFieldAttr.name(),
                typedEsField(type, tcf),
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

    // visible for testing
    static String nonLoadablePunkWarning(String fieldName, String mappedTypeName) {
        return Strings.format(
            "Field [%s] of type [%s] is unmapped in some indices and has no implicit "
                + "conversion from KEYWORD, so it will not be loaded from _source; values will be null in those indices",
            fieldName,
            mappedTypeName
        );
    }

    /**
     * {@link ResolveUnionTypes} creates new, synthetic attributes for union types:
     * If there was no {@code AbstractConvertFunction} that resolved multi-type fields in the {@link ResolveUnionTypes} rule,
     * then there could still be some {@code FieldAttribute}s that contain unresolved {@link UnionTypeEsField}s.
     * These need to be converted back to actual {@code UnresolvedAttribute} in order for validation to generate appropriate failures.
     * <p>
     * Finally, if {@code client_ip} is present in 2 indices, once with type {@code ip} and once with type {@code keyword},
     * using {@code EVAL x = to_ip(client_ip)} will create a single attribute @{code $$client_ip$converted_to$ip}.
     * This should not spill into the query output, so we drop such attributes at the end.
     */
    private static class UnionTypesCleanup extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {
        public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {

            // We start by dropping synthetic attributes if the plan is resolved
            LogicalPlan cleanPlan = plan.resolved() ? planWithoutSyntheticAttributes(plan) : plan;

            if (context.unmappedResolution() == UnmappedResolution.LOAD && cleanPlan.resolved()) {
                // A single-type PUNK that survives to here has neither an implicit nor an explicit KEYWORD conversion (those turn it into a
                // UnionTypeEsField earlier), so it falls back to null where unmapped. Warn once for each such field whose value the user
                // can actually observe (it reaches the final output or is consumed by some, non-conversion expression).
                warnObservedNonLoadablePunks(cleanPlan, context);
            }

            // If not, we apply checkUnresolved to the field attributes of the original plan, resulting in unsupported attributes
            // This removes attributes such as converted types if they are aliased, but retains them otherwise, while also guaranteeing that
            // unsupported / unresolved fields can be explicitly retained
            return cleanPlan.transformUp(
                LogicalPlan.class,
                p -> p.transformExpressionsOnly(
                    FieldAttribute.class,
                    fa -> fa.field() instanceof PotentiallyUnmappedSingleTypeEsField punk
                        ? fallbackToMappedType(fa, punk)
                        : fa.flagTypeConflicts()
                )
            );
        }

        private static void warnObservedNonLoadablePunks(LogicalPlan plan, AnalyzerContext context) {
            AttributeSet.Builder observed = AttributeSet.builder();
            plan.output().forEach(observed::add);
            plan.forEachDown(p -> observed.addAll(p.references()));
            AttributeSet observedFields = observed.build();

            Set<NameId> warned = new HashSet<>();
            plan.forEachExpressionDown(FieldAttribute.class, fa -> {
                if (fa.field() instanceof PotentiallyUnmappedSingleTypeEsField punk && observedFields.contains(fa) && warned.add(fa.id())) {
                    DataType mappedType = punk.mappedField().getDataType();
                    context.deferredHeaderWarnings().add(nonLoadablePunkWarning(fa.name(), mappedType.typeName()));
                }
            });
        }

        private static LogicalPlan planWithoutSyntheticAttributes(LogicalPlan plan) {
            // Virtual columns (today: _file.* and the standard metadata names on external datasets)
            // are kept out of default output, the same way the implicit `*` expansion drops them via
            // excludeExternalMetadata. But once the user names one explicitly — KEEP _index,
            // KEEP _file.path — it must reach the result, even when later commands (SORT, LIMIT, ...)
            // sit above the KEEP and make the relation's output, not the projection, the plan's top
            // node. We therefore strip a virtual attribute only when no explicit KEEP named it.
            //
            // Provenance matters: a DROP also resolves to a Project that carries surviving virtual
            // columns forward via childOutput, but that is NOT the user keeping them — so we scan
            // only Keep nodes (resolveKeep emits a Keep; resolveDrop emits a plain Project). A
            // `KEEP *` runs its projections through excludeExternalMetadata, so its Keep node never
            // lists a virtual column either. This is why we key off the Keep node identity rather
            // than the namespace of the column name.
            Set<String> explicitlyKept = explicitlyKeptVirtualNames(plan);
            // External metadata is hidden from default output (and surfaced via KEEP) ONLY for the
            // EXTERNAL command. Its shim auto-injects the whole _file.* family because EXTERNAL has
            // no METADATA grammar to be selective, so KEEP is how the user picks what surfaces. On
            // the FROM <dataset> path the user names metadata explicitly in a METADATA clause, so it
            // must surface unconditionally — KEEP there is ordinary projection, not a metadata gate.
            // The two are distinguishable on the relation: a FROM <dataset> leaf carries a
            // datasetName; the EXTERNAL shim's leaf does not.
            boolean hasExternalCommandRelation = plan.anyMatch(p -> p instanceof ExternalRelation er && er.datasetName() == null);
            List<Attribute> output = plan.output();
            List<Attribute> newOutput = new ArrayList<>(output.size());
            for (Attribute attr : output) {
                // Do not let the synthetic union type field attributes end up in the final output.
                if (attr.synthetic() && attr != NO_FIELDS.getFirst()) {
                    continue;
                }
                // EXTERNAL command only: hide its shim-injected metadata from default output unless
                // KEEP'd. Strip by VirtualAttribute type OR by well-known _file.* name (a defense
                // layer: downstream rules — notably FORK's output re-derivation through
                // toReferenceAttributesPreservingIds — can drop the VirtualAttribute marker). On the
                // FROM path nothing is stripped: every metadata column there was explicitly named in
                // METADATA and must reach the output.
                if (hasExternalCommandRelation) {
                    boolean isVirtual = attr instanceof VirtualAttribute || FileMetadataColumns.NAMES.contains(attr.name());
                    if (isVirtual && explicitlyKept.contains(attr.name()) == false) {
                        continue;
                    }
                }
                newOutput.add(attr);
            }

            return newOutput.size() == output.size() ? plan : new Project(Source.EMPTY, plan, newOutput);
        }

        /**
         * Names of every {@link VirtualAttribute} that appears in the projections of some
         * {@link Keep} node — i.e. the virtual columns the user pulled in by name
         * (KEEP _index, KEEP _file.path). Used to decide which virtual columns survive into the
         * final output instead of being hidden as default-output noise.
         * <p>
         * Scanning {@link Keep} specifically (not every {@link Project}) is the provenance gate: a
         * DROP resolves to a plain {@link Project} that carries surviving virtual columns forward —
         * that must not count as "the user kept it". {@code resolveKeep} emits {@link Keep};
         * {@code resolveDrop} emits {@link Project}. A {@code KEEP *} expansion routes through
         * {@code excludeExternalMetadata}, so its {@link Keep} lists no {@link VirtualAttribute}.
         */
        private static Set<String> explicitlyKeptVirtualNames(LogicalPlan plan) {
            Set<String> names = new HashSet<>();
            plan.forEachDown(Keep.class, keep -> {
                for (NamedExpression projection : keep.projections()) {
                    // Pair with the strip: type-marker OR well-known _file.* name. KEEP _file.path on a
                    // projection whose VirtualAttribute marker was dropped downstream still counts as
                    // an explicit keep.
                    if (projection instanceof VirtualAttribute || FileMetadataColumns.NAMES.contains(projection.name())) {
                        names.add(projection.name());
                    }
                }
            });
            return names;
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
                    if (f.field() instanceof TypeConflictedField tcf && allDates(context, tcf)) {
                        Map<ResolveUnionTypes.TypeResolutionKey, Expression> typeResolutions = new HashMap<>();
                        var convert = new ToDateNanos(f.source(), f, context.configuration());
                        tcf.types().forEach(type -> typeResolutions(convert, type, f, tcf, typeResolutions));
                        // The allDates check filters out fields that are not mapped in all indices, which includes
                        // potentiallyUnmapped fields. This assertion guards against future changes breaking that invariant.
                        assert tcf.isPotentiallyUnmapped() == false
                            : "Unexpected potentially unmapped field [" + tcf.getName() + "] in DateMillisToNanosInEsRelation";
                        var resolvedField = ResolveUnionTypes.resolvedUnionTypeFields(f, tcf, typeResolutions, null, context);
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

        private static boolean allDates(AnalyzerContext context, TypeConflictedField tcf) {
            if (tcf.types().stream().allMatch(DataType::isDate) == false) {
                return false;
            }
            // If the field is potentially unmapped (i.e. not mapped in all indices), we treat it as a keyword (not all dates),
            // so that it can be resolved via the union types / UnionTypeEsField mechanism instead.
            if (context.unmappedResolution() == UnmappedResolution.LOAD && tcf.isPotentiallyUnmapped()) {
                return false;
            }
            return true;
        }
    }

    /**
     * When {@code SET unmapped_fields="load"}, this analyzer rule auto-casts any field in {@link EsRelation} nodes that meets all the
     * criteria below, by re-writing it as {@link UnionTypeEsField}.
     * <ol>
     *     <li>Field is a PUNK (partially unmapped non-KEYWORD)</li>
     *     <li>Field's type is consistent where mapped. It can't be mapped as two different non-KEYWORD types.</li>
     *     <li>There exists a converter function to cast {@code KEYWORD} to the mapped type</li>
     * </ol>
     * PUNKs that fail the last criterion (no implicit {@code KEYWORD} converter, e.g., {@code TEXT}) are left untouched here and resolved
     * later: {@code ResolveUnionTypes} loads the unmapped leg when an explicit cast is applied directly to the field, and otherwise
     * {@code UnionTypesCleanup} replaces them with their mapped type ({@code null} where unmapped).
     */
    private static class ResolveTwoLeggedPunksInEsRelation extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {
        @Override
        public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
            if (context.unmappedResolution() != UnmappedResolution.LOAD) {
                return plan;
            }

            return plan.transformUp(EsRelation.class, esRelation -> {
                if (esRelation.indexMode() == IndexMode.LOOKUP) {
                    return esRelation;
                }

                return esRelation.transformExpressionsOnly(FieldAttribute.class, fa -> {
                    // We're looking for partially unmapped fields with exactly one mapped type, i.e.: two-legged PUNKs
                    if (fa.field() instanceof PotentiallyUnmappedSingleTypeEsField punk) {
                        DataType mappedType = punk.mappedField().getDataType();

                        // DENSE_VECTOR has a KEYWORD converter, but it reads hexadecimal strings whereas an unmapped DENSE_VECTOR loads
                        // from _source as an array of numbers (#152184). Implicitly casting a partially unmapped DENSE_VECTOR from KEYWORD
                        // would therefore produce garbage, so we exclude it from auto-casting.
                        if (mappedType != DataType.DENSE_VECTOR == false) {
                            return fa;
                        }

                        var convertFactory = EsqlDataTypeConverter.converterFunctionFactory(mappedType);
                        ConvertFunction convert = convertFactory == null
                            ? null
                            : convertFactory.apply(fa.source(), fa, context.configuration());
                        // We can only load an unmapped field from _source as KEYWORD, so without a converter accepting KEYWORD input we
                        // can't auto-cast. Leave the PUNK in place: a cast applied directly to the field is resolved by ResolveUnionTypes
                        // (which loads the unmapped leg from _source), while every other use falls back to the mapped type in
                        // UnionTypesCleanup (null where unmapped). The PUNK reports its mapped type rather than UNSUPPORTED, so renames and
                        // groupings carry the real type.
                        if (convert == null || convert.supportedTypes().contains(KEYWORD) == false) {
                            return fa;
                        }

                        Map<ResolveUnionTypes.TypeResolutionKey, Expression> typeResolutions = new HashMap<>();
                        typeResolutions(convert, mappedType, fa, punk, typeResolutions);

                        Expression potentiallyUnmappedConversion = ResolveUnionTypes.typeSpecificConvert(
                            convert,
                            fa.source(),
                            KEYWORD,
                            punk
                        );

                        EsField resolvedField = ResolveUnionTypes.resolvedUnionTypeFields(
                            fa,
                            punk,
                            typeResolutions,
                            potentiallyUnmappedConversion,
                            context
                        );

                        return new FieldAttribute(
                            fa.source(),
                            fa.parentName(),
                            fa.qualifier(),
                            fa.name(),
                            resolvedField,
                            fa.nullable(),
                            fa.id(),
                            fa.synthetic()
                        );
                    }
                    return fa;
                });
            });
        }
    }

    /**
     * The effective data type of a branch/output attribute when aligning the branches of a {@link Fork} / {@link UnionAll}.
     */
    private static DataType alignmentDataType(Attribute attr) {
        if (attr instanceof FieldAttribute fa && fa.field() instanceof TypeConflictedField tcf && tcf.isSingleTypePotentiallyUnmapped()) {
            return tcf.singleMappedTypeWidened();
        }
        return attr.dataType();
    }

    private static void typeResolutions(
        ConvertFunction convert,
        DataType type,
        FieldAttribute fa,
        TypeConflictedField tcf,
        Map<ResolveUnionTypes.TypeResolutionKey, Expression> typeResolutions
    ) {
        ResolveUnionTypes.TypeResolutionKey key = new ResolveUnionTypes.TypeResolutionKey(fa.name(), type);
        var concreteConvert = ResolveUnionTypes.typeSpecificConvert(convert, fa.source(), type, tcf);
        typeResolutions.put(key, concreteConvert);
    }

    private static FieldAttribute fallbackToMappedType(FieldAttribute fieldAttribute, PotentiallyUnmappedSingleTypeEsField punk) {
        return fieldAttribute.withField(punk.mappedField().withWidenedSmallNumeric());
    }

    /**
     * Take TypeConflictedFields in specific aggregations (min, max, sum, count, and avg) and if all original data types
     * are aggregate metric double + any combination of numerics, implicitly cast them to the same type: aggregate metric
     * double for count, and double for min, max, and sum. Avg gets replaced with its surrogate (Div(Sum, Count))
     */
    private static class ImplicitCastAggregateMetricDoubles extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {

        private boolean isTimeSeries = false;

        @Override
        public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
            Holder<IndexMode> indexMode = new Holder<>(IndexMode.STANDARD);
            plan.forEachUp(EsRelation.class, esRelation -> { indexMode.set(esRelation.indexMode()); });
            isTimeSeries = indexMode.get().isTsdb();
            return plan.transformUp(LogicalPlan.class, p -> doRule(p, context));
        }

        private LogicalPlan doRule(LogicalPlan plan, AnalyzerContext context) {
            if (plan instanceof EsRelation || plan instanceof Project || plan.childrenResolved() == false) {
                return plan;
            }
            Map<String, FieldAttribute> unionFields = new HashMap<>();
            Holder<Boolean> aborted = new Holder<>(Boolean.FALSE);
            var newPlan = plan.transformExpressionsOnly(AggregateFunction.class, aggFunc -> {
                Expression child;
                if (aggFunc.field() instanceof ToAggregateMetricDouble toAMD) {
                    child = tryToTransformFunction(aggFunc, toAMD.field(), aborted, unionFields, context);
                } else {
                    child = tryToTransformFunction(aggFunc, aggFunc.field(), aborted, unionFields, context);
                }
                return child;
            }).transformExpressionsOnly(EsqlBinaryComparison.class, comparison -> {
                Expression left = comparison.left();
                Expression right = comparison.right();
                Holder<Boolean> modified = new Holder<>(Boolean.FALSE);
                left = tryToTransformBinaryComparison(comparison, left, modified, unionFields, context);
                right = tryToTransformBinaryComparison(comparison, right, modified, unionFields, context);
                if (modified.get() == false) {
                    return comparison;
                }
                return comparison.replaceChildren(List.of(left, right));
            });
            if (unionFields.isEmpty() || aborted.get()) {
                return plan;
            }
            return ResolveUnionTypes.addGeneratedFieldsToEsRelations(newPlan, unionFields.values().stream().toList());
        }

        private Expression tryToTransformBinaryComparison(
            EsqlBinaryComparison comparison,
            Expression original,
            Holder<Boolean> modified,
            Map<String, FieldAttribute> unionFields,
            AnalyzerContext context
        ) {
            if (original instanceof FieldAttribute fa
                && fa.field() instanceof TypeConflictedField tcf
                && tcf instanceof PotentiallyUnmappedSingleTypeEsField == false
                && canBeCast(tcf)) {
                Map<String, Expression> typeConverters = new HashMap<>();
                for (DataType type : tcf.types()) {
                    ConvertFunction convert = type == AGGREGATE_METRIC_DOUBLE
                        ? FromAggregateMetricDouble.withMetric(comparison.source(), fa, AggregateMetricDoubleBlockBuilder.Metric.DEFAULT)
                        : new ToDouble(fa.source(), fa);
                    Expression expression = ResolveUnionTypes.typeSpecificConvert(convert, fa.source(), type, tcf);
                    typeConverters.put(type.typeName(), expression);
                }
                var newField = unionFields.computeIfAbsent(
                    Attribute.rawTemporaryName(fa.name(), comparison.functionName(), comparison.sourceText()),
                    newName -> new FieldAttribute(
                        fa.source(),
                        fa.parentName(),
                        fa.qualifier(),
                        newName,
                        ResolveUnionTypes.buildUnionTypeField(tcf, typeConverters, null, context),
                        fa.nullable(),
                        null,
                        true
                    )
                );
                modified.set(true);
                return newField;
            }
            return original;
        }

        private static boolean canBeCast(TypeConflictedField tcf) {
            return tcf.types().contains(AGGREGATE_METRIC_DOUBLE)
                && tcf.types().stream().allMatch(f -> f == AGGREGATE_METRIC_DOUBLE || f.isNumeric());
        }

        private Expression tryToTransformFunction(
            AggregateFunction aggFunc,
            Expression field,
            Holder<Boolean> aborted,
            Map<String, FieldAttribute> unionFields,
            AnalyzerContext context
        ) {
            if (field instanceof FieldAttribute fa && fa.field() instanceof TypeConflictedField tcf) {
                // A bare PUNK is a single mapped type, not a multi-type conflict: leave it so UnionTypesCleanup replaces it with the plain
                // mapped field and native aggregation applies. (Count's AMD surrogate COALESCEs an empty group to 0, unlike the bare
                // Sum(metric.count) this rule would build, which yields null.)
                if (tcf instanceof PotentiallyUnmappedSingleTypeEsField) {
                    return aggFunc;
                }
                if (canBeCast(tcf) == false) {
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
                if (aggFunc instanceof AvgOverTime avgOT) {
                    return new Div(
                        aggFunc.source(),
                        new SumOverTime(aggFunc.source(), field, aggFunc.filter(), aggFunc.window(), avgOT.timestamp()),
                        new CountOverTime(aggFunc.source(), field, aggFunc.filter(), aggFunc.window(), avgOT.timestamp())
                    );
                }

                Map<String, Expression> typeConverters = typeConverters(aggFunc, fa, tcf);
                var newField = unionFields.computeIfAbsent(
                    Attribute.rawTemporaryName(fa.name(), aggFunc.functionName(), aggFunc.sourceText()),
                    newName -> new FieldAttribute(
                        fa.source(),
                        fa.parentName(),
                        fa.qualifier(),
                        newName,
                        ResolveUnionTypes.buildUnionTypeField(tcf, typeConverters, null, context),
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
                if (aggFunc instanceof CountOverTime cot) {
                    return new SumOverTime(aggFunc.source(), children.getFirst(), aggFunc.filter(), aggFunc.window(), cot.timestamp());
                }
                return aggFunc.replaceChildren(children);
            }
            return aggFunc;
        }

        private Map<String, Expression> typeConverters(AggregateFunction aggFunc, FieldAttribute fa, TypeConflictedField tcf) {
            var metric = getMetric(aggFunc, isTimeSeries);
            Map<String, Expression> typeConverter = new HashMap<>();
            for (DataType type : tcf.types()) {
                final ConvertFunction convert;
                if (type == AGGREGATE_METRIC_DOUBLE) {
                    convert = FromAggregateMetricDouble.withMetric(aggFunc.source(), fa, metric);
                } else if (metric == AggregateMetricDoubleBlockBuilder.Metric.COUNT) {
                    // we have a numeric on hand so calculate MvCount on it so we can plug it into Sum(metric.count)
                    var tempConvert = new MvCount(aggFunc.source(), fa);
                    typeConverter.put(type.typeName(), countConvert(tempConvert, fa.source(), type, tcf));
                    continue;
                } else {
                    convert = new ToDouble(fa.source(), fa);
                }
                Expression expression = ResolveUnionTypes.typeSpecificConvert(convert, fa.source(), type, tcf);
                typeConverter.put(type.typeName(), expression);
            }
            return typeConverter;
        }

        private Expression countConvert(UnaryScalarFunction convert, Source source, DataType type, TypeConflictedField tcf) {
            FieldAttribute originalFieldAttr = (FieldAttribute) convert.field();
            FieldAttribute resolvedAttr = new FieldAttribute(
                source,
                originalFieldAttr.parentName(),
                originalFieldAttr.qualifier(),
                originalFieldAttr.name(),
                typedEsField(type, tcf),
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
     * Create an EsField from a TypeConflictedField instance, casted to a specific type, but ignoring its sub-fields which are irrelevant
     * here and are resolved independently as their own attributes. Carrying them would serialize the field's subfield properties to
     * data nodes, which fails when a subfield is itself an un-transportable conflict field (e.g. CompactInvalidMappedField from a
     * multi-index partially-unmapped multi-field).
     */
    private static EsField typedEsField(DataType type, TypeConflictedField tcf) {
        return new EsField(tcf.getName(), type, Map.of(), tcf.isAggregatable(), tcf.getTimeSeriesFieldType());
    }

    /**
     * Takes aggregation functions that don't natively support AggregateMetricDouble (i.e. aggregations other than
     * min, max, sum, count, avg) that receive an AggregateMetricDouble as input, and inserts a call to
     * FROM_AGGREGATE_METRIC_DOUBLE to fetch the DEFAULT metric.
     */
    private static class InsertFromAggregateMetricDouble extends Rule<LogicalPlan, LogicalPlan> {
        @Override
        public LogicalPlan apply(LogicalPlan plan) {
            return plan.transformUp(Aggregate.class, p -> p.childrenResolved() ? doRule(p) : p)
                .transformExpressionsUp(EsqlBinaryComparison.class, this::doRule);
        }

        private Expression doRule(EsqlBinaryComparison comparison) {
            Expression left = comparison.left();
            Expression right = comparison.right();
            boolean modified = false;
            if (left.resolved() == false || right.resolved() == false) {
                return comparison;
            }
            if (left.dataType() == AGGREGATE_METRIC_DOUBLE) {
                left = FromAggregateMetricDouble.withMetric(left.source(), left, AggregateMetricDoubleBlockBuilder.Metric.DEFAULT);
                modified = true;
            }
            if (right.dataType() == AGGREGATE_METRIC_DOUBLE) {
                right = FromAggregateMetricDouble.withMetric(right.source(), right, AggregateMetricDoubleBlockBuilder.Metric.DEFAULT);
                modified = true;
            }
            if (modified == false) {
                return comparison;
            }
            return comparison.replaceChildren(List.of(left, right));
        }

        private LogicalPlan doRule(Aggregate plan) {
            Holder<IndexMode> indexMode = new Holder<>(IndexMode.STANDARD);
            plan.forEachUp(EsRelation.class, esRelation -> { indexMode.set(esRelation.indexMode()); });
            final boolean isTimeSeries = indexMode.get().isTsdb();
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
                    ? maybePushDownConvertFunctions(unionAll, plan, convertFunctionsToAttributes, context)
                    : unionAll
            );

            // Carry over the synthetic convert-function attributes added to UnionAll output through Project above it.
            if (convertFunctionsToAttributes.isEmpty() == false) {
                planWithConvertFunctionsPushedDown = carryOverSyntheticAttributesThroughProjects(planWithConvertFunctionsPushedDown);
            }

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
            Map<AbstractConvertFunction, Attribute> convertFunctionsToAttributes,
            AnalyzerContext context
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
                List<FieldAttribute> resolvedUnionFields = new ArrayList<>();
                List<Attribute.IdIgnoringWrapper> branchUnionFieldAttributes = new ArrayList<>();
                List<Attribute> newChildOutput = new ArrayList<>(childOutput.size());

                for (Attribute oldAttr : childOutput) {
                    newChildOutput.add(oldAttr);
                    Set<AbstractConvertFunction> converts = oldOutputToConvertFunctions.get(oldAttr.name());
                    if (converts != null) {
                        for (AbstractConvertFunction convert : converts) {
                            Expression pushedDownConvert = convert.replaceChildren(Collections.singletonList(oldAttr));
                            // If this branch's input to the convert is itself a multi-typed field, ResolveUnionTypes would resolve the
                            // pushed-down convert into a synthetic field named exactly like the alias we would create here. Having both
                            // in the same branch scope makes the alignment Project's by-name reference ambiguous and it never resolves.
                            // Resolve it once, up front, and expose that field directly so there is no double conversion / name clash.
                            Expression resolved = ResolveUnionTypes.resolveConvertFunction(
                                (ConvertFunction) pushedDownConvert,
                                branchUnionFieldAttributes,
                                context
                            );
                            if (resolved instanceof FieldAttribute resolvedField && resolvedField.synthetic()) {
                                newChildOutput.add(resolvedField);
                                resolvedUnionFields.add(resolvedField);
                                newOutputToConvertFunctions.putIfAbsent(resolvedField.name(), convert);
                            } else {
                                String newAliasName = Attribute.rawTemporaryName(
                                    oldAttr.name(),
                                    "converted_to",
                                    convert.dataType().typeName()
                                );
                                Alias newAlias = new Alias(
                                    oldAttr.source(),
                                    newAliasName, // oldAttrName$$converted_to$$targetType
                                    pushedDownConvert,
                                    null, // generate a new id
                                    true // this'll be used to Project the synthetic attributes out when finishing analysis
                                );
                                newAliases.add(newAlias);
                                newChildOutput.add(newAlias.toAttribute());
                                newOutputToConvertFunctions.putIfAbsent(newAliasName, convert);
                            }
                            outputChanged = true;
                        }
                    }
                }
                newChildren.add(maybePushDownConvertFunctionsToChild(child, newAliases, resolvedUnionFields, newChildOutput));
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
         * Push down the conversion functions into the child plan. Single-type inputs are converted by adding an Eval with the new
         * aliases on top of the child plan; multi-typed inputs were already resolved to synthetic union-type fields (see
         * {@link #maybePushDownConvertFunctions}) which are instead injected into the branch's {@link EsRelation}.
         */
        private static LogicalPlan maybePushDownConvertFunctionsToChild(
            LogicalPlan child,
            List<Alias> aliases,
            List<FieldAttribute> resolvedUnionFields,
            List<Attribute> output
        ) {
            // Fork/UnionAll adds a projection on top of each child plan during resolveFork, check this pattern before pushing down
            // If the pattern doesn't match, something unexpected happened, just return the child as is
            if ((aliases.isEmpty() == false || resolvedUnionFields.isEmpty() == false) && child instanceof Project project) {
                LogicalPlan childOfProject = project.child();
                if (aliases.isEmpty() == false) {
                    childOfProject = new Eval(childOfProject.source(), childOfProject, aliases);
                }
                if (resolvedUnionFields.isEmpty() == false) {
                    childOfProject = ResolveUnionTypes.addGeneratedFieldsToEsRelations(childOfProject, resolvedUnionFields);
                }
                return new Project(project.source(), childOfProject, output);
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
            return unionAll.replaceSubPlansAndOutput(newChildren, newOutput);
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

            // Collect UnsupportedAttributes by column index so that rebuildUnionAllOutput
            // can use them for the UnionAll output, preserving original_types metadata.
            // Also doubles as the common type override: any column in this map has common type UNSUPPORTED.
            Map<Integer, UnsupportedAttribute> unsupportedAttributes = new HashMap<>();

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
                        unsupportedAttributes,
                        configuration
                    );
                    newChildOutput.add(resolved);
                    if (resolved != oldOutput) {
                        outputChanged = true;
                    }
                }
                // create a new eval for the casting expressions, and push it down under the projection
                newChildren.add(maybePushDownConvertFunctionsToChild(child, newAliases, List.of(), newChildOutput));
            }

            // Update common types: any column with unsupported attributes gets UNSUPPORTED
            unsupportedAttributes.keySet().forEach(i -> commonTypes.set(i, UNSUPPORTED));

            return outputChanged
                ? rebuildUnionAllOutput(unionAll, newChildren, commonTypes, updatedUnionAllOutput, unsupportedAttributes)
                : unionAll;
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
                DataType type = alignmentDataType(outputs.get(0).get(i));
                for (List<Attribute> out : outputs) {
                    type = commonType(type, alignmentDataType(out.get(i)));
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
         * an UnsupportedAttribute if the attribute is referenced in the parent plans (returned directly, causes verification error),
         * a Null alias with keyword type if the attribute is not referenced (returned for child output),
         * with UnsupportedAttribute stored in the side-channel map for the UnionAll output in both cases.
         */
        private static Attribute resolveAttribute(
            Attribute oldAttr,
            DataType targetType,
            int columnIndex,
            List<List<Attribute>> outputs,
            UnionAll unionAll,
            Map<Attribute, List<LogicalPlan>> outputToPlans,
            List<Alias> newAliases,
            Map<Integer, UnsupportedAttribute> unsupportedAttributes,
            Configuration configuration
        ) {
            if (targetType == null) {
                return createUnsupportedOrNull(oldAttr, columnIndex, outputs, unionAll, outputToPlans, newAliases, unsupportedAttributes);
            }

            if (alignmentDataType(oldAttr) != oldAttr.dataType()) {
                return oldAttr;
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
            Map<Integer, UnsupportedAttribute> unsupportedAttributes
        ) {
            Attribute unionAttr = unionAll.output().get(columnIndex);
            // Create the UnsupportedAttribute once — used in both branches.
            // Its presence in unsupportedAttributes also signals that commonType should be UNSUPPORTED.
            UnsupportedAttribute unsupported = unsupportedAttributes.computeIfAbsent(columnIndex, k -> {
                List<String> dataTypes = collectIncompatibleTypes(k, outputs);
                return new UnsupportedAttribute(
                    oldAttr.source(),
                    oldAttr.name(),
                    new UnsupportedEsField(oldAttr.name(), dataTypes),
                    "Column [" + oldAttr.name() + "] has conflicting data types in subqueries: " + dataTypes,
                    oldAttr.id()
                );
            });

            if (outputToPlans.containsKey(unionAttr)) {
                // Referenced by downstream plans — return UnsupportedAttribute directly (causes verification error)
                newAliases.add(new Alias(oldAttr.source(), oldAttr.name(), unsupported));
                return unsupported;
            } else {
                // Not referenced by downstream plans:
                // Return a null KEYWORD alias for the child output (so child plans work correctly).
                // The UnsupportedAttribute is already stored in the side-channel map above
                // for rebuildUnionAllOutput to use in the UnionAll output, preserving original_types metadata.
                Alias nullAlias = new Alias(oldAttr.source(), oldAttr.name(), new Literal(oldAttr.source(), null, KEYWORD));
                newAliases.add(nullAlias);
                return nullAlias.toAttribute();
            }
        }

        private static List<String> collectIncompatibleTypes(int columnIndex, List<List<Attribute>> outputs) {
            List<String> dataTypes = new ArrayList<>();
            for (List<Attribute> out : outputs) {
                Attribute attr = out.get(columnIndex);
                if (attr instanceof FieldAttribute fa && fa.field() instanceof TypeConflictedField tcf) {
                    dataTypes.addAll(tcf.types().stream().map(DataType::typeName).toList());
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
            List<Attribute> updatedUnionAllOutput,
            Map<Integer, UnsupportedAttribute> unsupportedAttributes
        ) {
            // Rebuild the newUnionAll's output to ensure the correct attributes are used
            List<Attribute> oldOutput = unionAll.output();
            List<Attribute> newOutput = new ArrayList<>(oldOutput.size());

            for (int i = 0; i < oldOutput.size(); i++) {
                Attribute oldAttr = oldOutput.get(i);
                DataType commonType = commonTypes.get(i);

                if (oldAttr.dataType() != commonType) {
                    Attribute newAttr;
                    UnsupportedAttribute ua = unsupportedAttributes.get(i);
                    if (commonType == UNSUPPORTED && ua != null) {
                        // Use the UnsupportedAttribute directly so that original_types metadata
                        // is preserved and flows through to the response output.
                        // Keep the id unchanged, otherwise the downstream operators won't recognize the attribute.
                        newAttr = new UnsupportedAttribute(
                            oldAttr.source(),
                            ua.qualifier(),
                            oldAttr.name(),
                            ua.field(),
                            ua.hasCustomMessage() ? ua.unresolvedMessage() : null,
                            oldAttr.id()
                        );
                    } else {
                        // keep the id unchanged, otherwise the downstream operators won't recognize the attribute
                        newAttr = new ReferenceAttribute(
                            oldAttr.source(),
                            null,
                            oldAttr.name(),
                            commonType,
                            oldAttr.nullable(),
                            oldAttr.id(),
                            oldAttr.synthetic()
                        );
                    }
                    newOutput.add(newAttr);
                    updatedUnionAllOutput.add(newAttr);
                } else {
                    newOutput.add(oldAttr);
                }
            }
            return unionAll.replaceSubPlansAndOutput(newChildren, newOutput);
        }

        /**
         * Update the attributes referencing the updated UnionAll output.
         * <p>
         * Beyond updating direct attribute references (e.g. a {@code KEEP} projection that names a fork-output attribute),
         * this also cascades the type change through {@link Alias} nodes whose child is a direct attribute reference.
         * <p>
         * Before the expression walk, scan the plan for {@link Alias} nodes whose immediate child is an attribute already in the update
         * map and add a {@code {alias.id → alias.withNewType}} entry. Because the traversal is bottom-up, chained renames such as
         * {@code x AS y, y AS z} are picked up in order. We register the alias output whenever it is resolved (i.e. without comparing the
         * alias' current child type against the map entry), because the alias may have been re-resolved with the updated child type
         * (e.g. inside a {@code ResolvingProject}) while other places in the plan (e.g. an outer {@code OrderBy}) still hold a cached
         * attribute reference, produced by {@link Alias#toAttribute()}, with the stale (pre-update) type. The subsequent
         * {@code transformExpressionsUp} then repairs every consumer of the alias output in one pass.
         */
        private static LogicalPlan updateAttributesReferencingUpdatedUnionAllOutput(
            LogicalPlan plan,
            List<Attribute> updatedUnionAllOutput
        ) {
            Map<NameId, Attribute> idToUpdatedAttr = new HashMap<>();
            updatedUnionAllOutput.forEach(attr -> idToUpdatedAttr.put(attr.id(), attr));

            // Cascade: collect Alias nodes above the UnionAll whose child directly references a changed attribute.
            plan.forEachExpressionUp(Alias.class, alias -> {
                if (alias.child() instanceof Attribute childAttr) {
                    Attribute updatedChild = idToUpdatedAttr.get(childAttr.id());
                    if (updatedChild != null) {
                        Attribute aliasOutput = alias.toAttribute();
                        // An unresolved alias (e.g. a RENAME over a cross-branch-conflicting UnionAll column) yields an
                        // UnresolvedAttribute whose dataType() throws; skip it — there is no type to cascade and the verifier rejects it.
                        if (aliasOutput.resolved()) {
                            idToUpdatedAttr.put(aliasOutput.id(), aliasOutput.withDataType(updatedChild.dataType()));
                        }
                    }
                }
            });

            return plan.transformExpressionsUp(Attribute.class, expr -> {
                Attribute updated = idToUpdatedAttr.get(expr.id());
                return (updated != null && expr.resolved() && expr.dataType() != updated.dataType()) ? updated : expr;
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
            // Delegate to UnionAll#pruneEmptyBranches — ViewUnionAll overrides it to preserve
            // its named-subqueries map so this rule works correctly when an EMPTY_SUBQUERY
            // branch lives next to a CPS shadow inside a ViewUnionAll.
            Map<IndexPattern, IndexResolution> indexResolutions = context.indexResolution();
            return unionAll.pruneEmptyBranches(child -> resolvesToEmptySubquery(child, indexResolutions));
        }

        private static boolean resolvesToEmptySubquery(LogicalPlan branch, Map<IndexPattern, IndexResolution> indexResolutions) {
            Holder<Boolean> isEmpty = new Holder<>(Boolean.FALSE);
            branch.forEachUp(UnresolvedRelation.class, ur -> {
                IndexResolution resolution = indexResolutions.get(ur.indexPattern());
                if (resolution == IndexResolution.EMPTY_SUBQUERY) {
                    isEmpty.set(Boolean.TRUE);
                }
            });
            return isEmpty.get();
        }
    }

    /**
     * Carry over synthetic attributes that are present in a {@link Project}'s input set but missing from its
     * projections, by appending them to the projection list. Used by the union-types resolution rules to
     * propagate newly introduced synthetic {@code $$<field>$converted_to$<type>} attributes (from either
     * multi-typed {@link EsRelation} fields or {@link UnionAll} branches) through intermediate
     * {@link Project} nodes that were resolved before the synthetic existed (typically those produced by
     * {@code RENAME}, {@code KEEP}, or {@code DROP}). Without this, downstream references inserted above
     * the {@link Project} would have no binding and the optimizer's plan consistency check would later
     * fail with missing references.
     *
     * <p>Used by both {@code ResolveUnionTypes} (for multi-typed EsRelation fields) and
     * {@code ResolveUnionTypesInUnionAll} (for type conflicts across {@link UnionAll} branches).
     */
    private static LogicalPlan carryOverSyntheticAttributesThroughProjects(LogicalPlan plan) {
        return plan.transformUp(Project.class, p -> {
            // Skip Projects whose projections are not yet resolved (e.g. an unexpanded KEEP wildcard sitting above a still-unresolved
            // union-typed field reference). Their output cannot be computed yet — calling p.output() would throw UnresolvedException.
            // Such Projects are revisited in a later analyzer iteration once they resolve.
            if (p.expressionsResolved() == false) {
                return p;
            }
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
}
