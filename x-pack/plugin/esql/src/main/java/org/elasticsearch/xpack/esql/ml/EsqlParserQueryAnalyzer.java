/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ml;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.QuerySetting;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryAnalyzer;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryStructure;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryStructure.SourceKind;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.PipelineStep;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.PipelineStep.Kind;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucket;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * {@link EsqlQueryAnalyzer} for ES|QL datafeeds in the ML plugin, backed by {@link EsqlParser}. Reports the parsed plan before
 * analysis, so field and index names are as written and nothing is resolved against mappings.
 */
public class EsqlParserQueryAnalyzer implements EsqlQueryAnalyzer {

    private final EsqlParser parser = new EsqlParser(new EsqlConfig(new EsqlFunctionRegistry()));

    @Override
    public EsqlQueryStructure analyze(String query) {
        EsqlStatement statement = parser.parse(
            query,
            new QueryParams(),
            new InferenceSettings(Settings.EMPTY),
            QueryPragmas.EMPTY,
            EsqlFlags.DEFAULTS
        );
        LogicalPlan plan = statement.plan();
        LogicalPlan source = leadingSource(plan);
        // FORK copies the input relation into every branch, so the same pattern can appear more than once.
        Set<String> sourcePatterns = new LinkedHashSet<>();
        Set<String> lookupIndices = new LinkedHashSet<>();
        plan.forEachDown(UnresolvedRelation.class, relation -> {
            Set<String> target = relation.indexMode() == IndexMode.LOOKUP ? lookupIndices : sourcePatterns;
            // One relation holds the comma-joined patterns of its FROM command.
            target.addAll(List.of(Strings.splitStringByCommaToArray(relation.indexPattern().indexPattern())));
        });
        return new EsqlQueryStructure(
            settings(statement),
            settingsText(query, statement),
            sourceCommand(source),
            sourceKind(source),
            List.copyOf(sourcePatterns),
            List.copyOf(lookupIndices),
            outerPipeline(plan)
        );
    }

    @Override
    public boolean isDateType(String outputType) {
        return DataType.fromEs(outputType).isDate();
    }

    private static Map<String, String> settings(EsqlStatement statement) {
        Map<String, String> settings = new LinkedHashMap<>();
        for (QuerySetting setting : statement.settings()) {
            Expression value = setting.value();
            settings.put(setting.name(), value instanceof Literal literal ? BytesRefs.toString(literal.value()) : value.sourceText());
        }
        return settings;
    }

    private static String settingsText(String query, EsqlStatement statement) {
        if (statement.settings().isEmpty()) {
            return "";
        }
        return query.substring(0, endOffset(query, statement.settings().getLast().source()));
    }

    /** Absolute offset just after {@code source} in {@code query}. */
    private static int endOffset(String query, Source source) {
        int lineStart = 0;
        for (int line = 1; line < source.source().getLineNumber(); line++) {
            lineStart = query.indexOf('\n', lineStart) + 1;
        }
        // The parser counts columns in code points, not UTF-16 chars.
        return query.offsetByCodePoints(lineStart, source.source().getColumnNumber() - 1) + source.text().length();
    }

    /** The relation (or the FROM with subqueries) the top-level pipeline reads, or the leaf when there is none. */
    private static LogicalPlan leadingSource(LogicalPlan plan) {
        LogicalPlan node = plan;
        while (true) {
            if (node instanceof UnresolvedRelation || node instanceof UnionAll) {
                return node;
            } else if (node instanceof UnaryPlan unary) {
                node = unary.child();
            } else if (node instanceof Join join) {
                node = join.left();
            } else if (node instanceof MergePlan merge && merge.children().isEmpty() == false) {
                // FORK branches all start from the same input pipeline.
                node = merge.children().getFirst();
            } else {
                return node;
            }
        }
    }

    private static String sourceCommand(LogicalPlan source) {
        if (source instanceof UnresolvedRelation) {
            return source.source().text();
        } else if (source instanceof UnionAll) {
            // The UnionAll source starts after the FROM keyword and ends before METADATA.
            return "FROM " + source.source().text();
        }
        return "";
    }

    private static SourceKind sourceKind(LogicalPlan source) {
        if (source instanceof UnresolvedRelation relation) {
            return relation.indexMode() == IndexMode.TIME_SERIES ? SourceKind.TS : SourceKind.FROM;
        }
        return source instanceof UnionAll ? SourceKind.FROM : SourceKind.OTHER;
    }

    private static List<PipelineStep> outerPipeline(LogicalPlan plan) {
        List<PipelineStep> steps = new ArrayList<>();
        LogicalPlan node = plan;
        while (true) {
            if (node instanceof UnionAll) {
                // FROM with subqueries is the source command, not a pipeline step.
                return steps;
            } else if (node instanceof MergePlan) {
                steps.add(new PipelineStep(Kind.MERGE, referencedFields(node)));
                return steps;
            } else if (node instanceof InlineStats inlineStats) {
                // INLINE STATS wraps an Aggregate that is not a STATS command of the pipeline.
                steps.add(new PipelineStep(Kind.OTHER, referencedFields(inlineStats.child())));
                node = inlineStats.child() instanceof Aggregate aggregate ? aggregate.child() : inlineStats.child();
            } else if (node instanceof Join join) {
                steps.add(new PipelineStep(Kind.OTHER, referencedFields(join)));
                node = join.left();
            } else if (node instanceof UnaryPlan unary) {
                steps.add(step(unary));
                node = unary.child();
            } else {
                return steps;
            }
        }
    }

    private static PipelineStep step(UnaryPlan node) {
        Set<String> fields = referencedFields(node);
        return switch (node) {
            case Limit ignored -> new PipelineStep(Kind.LIMIT, fields);
            case Aggregate aggregate -> new PipelineStep(Kind.STATS, fields, timeBucket(aggregate));
            case Filter ignored -> new PipelineStep(Kind.WHERE, fields);
            case OrderBy ignored -> new PipelineStep(Kind.SORT, fields);
            default -> new PipelineStep(Kind.OTHER, fields);
        };
    }

    private static Set<String> referencedFields(LogicalPlan node) {
        Set<String> fields = new LinkedHashSet<>();
        node.forEachExpression(UnresolvedAttribute.class, attribute -> fields.add(attribute.name()));
        return fields;
    }

    /** The fixed-span time bucket among the groupings, or {@code null} if there is none or more than one. */
    @Nullable
    private static TimeBucket timeBucket(Aggregate aggregate) {
        List<TimeBucket> buckets = new ArrayList<>();
        for (Expression grouping : aggregate.groupings()) {
            grouping.forEachDown(UnresolvedFunction.class, function -> {
                TimeBucket bucket = timeBucket(function);
                if (bucket != null) {
                    buckets.add(bucket);
                }
            });
        }
        return buckets.size() == 1 ? buckets.getFirst() : null;
    }

    @Nullable
    private static TimeBucket timeBucket(UnresolvedFunction function) {
        List<Expression> args = function.arguments();
        return switch (function.name().toLowerCase(Locale.ROOT)) {
            case "bucket" -> args.size() >= 2 ? timeBucket(args.get(0), args.get(1)) : null;
            case "date_trunc" -> args.size() == 2 ? timeBucket(args.get(1), args.get(0)) : null;
            case "tbucket" -> args.size() == 1 ? timeBucket(null, args.get(0)) : null;
            default -> null;
        };
    }

    @Nullable
    private static TimeBucket timeBucket(@Nullable Expression field, Expression span) {
        if (field != null && field instanceof UnresolvedAttribute == false) {
            return null;
        }
        TimeValue spanValue = span(span);
        if (spanValue == null) {
            return null;
        }
        return new TimeBucket(field == null ? null : ((UnresolvedAttribute) field).name(), spanValue);
    }

    /** A fixed span from a time-duration literal ({@code 1 hour}) or a string ({@code "1h"}); calendar periods are not fixed. */
    @Nullable
    private static TimeValue span(Expression span) {
        if (span instanceof Literal == false) {
            return null;
        }
        Literal literal = (Literal) span;
        if (literal.dataType() == DataType.TIME_DURATION && literal.value() instanceof Duration duration) {
            return TimeValue.timeValueMillis(duration.toMillis());
        }
        if (literal.dataType() == DataType.KEYWORD && literal.value() != null) {
            // Parse as ES|QL does: "1d" is a calendar period there, not 24 hours.
            try {
                return EsqlDataTypeConverter.maybeParseTemporalAmount(BytesRefs.toString(literal.value())) instanceof Duration duration
                    ? TimeValue.timeValueMillis(duration.toMillis())
                    : null;
            } catch (ParsingException | InvalidArgumentException | ArithmeticException e) {
                return null;
            }
        }
        return null;
    }
}
