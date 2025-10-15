/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.antlr.v4.runtime.tree.ParseTree;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.promql.subquery.Subquery;
import org.elasticsearch.xpack.esql.expression.promql.types.PromqlDataTypes;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.WithinSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Evaluation;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.InstantSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatchers;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.expression.promql.function.FunctionType.ACROSS_SERIES_AGGREGATION;
import static org.elasticsearch.xpack.esql.expression.promql.function.FunctionType.WITHIN_SERIES_AGGREGATION;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.source;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.typedParsing;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.visitList;
import static org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher.NAME;

public class LogicalPlanBuilder extends ExpressionBuilder {

    LogicalPlanBuilder() {
        this(null, null);
    }

    LogicalPlanBuilder(Instant start, Instant stop) {
        super(start, stop);
    }

    protected LogicalPlan plan(ParseTree ctx) {
        return typedParsing(this, ctx, LogicalPlan.class);
    }

    @Override
    public LogicalPlan visitSingleStatement(PromqlBaseParser.SingleStatementContext ctx) {
        return plan(ctx.expression());
    }

    @Override
    public LogicalPlan visitSelector(PromqlBaseParser.SelectorContext ctx) {
        Source source = source(ctx);
        PromqlBaseParser.SeriesMatcherContext seriesMatcher = ctx.seriesMatcher();
        String id = visitIdentifier(seriesMatcher.identifier());
        List<LabelMatcher> labels = new ArrayList<>();
        Expression series = null;
        List<Expression> labelExpressions = new ArrayList<>();

        if (id != null) {
            labels.add(new LabelMatcher(NAME, id, LabelMatcher.Matcher.EQ));
            series = new UnresolvedAttribute(source(seriesMatcher.identifier()), id);
        }

        PromqlBaseParser.LabelsContext labelsCtx = seriesMatcher.labels();
        if (labelsCtx != null) {
            // if no name is specified, check for non-empty matchers
            boolean nonEmptyMatcher = id != null;
            for (PromqlBaseParser.LabelContext labelCtx : labelsCtx.label()) {
                if (labelCtx.kind == null) {
                    throw new ParsingException(source(labelCtx), "No label matcher specified");
                }
                String kind = labelCtx.kind.getText();
                LabelMatcher.Matcher matcher = LabelMatcher.Matcher.from(kind);
                if (matcher == null) {
                    throw new ParsingException(source(labelCtx), "Unrecognized label matcher [{}]", kind);
                }
                var nameCtx = labelCtx.labelName();
                String labelName = visitLabelName(nameCtx);
                if (labelName.contains(":")) {
                    throw new ParsingException(source(nameCtx), "[:] not allowed in label names [{}]", labelName);
                }
                String labelValue = string(labelCtx.STRING());
                Source valueCtx = source(labelCtx.STRING());
                // name cannot be defined twice
                if (NAME.equals(labelName)) {
                    if (id != null) {
                        throw new ParsingException(source(nameCtx), "Metric name must not be defined twice: [{}] or [{}]", id, labelValue);
                    }
                    id = labelValue;
                    series = new UnresolvedAttribute(valueCtx, id);
                }
                labelExpressions.add(new UnresolvedAttribute(source(nameCtx), labelName));

                LabelMatcher label = new LabelMatcher(labelName, labelValue, matcher);
                // require at least one empty non-empty matcher
                if (nonEmptyMatcher == false && label.matchesEmpty() == false) {
                    nonEmptyMatcher = true;
                }
                labels.add(label);
            }
            if (nonEmptyMatcher == false) {
                throw new ParsingException(source(labelsCtx), "Vector selector must contain at least one non-empty matcher");
            }
        }
        Evaluation evaluation = visitEvaluation(ctx.evaluation());
        TimeValue range = visitDuration(ctx.duration());
        // TODO: TimeValue might not be needed after all
        Expression rangeEx = new Literal(source(ctx.duration()), Duration.ofSeconds(range.getSeconds()), DataType.TIME_DURATION);
        // fall back to default
        if (evaluation == null) {
            evaluation = new Evaluation(start);
        }

        final LabelMatchers matchers = new LabelMatchers(labels);
        final Evaluation finalEvaluation = evaluation;

        UnresolvedAttribute timestamp = new UnresolvedAttribute(source, MetadataAttribute.TIMESTAMP_FIELD);

        return range == null
            ? new InstantSelector(source, series, labelExpressions, matchers, finalEvaluation, timestamp)
            : new RangeSelector(source, series, labelExpressions, matchers, rangeEx, finalEvaluation, timestamp);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Object visitFunction(PromqlBaseParser.FunctionContext ctx) {
        Source source = source(ctx);
        String name = ctx.IDENTIFIER().getText().toLowerCase(Locale.ROOT);

        Boolean exists = PromqlFunctionRegistry.INSTANCE.functionExists(name);
        if (Boolean.TRUE.equals(exists) == false) {
            String message = exists == null ? "Function [{}] not implemented yet" : "Unknown function [{}]";
            throw new ParsingException(source, message, name);
        }

        var metadata = PromqlFunctionRegistry.INSTANCE.functionMetadata(name);

        // TODO: the list of params could contain literals so need to handle that
        var paramsCtx = ctx.functionParams();
        List<Node> params = paramsCtx != null ? visitList(this, paramsCtx.expression(), Node.class) : emptyList();

        int paramCount = params.size();
        String message = "Invalid number of parameters for function [{}], required [{}], found [{}]";
        if (paramCount < metadata.arity().min()) {
            throw new ParsingException(source, message, name, metadata.arity().min(), paramCount);
        }
        if (paramCount > metadata.arity().max()) {
            throw new ParsingException(source, message, name, metadata.arity().max(), paramCount);
        }

        // child plan is always the first parameter
        LogicalPlan child = (LogicalPlan) params.get(0);

        // PromQl expects early validation of the tree so let's do it here
        PromqlBaseParser.GroupingContext groupingContext = ctx.grouping();

        LogicalPlan plan = null;
        // explicit grouping
        if (groupingContext != null) {
            var grouping = groupingContext.BY() != null ? AcrossSeriesAggregate.Grouping.BY : AcrossSeriesAggregate.Grouping.WITHOUT;

            if (grouping != AcrossSeriesAggregate.Grouping.BY) {
                throw new ParsingException(source, "[{}] clause not supported yet", grouping.name().toLowerCase(Locale.ROOT), name);
            }

            if (metadata.functionType() != ACROSS_SERIES_AGGREGATION) {
                throw new ParsingException(
                    source,
                    "[{}] clause not allowed on non-aggregation function [{}]",
                    grouping.name().toLowerCase(Locale.ROOT),
                    name
                );
            }

            PromqlBaseParser.LabelListContext labelListCtx = groupingContext.labelList();
            List<String> groupingKeys = visitLabelList(labelListCtx);
            // TODO: this
            List<Expression> groupings = new ArrayList<>(groupingKeys.size());
            for (int i = 0; i < groupingKeys.size(); i++) {
                groupings.add(new UnresolvedAttribute(source(labelListCtx.labelName(i)), groupingKeys.get(i)));
            }
            plan = new AcrossSeriesAggregate(source, child, name, List.of(), grouping, groupings);
        } else {
            if (metadata.functionType() == ACROSS_SERIES_AGGREGATION) {
                plan = new AcrossSeriesAggregate(source, child, name, List.of(), AcrossSeriesAggregate.Grouping.NONE, List.of());
            } else if (metadata.functionType() == WITHIN_SERIES_AGGREGATION) {
                if (child instanceof RangeSelector == false) {
                    throw new ParsingException(source, "expected type range vector in call to function [{}], got instant vector", name);
                }

                plan = new WithinSeriesAggregate(source, child, name, List.of());
                // instant selector function - definitely no grouping
            }
        }
        //
        return plan;
    }

    @Override
    public Subquery visitSubquery(PromqlBaseParser.SubqueryContext ctx) {
        Source source = source(ctx);
        Expression expression = expression(ctx.expression());

        if (PromqlDataTypes.isInstantVector(expression.dataType()) == false) {
            throw new ParsingException(source, "Subquery is only allowed on instant vector, got {}", expression.dataType().typeName());
        }

        Evaluation evaluation = visitEvaluation(ctx.evaluation());
        if (evaluation == null) {
            // TODO: fallback to defaults
        }

        Expression rangeEx = expression(ctx.range);
        Expression resolution = expression(ctx.subqueryResolution());

        return new Subquery(
            source(ctx),
            expression(ctx.expression()),
            expressionToTimeValue(rangeEx),
            expressionToTimeValue(resolution),
            evaluation
        );
    }
}
