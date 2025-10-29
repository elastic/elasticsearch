/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.VectorMatch;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.arithmetic.VectorBinaryArithmetic;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.comparison.VectorBinaryComparison;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.set.VectorBinarySet;
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
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.expression.promql.function.FunctionType.ACROSS_SERIES_AGGREGATION;
import static org.elasticsearch.xpack.esql.expression.promql.function.FunctionType.WITHIN_SERIES_AGGREGATION;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.typedParsing;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.visitList;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.AND;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.ASTERISK;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.CARET;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.EQ;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.GT;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.GTE;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.LT;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.LTE;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.MINUS;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.NEQ;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.OR;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.PERCENT;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.PLUS;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.SLASH;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.UNLESS;
import static org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher.NAME;

public class PromqlLogicalPlanBuilder extends PromqlExpressionBuilder {

    PromqlLogicalPlanBuilder() {
        this(null, null, 0, 0);
    }

    PromqlLogicalPlanBuilder(Instant start, Instant end, int startLine, int startColumn) {
        super(start, end, startLine, startColumn);
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
        var durationCtx = ctx.duration();
        Expression rangeEx = null;
        if (durationCtx != null) {
            TimeValue range = visitDuration(durationCtx);
            // TODO: TimeValue might not be needed after all
            rangeEx = new Literal(source(ctx.duration()), Duration.ofSeconds(range.getSeconds()), DataType.TIME_DURATION);
            // fall back to default
            if (evaluation == null) {
                evaluation = Evaluation.NONE;
            }
        }

        final LabelMatchers matchers = new LabelMatchers(labels);
        final Evaluation finalEvaluation = evaluation;

        UnresolvedTimestamp timestamp = new UnresolvedTimestamp(source);

        return rangeEx == null
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

    private LogicalPlan wrapLiteral(ParserRuleContext ctx) {
        if (ctx == null) {
            return null;
        }
        Source source = source(ctx);
        Object result = visit(ctx);
        return switch (result) {
            case LogicalPlan plan -> plan;
            case Literal literal -> new LiteralSelector(source, literal);
            case Expression expr -> throw new ParsingException(
                source,
                "Expected a plan or literal, got expression [{}]",
                expr.getClass().getSimpleName()
            );
            default -> throw new ParsingException(source, "Expected a plan, got [{}]", result.getClass().getSimpleName());
        };
    }

    @Override
    public LogicalPlan visitArithmeticUnary(PromqlBaseParser.ArithmeticUnaryContext ctx) {
        Source source = source(ctx);
        LogicalPlan unary = wrapLiteral(ctx.expression());

        if (ctx.operator.getType() == MINUS) {
            // TODO: optimize negation of literal
            LiteralSelector zero = new LiteralSelector(source, Literal.fromDouble(source, 0.0));
            return new VectorBinaryArithmetic(source, zero, unary, VectorMatch.NONE, VectorBinaryArithmetic.ArithmeticOp.SUB);
        }

        return unary;
    }

    @Override
    public LogicalPlan visitArithmeticBinary(PromqlBaseParser.ArithmeticBinaryContext ctx) {
        Source source = source(ctx);
        LogicalPlan le = wrapLiteral(ctx.left);
        LogicalPlan re = wrapLiteral(ctx.right);

        boolean bool = ctx.BOOL() != null;
        int opType = ctx.op.getType();
        String opText = ctx.op.getText();

        // validate operation against expression types
        boolean leftIsScalar = le instanceof LiteralSelector;
        boolean rightIsScalar = re instanceof LiteralSelector;

        // comparisons against scalars require bool
        if (bool == false && leftIsScalar && rightIsScalar) {
            switch (opType) {
                case EQ:
                case NEQ:
                case LT:
                case LTE:
                case GT:
                case GTE:
                    throw new ParsingException(source, "Comparisons [{}] between scalars must use the BOOL modifier", opText);
            }
        }
        // set operations are not allowed on scalars
        if (leftIsScalar || rightIsScalar) {
            switch (opType) {
                case AND:
                case UNLESS:
                case OR:
                    throw new ParsingException(source, "Set operator [{}] not allowed in binary scalar expression", opText);
            }
        }

        VectorMatch modifier = VectorMatch.NONE;

        PromqlBaseParser.ModifierContext modifierCtx = ctx.modifier();
        if (modifierCtx != null) {
            // modifiers work only on vectors
            if (le instanceof RangeSelector || re instanceof RangeSelector) {
                throw new ParsingException(source, "Vector matching allowed only between instant vectors");
            }

            VectorMatch.Filter filter = modifierCtx.ON() != null ? VectorMatch.Filter.ON : VectorMatch.Filter.IGNORING;
            List<String> filterList = visitLabelList(modifierCtx.modifierLabels);
            VectorMatch.Joining joining = VectorMatch.Joining.NONE;
            List<String> groupingList = visitLabelList(modifierCtx.groupLabels);
            if (modifierCtx.joining != null) {
                joining = modifierCtx.GROUP_LEFT() != null ? VectorMatch.Joining.LEFT : VectorMatch.Joining.RIGHT;

                // grouping not allowed with logic operators
                switch (opType) {
                    case AND:
                    case UNLESS:
                    case OR:
                        throw new ParsingException(source(modifierCtx), "No grouping [{}] allowed for [{}] operator", joining, opText);
                }

                // label declared in ON cannot appear in grouping
                if (modifierCtx.ON() != null) {
                    List<String> repeatedLabels = new ArrayList<>(groupingList);
                    if (filterList.isEmpty() == false && repeatedLabels.retainAll(filterList) && repeatedLabels.isEmpty() == false) {
                        throw new ParsingException(
                            source(modifierCtx.ON()),
                            "Label{} {} must not occur in ON and GROUP clause at once",
                            repeatedLabels.size() > 1 ? "s" : "",
                            repeatedLabels
                        );
                    }

                }
            }

            modifier = new VectorMatch(filter, new LinkedHashSet<>(filterList), joining, new LinkedHashSet<>(groupingList));
        }

        VectorBinaryOperator.BinaryOp binaryOperator = switch (opType) {
            case CARET -> VectorBinaryArithmetic.ArithmeticOp.POW;
            case ASTERISK -> VectorBinaryArithmetic.ArithmeticOp.MUL;
            case PERCENT -> VectorBinaryArithmetic.ArithmeticOp.MOD;
            case SLASH -> VectorBinaryArithmetic.ArithmeticOp.DIV;
            case MINUS -> VectorBinaryArithmetic.ArithmeticOp.SUB;
            case PLUS -> VectorBinaryArithmetic.ArithmeticOp.ADD;
            case EQ -> VectorBinaryComparison.ComparisonOp.EQ;
            case NEQ -> VectorBinaryComparison.ComparisonOp.NEQ;
            case LT -> VectorBinaryComparison.ComparisonOp.LT;
            case LTE -> VectorBinaryComparison.ComparisonOp.LTE;
            case GT -> VectorBinaryComparison.ComparisonOp.GT;
            case GTE -> VectorBinaryComparison.ComparisonOp.GTE;
            case AND -> VectorBinarySet.SetOp.INTERSECT;
            case UNLESS -> VectorBinarySet.SetOp.SUBTRACT;
            case OR -> VectorBinarySet.SetOp.UNION;
            default -> throw new ParsingException(source(ctx.op), "Unknown arithmetic {}", opText);
        };

        return switch (binaryOperator) {
            case VectorBinaryArithmetic.ArithmeticOp arithmeticOp -> new VectorBinaryArithmetic(source, le, re, modifier, arithmeticOp);
            case VectorBinaryComparison.ComparisonOp comparisonOp -> new VectorBinaryComparison(
                source,
                le,
                re,
                modifier,
                bool,
                comparisonOp
            );
            case VectorBinarySet.SetOp setOp -> new VectorBinarySet(source, le, re, modifier, setOp);
            default -> throw new ParsingException(source(ctx.op), "Unknown arithmetic {}", opText);
        };
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
