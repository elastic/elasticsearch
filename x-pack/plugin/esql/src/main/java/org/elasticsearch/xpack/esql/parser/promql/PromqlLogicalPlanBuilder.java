/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.Arithmetics;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.promql.subquery.Subquery;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.AcrossSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.WithinSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorBinaryOperator.BinaryOp;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.VectorMatch;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.arithmetic.VectorBinaryArithmetic;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.arithmetic.VectorBinaryArithmetic.ArithmeticOp;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.comparison.VectorBinaryComparison;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.comparison.VectorBinaryComparison.ComparisonOp;
import org.elasticsearch.xpack.esql.plan.logical.promql.operator.set.VectorBinarySet;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Evaluation;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.InstantSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatchers;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.RangeSelector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.expression.promql.function.FunctionType.ACROSS_SERIES_AGGREGATION;
import static org.elasticsearch.xpack.esql.expression.promql.function.FunctionType.WITHIN_SERIES_AGGREGATION;
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

    public static final Duration GLOBAL_EVALUATION_INTERVAL = Duration.ofMinutes(1);

    PromqlLogicalPlanBuilder(Literal start, Literal end, int startLine, int startColumn) {
        super(start, end, startLine, startColumn);
    }

    protected LogicalPlan plan(ParseTree ctx) {
        // wrap literal (expressions) into a plan to on demand instead of passing around
        // LiteralSelector everywhere
        return wrapLiteral(ctx);
    }

    @Override
    public LogicalPlan visitSingleStatement(PromqlBaseParser.SingleStatementContext ctx) {
        return plan(ctx.expression());
    }

    static boolean isRangeVector(LogicalPlan plan) {
        return switch (plan) {
            case RangeSelector r -> true;
            case Subquery s -> true;
            default -> false;
        };
    }

    static boolean isScalar(LogicalPlan plan) {
        return plan instanceof LiteralSelector;
    }

    private LogicalPlan wrapLiteral(ParseTree ctx) {
        if (ctx == null) {
            return null;
        }
        Source source = source(ctx);
        Object result = visit(ctx);
        return switch (result) {
            case LogicalPlan plan -> plan;
            case Literal literal -> new LiteralSelector(source, literal);
            case Duration duration -> new LiteralSelector(source, Literal.timeDuration(source, duration));
            case Expression expr -> throw new ParsingException(
                source,
                "Expected a plan or literal, got expression [{}]",
                expr.getClass().getSimpleName()
            );
            default -> throw new ParsingException(source, "Expected a plan, got [{}]", result.getClass().getSimpleName());
        };
    }

    private Literal unwrapLiteral(ParserRuleContext ctx) {
        Object o = visit(ctx);
        return switch (o) {
            case Literal literal -> literal;
            case Expression expression -> {
                if (expression.foldable()) {
                    yield Literal.of(FoldContext.small(), expression);
                }
                throw new ParsingException(source(ctx), "Constant expression required, found [{}]", expression.sourceText());
            }
            case LiteralSelector selector -> selector.literal();
            default -> throw new ParsingException(source(ctx), "Constant expression required, found [{}]", ctx.getText());
        };
    }

    @Override
    public LogicalPlan visitSelector(PromqlBaseParser.SelectorContext ctx) {
        Source source = source(ctx);
        PromqlBaseParser.SeriesMatcherContext seriesMatcher = ctx.seriesMatcher();
        String id = visitIdentifier(seriesMatcher.identifier());
        List<LabelMatcher> labels = new ArrayList<>();
        Expression series = null;
        List<Expression> labelExpressions = new ArrayList<>();

        boolean identifierId = (id != null);

        if (id != null) {
            labels.add(new LabelMatcher(NAME, id, LabelMatcher.Matcher.EQ));
            // TODO: metric/ts name can be missing (e.g. {label=~"value"})
            series = new UnresolvedAttribute(source(seriesMatcher.identifier()), id);
        }

        boolean nonEmptyMatcher = id != null;
        Set<String> seenLabelNames = new LinkedHashSet<>();

        PromqlBaseParser.LabelsContext labelsCtx = seriesMatcher.labels();
        if (labelsCtx != null) {
            // if no name is specified, check for non-empty matchers
            for (PromqlBaseParser.LabelContext labelCtx : labelsCtx.label()) {
                var nameCtx = labelCtx.labelName();
                String labelName = visitLabelName(nameCtx);
                if (labelName.contains(":")) {
                    throw new ParsingException(source(nameCtx), "[:] not allowed in label names [{}]", labelName);
                }

                // shortcut for specifying the name (no matcher operator)
                if (labelCtx.kind == null) {
                    if (identifierId) {
                        throw new ParsingException(source(labelCtx), "Metric name must not be defined twice: [{}] or [{}]", id, labelName);
                    }
                    // set id/series from first label-based name
                    if (id == null) {
                        id = labelName;
                        series = new UnresolvedAttribute(source(labelCtx), id);
                    }
                    // always add as label matcher
                    labels.add(new LabelMatcher(NAME, labelName, LabelMatcher.Matcher.EQ));
                    // add unresolved attribute on first encounter
                    if (seenLabelNames.add(NAME)) {
                        labelExpressions.add(new UnresolvedAttribute(source(nameCtx), NAME));
                    }
                    nonEmptyMatcher = true;

                    continue;
                }

                String kind = labelCtx.kind.getText();
                LabelMatcher.Matcher matcher = LabelMatcher.Matcher.from(kind);
                if (matcher == null) {
                    throw new ParsingException(source(labelCtx), "Unrecognized label matcher [{}]", kind);
                }

                String labelValue = string(labelCtx.STRING());
                Source valueCtx = source(labelCtx.STRING());
                // __name__ with explicit matcher
                if (NAME.equals(labelName)) {
                    if (identifierId) {
                        throw new ParsingException(source(nameCtx), "Metric name must not be defined twice: [{}] or [{}]", id, labelValue);
                    }
                    // set id/series from first label-based name
                    if (id == null) {
                        id = labelValue;
                        series = new UnresolvedAttribute(valueCtx, id);
                    }
                }

                // always add label matcher
                LabelMatcher label = new LabelMatcher(labelName, labelValue, matcher);
                labels.add(label);
                // add unresolved attribute on first encounter
                if (seenLabelNames.add(labelName)) {
                    labelExpressions.add(new UnresolvedAttribute(source(nameCtx), labelName));
                }

                // require at least one non-empty matcher
                if (nonEmptyMatcher == false && label.matchesEmpty() == false) {
                    nonEmptyMatcher = true;
                }
            }

            if (nonEmptyMatcher == false) {
                throw new ParsingException(source(labelsCtx), "Vector selector must contain at least one non-empty matcher");
            }
        }
        Evaluation evaluation = visitEvaluation(ctx.evaluation());
        Expression range = visitDuration(ctx.duration());

        final LabelMatchers matchers = new LabelMatchers(labels);

        return range == Literal.NULL
            ? new InstantSelector(source, series, labelExpressions, matchers, evaluation)
            : new RangeSelector(source, series, labelExpressions, matchers, range, evaluation);
    }

    @Override
    public LogicalPlan visitArithmeticUnary(PromqlBaseParser.ArithmeticUnaryContext ctx) {
        Source source = source(ctx);
        LogicalPlan unary = wrapLiteral(ctx.expression());

        // unary operators do not make sense outside numeric data
        if (unary instanceof LiteralSelector literalSelector) {
            Literal literal = literalSelector.literal();
            Object value = literal.value();
            DataType dataType = literal.dataType();
            if (dataType.isNumeric() == false || value instanceof Number == false) {
                throw new ParsingException(
                    source,
                    "Unary expression only allowed on expressions of type numeric or instant vector, got [{}]",
                    dataType.typeName()
                );
            }
            // optimize negation in case of literals
            if (ctx.operator.getType() == MINUS) {
                Number negatedValue = Arithmetics.negate((Number) value);
                unary = new LiteralSelector(source, new Literal(unary.source(), negatedValue, dataType));
            }
        }
        // forbid range selectors
        else if (isRangeVector(unary)) {
            throw new ParsingException(
                source,
                "Unary expression only allowed on expressions of type numeric or instant vector, got [{}]",
                unary.nodeName()
            );
        }

        // For non-literals (vectors), rewrite as 0 - expression
        if (ctx.operator.getType() == MINUS) {
            LiteralSelector zero = new LiteralSelector(source, Literal.integer(source, 0));
            return new VectorBinaryArithmetic(source, zero, unary, VectorMatch.NONE, ArithmeticOp.SUB);
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
        boolean leftIsScalar = isScalar(le);
        boolean rightIsScalar = isScalar(re);

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

        BinaryOp binaryOperator = binaryOp(ctx.op);

        // Handle scalar folding once validation passes
        if (le instanceof LiteralSelector leftSel && re instanceof LiteralSelector rightSel) {
            Literal leftLiteral = leftSel.literal();
            Literal rightLiteral = rightSel.literal();

            // Extract values
            Object leftValue = leftLiteral.value();
            Object rightValue = rightLiteral.value();

            // arithmetics
            if (binaryOperator instanceof ArithmeticOp arithmeticOp) {
                Object result = PromqlFoldingUtils.evaluate(source, leftValue, rightValue, arithmeticOp);
                DataType resultType = determineResultType(result);
                return new LiteralSelector(source, new Literal(source, result, resultType));
            }

            // comparisons
            if (binaryOperator instanceof ComparisonOp compOp) {
                int result = PromqlFoldingUtils.evaluate(source, leftValue, rightValue, compOp) ? 1 : 0;
                return new LiteralSelector(source, Literal.integer(source, result));
            }

            // Set operations fall through to vector handling
        }

        VectorMatch modifier = VectorMatch.NONE;

        PromqlBaseParser.ModifierContext modifierCtx = ctx.modifier();
        if (modifierCtx != null) {
            // modifiers work only on vectors
            if (isRangeVector(le) || isRangeVector(re) || isScalar(le) || isScalar(re)) {
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

        return switch (binaryOperator) {
            case ArithmeticOp arithmeticOp -> new VectorBinaryArithmetic(source, le, re, modifier, arithmeticOp);
            case ComparisonOp comparisonOp -> new VectorBinaryComparison(source, le, re, modifier, bool, comparisonOp);
            case VectorBinarySet.SetOp setOp -> new VectorBinarySet(source, le, re, modifier, setOp);
            default -> throw new ParsingException(source(ctx.op), "Unknown arithmetic {}", opText);
        };
    }

    private BinaryOp binaryOp(Token opType) {
        return switch (opType.getType()) {
            case CARET -> ArithmeticOp.POW;
            case ASTERISK -> ArithmeticOp.MUL;
            case PERCENT -> ArithmeticOp.MOD;
            case SLASH -> ArithmeticOp.DIV;
            case MINUS -> ArithmeticOp.SUB;
            case PLUS -> ArithmeticOp.ADD;
            case EQ -> ComparisonOp.EQ;
            case NEQ -> ComparisonOp.NEQ;
            case LT -> ComparisonOp.LT;
            case LTE -> ComparisonOp.LTE;
            case GT -> ComparisonOp.GT;
            case GTE -> ComparisonOp.GTE;
            case AND -> VectorBinarySet.SetOp.INTERSECT;
            case UNLESS -> VectorBinarySet.SetOp.SUBTRACT;
            case OR -> VectorBinarySet.SetOp.UNION;
            default -> throw new ParsingException(source(opType), "Unknown arithmetic {}", opType.getText());
        };
    }

    /**
     * Determine DataType from the result value.
     */
    private DataType determineResultType(Object value) {
        return switch (value) {
            case Duration d -> DataType.TIME_DURATION;
            case Integer i -> DataType.INTEGER;
            case Long l -> DataType.LONG;
            case Double d -> DataType.DOUBLE;
            case Number n -> DataType.DOUBLE; // fallback for other Number types
            default -> throw new IllegalArgumentException("Unexpected result type: " + value.getClass());
        };
    }

    @Override
    public Object visitParenthesized(PromqlBaseParser.ParenthesizedContext ctx) {
        return visit(ctx.expression());
    }

    @Override
    @SuppressWarnings("rawtypes")
    public LogicalPlan visitFunction(PromqlBaseParser.FunctionContext ctx) {
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
                if (isRangeVector(child) == false) {
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
        LogicalPlan plan = plan(ctx.expression());

        if (isRangeVector(plan)) {
            throw new ParsingException(source, "Subquery is only allowed on instant vector, got {}", plan.nodeName());
        }

        Evaluation evaluation = visitEvaluation(ctx.evaluation());
        Literal rangeEx = visitDuration(ctx.range);
        Literal resolution = visitSubqueryResolution(ctx.subqueryResolution());

        if (resolution == null) {
            resolution = Literal.timeDuration(Source.EMPTY, GLOBAL_EVALUATION_INTERVAL);
        }
        return new Subquery(source(ctx), plan, rangeEx, resolution, evaluation);
    }

    /**
     * Parse subquery resolution, reusing the same expression folding logic used for duration arithmetic.
     */
    public Literal visitSubqueryResolution(PromqlBaseParser.SubqueryResolutionContext ctx) {
        if (ctx == null) {
            return Literal.NULL;
        }

        // Case 1: COLON (resolution=duration)?
        // Examples: ":5m", ":(5m + 1m)", etc.
        // This reuses visitDuration which already handles arithmetic through expression folding
        if (ctx.resolution != null) {
            return visitDuration(ctx.resolution);
        }

        // Case 2-5: TIME_VALUE_WITH_COLON cases
        // Examples: ":5m", ":5m * 2", ":5m ^ 2", ":5m + 1m", etc.
        var timeCtx = ctx.TIME_VALUE_WITH_COLON();
        if (timeCtx != null) {
            // Parse the base time value (e.g., ":5m" -> "5m")
            String timeString = timeCtx.getText().substring(1).trim();
            Source timeSource = source(timeCtx);
            Duration baseValue = PromqlParserUtils.parseDuration(timeSource, timeString);

            if (ctx.op == null || ctx.expression() == null) {
                return Literal.timeDuration(source(timeCtx), baseValue);
            }

            // Evaluate right expression
            Object rightValue = unwrapLiteral(ctx.expression()).value();

            // Perform arithmetic using utility
            BinaryOp binaryOp = binaryOp(ctx.op);
            Object result;
            if (binaryOp instanceof ArithmeticOp operation) {
                result = PromqlFoldingUtils.evaluate(source(ctx), baseValue, rightValue, operation);
            } else {
                throw new ParsingException(source(ctx), "Unsupported binary operator [{}] in time duration", binaryOp);
            }
            // Result should be Duration
            if (result instanceof Duration duration) {
                return Literal.timeDuration(source(timeCtx), duration);
            }

            throw new ParsingException(source(ctx), "Expected duration result, got [{}]", result.getClass().getSimpleName());
        }

        // Just COLON with no resolution - use default
        return Literal.NULL;
    }
}
