/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.dissect.DissectException;
import org.elasticsearch.dissect.DissectParser;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.PromqlFeatures;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.EmptyAttribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedStar;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.parser.promql.PromqlParserUtils;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.QuerySetting;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.ChangePoint;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Explain;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Insist;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Lookup;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.fuse.Fuse;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.show.ShowInfo;
import org.joni.exception.SyntaxException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.WILDCARD;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputExpressions;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.source;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.typedParsing;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.visitList;
import static org.elasticsearch.xpack.esql.plan.logical.Enrich.Mode;

/**
 * Translates what we get back from Antlr into the data structures the rest of the planner steps will act on.  Generally speaking, things
 * which change the grammar will need to make changes here as well.
 */
public class LogicalPlanBuilder extends ExpressionBuilder {

    private static final String TIME = "time", START = "start", END = "end", STEP = "step";
    private static final Set<String> PROMQL_ALLOWED_PARAMS = Set.of(TIME, START, END, STEP);

    /**
     * Maximum number of commands allowed per query
     */
    public static final int MAX_QUERY_DEPTH = 500;

    public LogicalPlanBuilder(ParsingContext context) {
        super(context);
    }

    private int queryDepth = 0;

    protected EsqlStatement statement(ParseTree ctx) {
        EsqlStatement p = typedParsing(this, ctx, EsqlStatement.class);
        return p;
    }

    protected LogicalPlan plan(ParseTree ctx) {
        LogicalPlan p = ParserUtils.typedParsing(this, ctx, LogicalPlan.class);
        if (p instanceof Explain == false && p.anyMatch(logicalPlan -> logicalPlan instanceof Explain)) {
            throw new ParsingException(source(ctx), "EXPLAIN does not support downstream commands");
        }
        if (p instanceof Explain explain && explain.query().anyMatch(logicalPlan -> logicalPlan instanceof Explain)) {
            // TODO this one is never reached because the Parser fails to understand multiple round brackets
            throw new ParsingException(source(ctx), "EXPLAIN cannot be used inside another EXPLAIN command");
        }
        var errors = this.context.params().parsingErrors();
        if (errors.hasNext() == false) {
            return p;
        } else {
            throw ParsingException.combineParsingExceptions(errors);
        }
    }

    @Override
    public EsqlStatement visitStatements(EsqlBaseParser.StatementsContext ctx) {
        List<QuerySetting> settings = new ArrayList<>();
        for (EsqlBaseParser.SetCommandContext setCommandContext : ctx.setCommand()) {
            settings.add(visitSetCommand(setCommandContext));
        }

        LogicalPlan query = visitSingleStatement(ctx.singleStatement());
        return new EsqlStatement(query, settings);
    }

    protected List<LogicalPlan> plans(List<? extends ParserRuleContext> ctxs) {
        return ParserUtils.visitList(this, ctxs, LogicalPlan.class);
    }

    @Override
    public LogicalPlan visitSingleStatement(EsqlBaseParser.SingleStatementContext ctx) {
        var plan = plan(ctx.query());
        telemetryAccounting(plan);
        return plan;
    }

    @Override
    public QuerySetting visitSetCommand(EsqlBaseParser.SetCommandContext ctx) {
        var field = visitSetField(ctx.setField());
        return new QuerySetting(source(ctx), field);
    }

    @Override
    public Alias visitSetField(EsqlBaseParser.SetFieldContext ctx) {
        String name = visitIdentifier(ctx.identifier());
        Expression value = expression(ctx.constant());
        return new Alias(source(ctx), name, value);
    }

    @Override
    public LogicalPlan visitCompositeQuery(EsqlBaseParser.CompositeQueryContext ctx) {
        queryDepth++;
        if (queryDepth > MAX_QUERY_DEPTH) {
            throw new ParsingException(
                "ESQL statement exceeded the maximum query depth allowed ({}): [{}]",
                MAX_QUERY_DEPTH,
                ctx.getText()
            );
        }
        try {
            LogicalPlan input = plan(ctx.query());
            telemetryAccounting(input);
            PlanFactory makePlan = typedParsing(this, ctx.processingCommand(), PlanFactory.class);
            return makePlan.apply(input);
        } finally {
            queryDepth--;
        }
    }

    private LogicalPlan telemetryAccounting(LogicalPlan node) {
        if (node instanceof TelemetryAware ma) {
            this.context.telemetry().command(ma);
        }
        return node;
    }

    @Override
    public PlanFactory visitEvalCommand(EsqlBaseParser.EvalCommandContext ctx) {
        return p -> new Eval(source(ctx), p, visitFields(ctx.fields()));
    }

    @Override
    public PlanFactory visitGrokCommand(EsqlBaseParser.GrokCommandContext ctx) {
        return p -> {
            Source source = source(ctx);
            FoldContext patternFoldContext = FoldContext.small(); /* TODO remove me */
            List<String> patterns = ctx.string()
                .stream()
                .map(stringContext -> BytesRefs.toString(visitString(stringContext).fold(patternFoldContext)))
                .toList();

            for (int i = 0; i < patterns.size(); i++) {
                String pattern = patterns.get(i);

                // Validate each pattern individually,
                // as multiple invalid patterns could be combined to form a valid one
                // see https://github.com/elastic/elasticsearch/issues/136750
                try {
                    Grok.pattern(source, pattern);
                } catch (SyntaxException e) {
                    throw new ParsingException(source(ctx.string(i)), "Invalid GROK pattern [{}]: [{}]", pattern, e.getMessage());
                }
            }

            String combinePattern = org.elasticsearch.grok.Grok.combinePatterns(patterns);

            Grok.Parser grokParser = Grok.pattern(source, combinePattern);

            validateGrokPattern(source, grokParser, combinePattern, patterns);
            Grok result = new Grok(source(ctx), p, expression(ctx.primaryExpression()), grokParser);
            return result;
        };
    }

    private void validateGrokPattern(Source source, Grok.Parser grokParser, String pattern, List<String> originalPatterns) {
        Map<String, DataType> definedAttributes = new HashMap<>();
        for (Attribute field : grokParser.extractedFields()) {
            String name = field.name();
            DataType type = field.dataType();
            DataType prev = definedAttributes.put(name, type);
            if (prev != null) {
                if (originalPatterns.size() == 1) {
                    throw new ParsingException(
                        source,
                        "Invalid GROK pattern [{}]: the attribute [{}] is defined multiple times with different types",
                        originalPatterns.getFirst(),
                        name
                    );
                } else {
                    throw new ParsingException(
                        source,
                        "Invalid GROK patterns {}: the attribute [{}] is defined multiple times with different types",
                        originalPatterns,
                        name
                    );
                }
            }
        }
    }

    @Override
    public PlanFactory visitDissectCommand(EsqlBaseParser.DissectCommandContext ctx) {
        return p -> {
            String pattern = BytesRefs.toString(visitString(ctx.string()).fold(FoldContext.small() /* TODO remove me */));
            Map<String, Object> options = visitDissectCommandOptions(ctx.dissectCommandOptions());
            String appendSeparator = "";
            for (Map.Entry<String, Object> item : options.entrySet()) {
                if (item.getKey().equalsIgnoreCase("append_separator") == false) {
                    throw new ParsingException(source(ctx), "Invalid option for dissect: [{}]", item.getKey());
                }
                if (item.getValue() instanceof BytesRef == false) {
                    throw new ParsingException(
                        source(ctx),
                        "Invalid value for dissect append_separator: expected a string, but was [{}]",
                        item.getValue()
                    );
                }
                appendSeparator = BytesRefs.toString(item.getValue());
            }
            Source src = source(ctx);

            try {
                DissectParser parser = new DissectParser(pattern, appendSeparator);

                Set<String> referenceKeys = parser.referenceKeys();
                if (referenceKeys.isEmpty() == false) {
                    throw new ParsingException(
                        src,
                        "Reference keys not supported in dissect patterns: [%{*{}}]",
                        referenceKeys.iterator().next()
                    );
                }

                Dissect.Parser esqlDissectParser = new Dissect.Parser(pattern, appendSeparator, parser);
                List<Attribute> keys = esqlDissectParser.keyAttributes(src);

                return new Dissect(src, p, expression(ctx.primaryExpression()), esqlDissectParser, keys);
            } catch (DissectException e) {
                throw new ParsingException(src, "Invalid pattern for dissect: [{}]", pattern);
            }
        };
    }

    @Override
    public PlanFactory visitMvExpandCommand(EsqlBaseParser.MvExpandCommandContext ctx) {
        UnresolvedAttribute field = visitQualifiedName(ctx.qualifiedName());
        Source src = source(ctx);
        return child -> new MvExpand(src, child, field, new UnresolvedAttribute(src, field.qualifier(), field.name(), null));

    }

    @Override
    public Map<String, Object> visitDissectCommandOptions(EsqlBaseParser.DissectCommandOptionsContext ctx) {
        if (ctx == null) {
            return Map.of();
        }
        Map<String, Object> result = new HashMap<>();
        for (EsqlBaseParser.DissectCommandOptionContext option : ctx.dissectCommandOption()) {
            result.put(visitIdentifier(option.identifier()), expression(option.constant()).fold(FoldContext.small() /* TODO remove me */));
        }
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public LogicalPlan visitRowCommand(EsqlBaseParser.RowCommandContext ctx) {
        return new Row(source(ctx), (List<Alias>) (List) mergeOutputExpressions(visitFields(ctx.fields()), List.of()));
    }

    private LogicalPlan visitRelation(Source source, IndexMode indexMode, EsqlBaseParser.IndexPatternAndMetadataFieldsContext ctx) {
        List<EsqlBaseParser.IndexPatternOrSubqueryContext> ctxs = ctx == null ? null : ctx.indexPatternOrSubquery();
        List<EsqlBaseParser.IndexPatternContext> indexPatternsCtx = new ArrayList<>();
        List<EsqlBaseParser.SubqueryContext> subqueriesCtx = new ArrayList<>();
        if (ctxs != null) {
            ctxs.forEach(c -> {
                if (c.indexPattern() != null) {
                    indexPatternsCtx.add(c.indexPattern());
                } else {
                    subqueriesCtx.add(c.subquery());
                }
            });
        }
        IndexPattern table = new IndexPattern(source, visitIndexPattern(indexPatternsCtx));
        List<Subquery> subqueries = visitSubqueriesInFromCommand(subqueriesCtx);
        Map<String, Attribute> metadataMap = new LinkedHashMap<>();
        if (ctx.metadata() != null) {
            for (var c : ctx.metadata().UNQUOTED_SOURCE()) {
                String id = c.getText();
                Source src = source(c);
                if (MetadataAttribute.isSupported(id) == false) {
                    throw new ParsingException(src, "unsupported metadata field [" + id + "]");
                }
                Attribute a = metadataMap.put(id, MetadataAttribute.create(src, id));
                if (a != null) {
                    throw new ParsingException(src, "metadata field [" + id + "] already declared [" + a.source().source() + "]");
                }
            }
        }
        List<Attribute> metadataFields = List.of(metadataMap.values().toArray(Attribute[]::new));
        final String commandName = indexMode == IndexMode.TIME_SERIES ? "TS" : "FROM";
        UnresolvedRelation unresolvedRelation = new UnresolvedRelation(source, table, false, metadataFields, indexMode, null, commandName);
        if (subqueries.isEmpty()) {
            return unresolvedRelation;
        } else {
            // subquery is not supported with time-series indices at the moment
            if (indexMode == IndexMode.TIME_SERIES) {
                throw new ParsingException(source, "Subqueries are not supported in TS command");
            }

            List<LogicalPlan> mainQueryAndSubqueries = new ArrayList<>(subqueries.size() + 1);
            if (table.indexPattern().isEmpty() == false) {
                mainQueryAndSubqueries.add(unresolvedRelation);
                telemetryAccounting(unresolvedRelation);
            }
            mainQueryAndSubqueries.addAll(subqueries);

            if (mainQueryAndSubqueries.size() == 1) {
                // if there is only one child, return it directly, no need for UnionAll
                return table.indexPattern().isEmpty() ? subqueries.get(0).plan() : unresolvedRelation;
            } else {
                // the output of UnionAll is resolved by analyzer
                return new UnionAll(source(ctxs.getFirst(), ctxs.getLast()), mainQueryAndSubqueries, List.of());
            }
        }
    }

    private List<Subquery> visitSubqueriesInFromCommand(List<EsqlBaseParser.SubqueryContext> ctxs) {
        if (EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled() == false) {
            return List.of();
        }
        if (ctxs == null) {
            return List.of();
        }
        List<Subquery> subqueries = new ArrayList<>();
        for (EsqlBaseParser.SubqueryContext ctx : ctxs) {
            LogicalPlan plan = visitSubquery(ctx);
            subqueries.add(new Subquery(source(ctx), plan));
        }
        return subqueries;
    }

    @Override
    public LogicalPlan visitSubquery(EsqlBaseParser.SubqueryContext ctx) {
        // build a subquery tree
        EsqlBaseParser.FromCommandContext fromCtx = ctx.fromCommand();
        LogicalPlan plan = visitFromCommand(fromCtx);
        List<PlanFactory> processingCommands = visitList(this, ctx.processingCommand(), PlanFactory.class);
        for (PlanFactory processingCommand : processingCommands) {
            telemetryAccounting(plan);
            plan = processingCommand.apply(plan);
        }
        telemetryAccounting(plan);
        return plan;
    }

    @Override
    public LogicalPlan visitFromCommand(EsqlBaseParser.FromCommandContext ctx) {
        return visitRelation(source(ctx), IndexMode.STANDARD, ctx.indexPatternAndMetadataFields());
    }

    @Override
    public PlanFactory visitInsistCommand(EsqlBaseParser.InsistCommandContext ctx) {
        var source = source(ctx);
        List<NamedExpression> fields = visitQualifiedNamePatterns(ctx.qualifiedNamePatterns(), ne -> {
            if (ne instanceof UnresolvedStar || ne instanceof UnresolvedNamePattern) {
                Source neSource = ne.source();
                throw new ParsingException(neSource, "INSIST doesn't support wildcards, found [{}]", neSource.text());
            }
        });
        return input -> new Insist(
            source,
            input,
            fields.stream().map(ne -> (Attribute) new UnresolvedAttribute(ne.source(), ne.name())).toList()
        );
    }

    @Override
    public PlanFactory visitStatsCommand(EsqlBaseParser.StatsCommandContext ctx) {
        final Stats stats = stats(source(ctx), ctx.grouping, ctx.stats);
        // Only the first STATS command in a TS query is treated as the time-series aggregation
        return input -> {
            if (input.anyMatch(p -> p instanceof Aggregate) == false
                && input.anyMatch(p -> p instanceof UnresolvedRelation ur && ur.indexMode() == IndexMode.TIME_SERIES)) {
                return new TimeSeriesAggregate(source(ctx), input, stats.groupings, stats.aggregates, null);
            } else {
                return new Aggregate(source(ctx), input, stats.groupings, stats.aggregates);
            }
        };
    }

    private record Stats(List<Expression> groupings, List<? extends NamedExpression> aggregates) {}

    private Stats stats(Source source, EsqlBaseParser.FieldsContext groupingsCtx, EsqlBaseParser.AggFieldsContext aggregatesCtx) {
        List<NamedExpression> groupings = visitGrouping(groupingsCtx);
        List<NamedExpression> aggregates = new ArrayList<>(visitAggFields(aggregatesCtx));

        if (aggregates.isEmpty() && groupings.isEmpty()) {
            throw new ParsingException(source, "At least one aggregation or grouping expression required in [{}]", source.text());
        }
        // grouping keys are automatically added as aggregations however the user is not allowed to specify them
        if (groupings.isEmpty() == false && aggregates.isEmpty() == false) {
            var groupNames = new LinkedHashSet<>(Expressions.names(groupings));
            var groupRefNames = new LinkedHashSet<>(Expressions.names(Expressions.references(groupings)));

            for (NamedExpression aggregate : aggregates) {
                Expression e = Alias.unwrap(aggregate);
                if (e.resolved() == false && e instanceof UnresolvedFunction == false) {
                    String name = e.sourceText();
                    if (groupNames.contains(name)) {
                        fail(e, "grouping key [{}] already specified in the STATS BY clause", name);
                    } else if (groupRefNames.contains(name)) {
                        fail(e, "Cannot specify grouping expression [{}] as an aggregate", name);
                    }
                }
            }
        }
        // since groupings are aliased, add refs to it in the aggregates
        for (Expression group : groupings) {
            aggregates.add(Expressions.attribute(group));
        }
        return new Stats(new ArrayList<>(groupings), aggregates);
    }

    private void fail(Expression exp, String message, Object... args) {
        throw new VerificationException(Collections.singletonList(Failure.fail(exp, message, args)));
    }

    @Override
    public PlanFactory visitInlineStatsCommand(EsqlBaseParser.InlineStatsCommandContext ctx) {
        var source = source(ctx);
        if (false == EsqlCapabilities.Cap.INLINE_STATS.isEnabled()) {
            throw new ParsingException(source, "INLINE STATS command currently requires a snapshot build");
        }
        // TODO: drop after next minor release
        if (ctx.INLINESTATS() != null) {
            HeaderWarning.addWarning(
                "Line {}:{}: INLINESTATS is deprecated, use INLINE STATS instead",
                source.source().getLineNumber(),
                source.source().getColumnNumber()
            );
        }
        List<Alias> aggFields = visitAggFields(ctx.stats);
        List<NamedExpression> aggregates = new ArrayList<>(aggFields);
        List<NamedExpression> groupings = visitGrouping(ctx.grouping);
        aggregates.addAll(groupings);
        return input -> new InlineStats(source, new Aggregate(source, input, new ArrayList<>(groupings), aggregates));
    }

    @Override
    public PlanFactory visitWhereCommand(EsqlBaseParser.WhereCommandContext ctx) {
        Expression expression = expression(ctx.booleanExpression());
        return input -> new Filter(source(ctx), input, expression);
    }

    @Override
    public PlanFactory visitLimitCommand(EsqlBaseParser.LimitCommandContext ctx) {
        Source source = source(ctx);
        Object val = expression(ctx.constant()).fold(FoldContext.small() /* TODO remove me */);
        if (val instanceof Integer i && i >= 0) {
            return input -> new Limit(source, new Literal(source, i, DataType.INTEGER), input);
        }

        String valueType = expression(ctx.constant()).dataType().typeName();

        throw new ParsingException(
            source,
            "value of ["
                + source.text()
                + "] must be a non negative integer, found value ["
                + ctx.constant().getText()
                + "] type ["
                + valueType
                + "]"
        );
    }

    @Override
    public PlanFactory visitSortCommand(EsqlBaseParser.SortCommandContext ctx) {
        List<Order> orders = visitList(this, ctx.orderExpression(), Order.class);
        Source source = source(ctx);
        return input -> new OrderBy(source, input, orders);
    }

    @Override
    public Explain visitExplainCommand(EsqlBaseParser.ExplainCommandContext ctx) {
        return new Explain(source(ctx), plan(ctx.subqueryExpression().query()));
    }

    @Override
    public PlanFactory visitDropCommand(EsqlBaseParser.DropCommandContext ctx) {
        List<NamedExpression> removals = visitQualifiedNamePatterns(ctx.qualifiedNamePatterns(), ne -> {
            if (ne instanceof UnresolvedStar) {
                var src = ne.source();
                throw new ParsingException(src, "Removing all fields is not allowed [{}]", src.text());
            }
        });

        return child -> new Drop(source(ctx), child, removals);
    }

    @Override
    public PlanFactory visitRenameCommand(EsqlBaseParser.RenameCommandContext ctx) {
        List<Alias> renamings = ctx.renameClause().stream().map(this::visitRenameClause).toList();
        return child -> new Rename(source(ctx), child, renamings);
    }

    @Override
    public PlanFactory visitKeepCommand(EsqlBaseParser.KeepCommandContext ctx) {
        final Holder<Boolean> hasSeenStar = new Holder<>(false);
        List<NamedExpression> projections = visitQualifiedNamePatterns(ctx.qualifiedNamePatterns(), ne -> {
            if (ne instanceof UnresolvedStar) {
                if (hasSeenStar.get()) {
                    var src = ne.source();
                    throw new ParsingException(src, "Cannot specify [*] more than once", src.text());
                } else {
                    hasSeenStar.set(Boolean.TRUE);
                }
            }
        });

        return child -> new Keep(source(ctx), child, projections);
    }

    @Override
    public LogicalPlan visitShowInfo(EsqlBaseParser.ShowInfoContext ctx) {
        return new ShowInfo(source(ctx));
    }

    @Override
    public PlanFactory visitEnrichCommand(EsqlBaseParser.EnrichCommandContext ctx) {
        return child -> {
            var source = source(ctx);
            Tuple<Mode, String> tuple = parsePolicyName(ctx.policyName);
            Mode mode = tuple.v1();
            String policyNameString = tuple.v2();

            NamedExpression matchField = ctx.ON() != null ? visitQualifiedNamePattern(ctx.matchField) : new EmptyAttribute(source);
            String patternString = matchField instanceof UnresolvedNamePattern up ? up.pattern()
                : matchField instanceof UnresolvedStar ? WILDCARD
                : null;
            if (patternString != null) {
                throw new ParsingException(
                    source,
                    "Using wildcards [*] in ENRICH WITH projections is not allowed, found [{}]",
                    patternString
                );
            }

            List<NamedExpression> keepClauses = visitList(this, ctx.enrichWithClause(), NamedExpression.class);

            // If this is a remote-only ENRICH, any upstream LOOKUP JOINs need to be treated as remote-only, too.
            if (mode == Mode.REMOTE) {
                child = child.transformDown(LookupJoin.class, lj -> new LookupJoin(lj.source(), lj.left(), lj.right(), lj.config(), true));
            }

            return new Enrich(
                source,
                child,
                mode,
                Literal.keyword(source(ctx.policyName), policyNameString),
                matchField,
                null,
                Map.of(),
                keepClauses.isEmpty() ? List.of() : keepClauses
            );
        };
    }

    @Override
    public PlanFactory visitChangePointCommand(EsqlBaseParser.ChangePointCommandContext ctx) {
        Source src = source(ctx);
        Attribute value = visitQualifiedName(ctx.value);
        Attribute key = ctx.key == null ? new UnresolvedAttribute(src, "@timestamp") : visitQualifiedName(ctx.key);

        UnresolvedAttribute parsedTargetTypeColumn = visitQualifiedName(ctx.targetType);
        UnresolvedAttribute parsedTargetPvalueColumn = visitQualifiedName(ctx.targetPvalue);

        if (parsedTargetTypeColumn != null && parsedTargetTypeColumn.qualifier() != null) {
            throw qualifiersUnsupportedInFieldDefinitions(parsedTargetTypeColumn.source(), ctx.targetType.getText());
        }

        if (parsedTargetPvalueColumn != null && parsedTargetPvalueColumn.qualifier() != null) {
            throw qualifiersUnsupportedInFieldDefinitions(parsedTargetPvalueColumn.source(), ctx.targetPvalue.getText());
        }

        Attribute targetType = new ReferenceAttribute(
            src,
            null,
            parsedTargetTypeColumn == null ? "type" : parsedTargetTypeColumn.name(),
            DataType.KEYWORD
        );
        Attribute targetPvalue = new ReferenceAttribute(
            src,
            null,
            parsedTargetPvalueColumn == null ? "pvalue" : parsedTargetPvalueColumn.name(),
            DataType.DOUBLE
        );
        return child -> new ChangePoint(src, child, value, key, targetType, targetPvalue);
    }

    private static Tuple<Mode, String> parsePolicyName(EsqlBaseParser.EnrichPolicyNameContext ctx) {
        String stringValue;
        if (ctx.ENRICH_POLICY_NAME() != null) {
            stringValue = ctx.ENRICH_POLICY_NAME().getText();
        } else {
            stringValue = ctx.QUOTED_STRING().getText();
            stringValue = stringValue.substring(1, stringValue.length() - 1);
        }

        int index = stringValue.indexOf(":");
        Mode mode = null;
        if (index >= 0) {
            String modeValue = stringValue.substring(0, index);

            if (modeValue.startsWith("_")) {
                mode = Mode.from(modeValue.substring(1));
            }

            if (mode == null) {
                throw new ParsingException(
                    source(ctx),
                    "Unrecognized value [{}], ENRICH policy qualifier needs to be one of {}",
                    modeValue,
                    Arrays.stream(Mode.values()).map(s -> "_" + s).toList()
                );
            }
        } else {
            mode = Mode.ANY;
        }

        String policyName = index < 0 ? stringValue : stringValue.substring(index + 1);
        return new Tuple<>(mode, policyName);
    }

    @Override
    public LogicalPlan visitTimeSeriesCommand(EsqlBaseParser.TimeSeriesCommandContext ctx) {
        return visitRelation(source(ctx), IndexMode.TIME_SERIES, ctx.indexPatternAndMetadataFields());
    }

    @Override
    public PlanFactory visitLookupCommand(EsqlBaseParser.LookupCommandContext ctx) {
        if (false == Build.current().isSnapshot()) {
            throw new ParsingException(source(ctx), "LOOKUP__ is in preview and only available in SNAPSHOT build");
        }
        var source = source(ctx);

        @SuppressWarnings("unchecked")
        List<Attribute> matchFields = (List<Attribute>) (List) visitQualifiedNamePatterns(ctx.qualifiedNamePatterns(), ne -> {
            if (ne instanceof UnresolvedNamePattern || ne instanceof UnresolvedStar) {
                var src = ne.source();
                throw new ParsingException(src, "Using wildcards [*] in LOOKUP ON is not allowed yet [{}]", src.text());
            }
            if ((ne instanceof UnresolvedAttribute) == false) {
                throw new IllegalStateException(
                    "visitQualifiedNamePatterns can only return UnresolvedNamePattern, UnresolvedStar or UnresolvedAttribute"
                );
            }
        });

        Literal tableName = Literal.keyword(source, visitIndexPattern(List.of(ctx.indexPattern())));

        return p -> new Lookup(source, p, tableName, matchFields, null /* localRelation will be resolved later*/);
    }

    @Override
    public PlanFactory visitJoinCommand(EsqlBaseParser.JoinCommandContext ctx) {
        var source = source(ctx);
        if (false == EsqlCapabilities.Cap.JOIN_LOOKUP_V12.isEnabled()) {
            throw new ParsingException(source, "JOIN is in preview and only available in SNAPSHOT build");
        }

        if (ctx.type != null && ctx.type.getType() != EsqlBaseParser.JOIN_LOOKUP) {
            String joinType = ctx.type == null ? "(INNER)" : ctx.type.getText();
            throw new ParsingException(source, "only LOOKUP JOIN available, {} JOIN unsupported at the moment", joinType);
        }

        var target = ctx.joinTarget();
        var rightPattern = visitIndexPattern(List.of(target.index));
        if (rightPattern.contains(WILDCARD)) {
            throw new ParsingException(source(target), "invalid index pattern [{}], * is not allowed in LOOKUP JOIN", rightPattern);
        }
        if (RemoteClusterAware.isRemoteIndexName(rightPattern)) {
            throw new ParsingException(
                source(target),
                "invalid index pattern [{}], remote clusters are not supported with LOOKUP JOIN",
                rightPattern
            );
        }
        if (rightPattern.contains(IndexNameExpressionResolver.SelectorResolver.SELECTOR_SEPARATOR)) {
            throw new ParsingException(
                source(target),
                "invalid index pattern [{}], index pattern selectors are not supported in LOOKUP JOIN",
                rightPattern
            );
        }

        UnresolvedRelation right = new UnresolvedRelation(
            source(target),
            new IndexPattern(source(target.index), rightPattern),
            false,
            emptyList(),
            IndexMode.LOOKUP,
            null
        );

        var condition = ctx.joinCondition();
        var joinInfo = typedParsing(this, condition, JoinInfo.class);

        return p -> {
            boolean hasRemotes = p.anyMatch(node -> {
                if (node instanceof UnresolvedRelation r) {
                    return Arrays.stream(Strings.splitStringByCommaToArray(r.indexPattern().indexPattern()))
                        .anyMatch(RemoteClusterAware::isRemoteIndexName);
                } else {
                    return false;
                }
            });
            if (hasRemotes && EsqlCapabilities.Cap.ENABLE_LOOKUP_JOIN_ON_REMOTE.isEnabled() == false) {
                throw new ParsingException(source, "remote clusters are not supported with LOOKUP JOIN");
            }
            return new LookupJoin(
                source,
                p,
                right,
                joinInfo.joinFields(),
                hasRemotes,
                Predicates.combineAndWithSource(joinInfo.joinExpressions(), source(condition))
            );
        };
    }

    private record JoinInfo(List<Attribute> joinFields, List<Expression> joinExpressions) {}

    @Override
    public JoinInfo visitJoinCondition(EsqlBaseParser.JoinConditionContext ctx) {
        var expressions = visitList(this, ctx.booleanExpression(), Expression.class);
        if (expressions.isEmpty()) {
            throw new ParsingException(source(ctx), "JOIN ON clause cannot be empty");
        }

        // Inspect the first expression to determine the type of join (field-based or expression-based)
        // We treat literals as field-based as it is more likely the user was trying to write a field name
        // and so the field based error message is more helpful
        boolean isFieldBased = expressions.get(0) instanceof UnresolvedAttribute || expressions.get(0) instanceof Literal;

        if (isFieldBased) {
            return processFieldBasedJoin(expressions);
        } else {
            return processExpressionBasedJoin(expressions, ctx);
        }
    }

    private JoinInfo processFieldBasedJoin(List<Expression> expressions) {
        List<Attribute> joinFields = new ArrayList<>(expressions.size());
        for (var f : expressions) {
            // verify each field is an unresolved attribute
            if (f instanceof UnresolvedAttribute ua) {
                if (ua.qualifier() != null) {
                    throw new ParsingException(
                        ua.source(),
                        "JOIN ON clause only supports unqualified fields, found [{}]",
                        ua.qualifiedName()
                    );
                }
                joinFields.add(ua);
            } else {
                throw new ParsingException(
                    f.source(),
                    "JOIN ON clause must be a comma separated list of fields or a single expression, found [{}]",
                    f.sourceText()
                );
            }
        }
        validateJoinFields(joinFields);
        return new JoinInfo(joinFields, emptyList());
    }

    private JoinInfo processExpressionBasedJoin(List<Expression> expressions, EsqlBaseParser.JoinConditionContext ctx) {
        if (LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled() == false) {
            throw new ParsingException(source(ctx), "JOIN ON clause only supports fields at the moment.");
        }
        List<Attribute> joinFields = new ArrayList<>();
        List<Expression> joinExpressions = new ArrayList<>();
        if (expressions.size() != 1) {
            throw new ParsingException(
                source(ctx),
                "JOIN ON clause with expressions only supports a single expression, found [{}]",
                expressions
            );
        }
        expressions = Predicates.splitAnd(expressions.get(0));
        for (var f : expressions) {
            addJoinExpression(f, joinFields, joinExpressions, ctx);
        }
        if (joinFields.isEmpty()) {
            throw new ParsingException(
                source(ctx),
                "JOIN ON clause with expressions must contain at least one condition relating the left index and the lookup index"
            );
        }
        return new JoinInfo(joinFields, joinExpressions);
    }

    private void addJoinExpression(
        Expression exp,
        List<Attribute> joinFields,
        List<Expression> joinExpressions,
        EsqlBaseParser.JoinConditionContext ctx
    ) {
        exp = handleNegationOfEquals(exp);
        if (containsBareFieldsInBooleanExpression(exp)) {
            throw new ParsingException(
                source(ctx),
                "JOIN ON clause only supports fields or AND of Binary Expressions at the moment, found [{}]",
                exp.sourceText()
            );
        }
        if (exp instanceof EsqlBinaryComparison comparison
            && comparison.left() instanceof UnresolvedAttribute left
            && comparison.right() instanceof UnresolvedAttribute right) {
            joinFields.add(left);
            joinFields.add(right);
        }
        joinExpressions.add(exp);
    }

    private boolean containsBareFieldsInBooleanExpression(Expression expression) {
        if (expression instanceof UnresolvedAttribute) {
            return true; // This is a bare field
        }
        if (expression instanceof EsqlBinaryComparison) {
            return false; // This is a binary comparison, not a bare field
        }
        if (expression instanceof BinaryLogic binaryLogic) {
            // Check if either side contains bare fields
            return containsBareFieldsInBooleanExpression(binaryLogic.left()) || containsBareFieldsInBooleanExpression(binaryLogic.right());
        }
        // For other expression types (functions, constants, etc.), they are not bare fields
        return false;
    }

    private void validateJoinFields(List<Attribute> joinFields) {
        if (joinFields.size() > 1) {
            Set<String> matchFieldNames = new LinkedHashSet<>();
            for (Attribute field : joinFields) {
                if (matchFieldNames.add(field.name()) == false) {
                    throw new ParsingException(
                        field.source(),
                        "JOIN ON clause does not support multiple fields with the same name, found multiple instances of [{}]",
                        field.name()
                    );
                }
            }
        }
    }

    private Expression handleNegationOfEquals(Expression f) {
        if (f instanceof Not not && not.children().size() == 1 && not.children().get(0) instanceof Equals equals) {
            // we only support NOT on Equals, by converting it to NotEquals
            return equals.negate();
        }
        return f;
    }

    private void checkForRemoteClusters(LogicalPlan plan, Source source, String commandName) {
        plan.forEachUp(UnresolvedRelation.class, r -> {
            for (var indexPattern : Strings.splitStringByCommaToArray(r.indexPattern().indexPattern())) {
                if (RemoteClusterAware.isRemoteIndexName(indexPattern)) {
                    throw new ParsingException(
                        source,
                        "invalid index pattern [{}], remote clusters are not supported with {}",
                        r.indexPattern().indexPattern(),
                        commandName
                    );
                }
            }
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public PlanFactory visitForkCommand(EsqlBaseParser.ForkCommandContext ctx) {
        List<PlanFactory> subQueries = visitForkSubQueries(ctx.forkSubQueries());
        if (subQueries.size() > Fork.MAX_BRANCHES) {
            throw new ParsingException(source(ctx), "Fork supports up to " + Fork.MAX_BRANCHES + " branches");
        }

        return input -> {
            if (EsqlCapabilities.Cap.ENABLE_FORK_FOR_REMOTE_INDICES.isEnabled() == false) {
                checkForRemoteClusters(input, source(ctx), "FORK");
            }
            List<LogicalPlan> subPlans = subQueries.stream().map(planFactory -> planFactory.apply(input)).toList();
            return new Fork(source(ctx), subPlans, List.of());
        };
    }

    @Override
    public List<PlanFactory> visitForkSubQueries(EsqlBaseParser.ForkSubQueriesContext ctx) {
        ArrayList<PlanFactory> list = new ArrayList<>();
        int count = 1; // automatic fork branch ids start at 1
        NameId firstForkNameId = null;  // stores the id of the first _fork

        for (var subQueryCtx : ctx.forkSubQuery()) {
            var subQuery = visitForkSubQuery(subQueryCtx);
            var literal = Literal.keyword(source(ctx), "fork" + count++);

            // align _fork id across all fork branches
            Alias alias = null;
            if (firstForkNameId == null) {
                alias = new Alias(source(ctx), Fork.FORK_FIELD, literal);
                firstForkNameId = alias.id();
            } else {
                alias = new Alias(source(ctx), Fork.FORK_FIELD, literal, firstForkNameId);
            }

            var finalAlias = alias;
            PlanFactory eval = p -> new Eval(source(ctx), subQuery.apply(p), List.of(finalAlias));
            list.add(eval);
        }
        return List.copyOf(list);
    }

    @Override
    public PlanFactory visitForkSubQuery(EsqlBaseParser.ForkSubQueryContext ctx) {
        var subCtx = ctx.forkSubQueryCommand();
        if (subCtx instanceof EsqlBaseParser.SingleForkSubQueryCommandContext sglCtx) {
            return typedParsing(this, sglCtx.forkSubQueryProcessingCommand(), PlanFactory.class);
        } else if (subCtx instanceof EsqlBaseParser.CompositeForkSubQueryContext compCtx) {
            return visitCompositeForkSubQuery(compCtx);
        } else {
            throw new AssertionError("Unknown context: " + ctx);
        }
    }

    @Override
    public PlanFactory visitCompositeForkSubQuery(EsqlBaseParser.CompositeForkSubQueryContext ctx) {
        PlanFactory lowerPlan = ParserUtils.typedParsing(this, ctx.forkSubQueryCommand(), PlanFactory.class);
        PlanFactory makePlan = typedParsing(this, ctx.forkSubQueryProcessingCommand(), PlanFactory.class);
        return input -> makePlan.apply(lowerPlan.apply(input));
    }

    @Override
    public PlanFactory visitFuseCommand(EsqlBaseParser.FuseCommandContext ctx) {
        Source source = source(ctx);
        return input -> {
            Attribute scoreAttr = visitFuseScoreBy(ctx.fuseConfiguration(), source);
            Attribute discriminatorAttr = visitFuseGroupBy(ctx.fuseConfiguration(), source);

            List<NamedExpression> keys = visitFuseKeyBy(ctx.fuseConfiguration(), source);

            MapExpression options = visitFuseOptions(ctx.fuseConfiguration());

            String fuseTypeName = ctx.fuseType == null ? Fuse.FuseType.RRF.name() : visitIdentifier(ctx.fuseType);
            Fuse.FuseType fuseType;
            try {
                fuseType = Fuse.FuseType.valueOf(fuseTypeName.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new ParsingException(source(ctx), "Fuse type " + fuseTypeName + " is not supported");
            }

            return new Fuse(source, input, scoreAttr, discriminatorAttr, keys, fuseType, options);
        };
    }

    private Attribute visitFuseScoreBy(List<EsqlBaseParser.FuseConfigurationContext> fuseConfigurationContexts, Source source) {
        Attribute scoreAttr = null;
        for (EsqlBaseParser.FuseConfigurationContext fuseConfigurationContext : fuseConfigurationContexts) {
            if (fuseConfigurationContext.score != null) {
                if (scoreAttr != null) {
                    throw new ParsingException(source(fuseConfigurationContext), "Only one SCORE BY can be specified");
                }
                scoreAttr = visitQualifiedName(fuseConfigurationContext.score);
            }
        }

        return scoreAttr == null ? new UnresolvedAttribute(source, MetadataAttribute.SCORE) : scoreAttr;
    }

    private Attribute visitFuseGroupBy(List<EsqlBaseParser.FuseConfigurationContext> fuseConfigurationContexts, Source source) {
        Attribute groupByAttr = null;
        for (EsqlBaseParser.FuseConfigurationContext fuseConfigurationContext : fuseConfigurationContexts) {
            if (fuseConfigurationContext.group != null) {
                if (groupByAttr != null) {
                    throw new ParsingException(source(fuseConfigurationContext), "Only one GROUP BY can be specified");
                }
                groupByAttr = visitQualifiedName(fuseConfigurationContext.group);
            }
        }

        return groupByAttr == null ? new UnresolvedAttribute(source, Fork.FORK_FIELD) : groupByAttr;
    }

    private List<NamedExpression> visitFuseKeyBy(List<EsqlBaseParser.FuseConfigurationContext> fuseConfigurationContexts, Source source) {
        List<NamedExpression> keys = null;

        for (EsqlBaseParser.FuseConfigurationContext fuseConfigurationContext : fuseConfigurationContexts) {
            if (fuseConfigurationContext.key != null) {
                if (keys != null) {
                    throw new ParsingException(source(fuseConfigurationContext), "Only one KEY BY can be specified");
                }

                keys = visitGrouping(fuseConfigurationContext.key);
            }
        }

        return keys == null
            ? List.of(new UnresolvedAttribute(source, IdFieldMapper.NAME), new UnresolvedAttribute(source, MetadataAttribute.INDEX))
            : keys;
    }

    private MapExpression visitFuseOptions(List<EsqlBaseParser.FuseConfigurationContext> fuseConfigurationContexts) {
        MapExpression options = null;

        for (EsqlBaseParser.FuseConfigurationContext fuseConfigurationContext : fuseConfigurationContexts) {
            if (fuseConfigurationContext.options != null) {
                if (options != null) {
                    throw new ParsingException(source(fuseConfigurationContext), "Only one WITH can be specified");
                }
                options = visitMapExpression(fuseConfigurationContext.options);
            }
        }

        return options;
    }

    @Override
    public PlanFactory visitRerankCommand(EsqlBaseParser.RerankCommandContext ctx) {
        Source source = source(ctx);
        List<Alias> rerankFields = visitRerankFields(ctx.rerankFields());
        Expression queryText = expression(ctx.queryText);
        Attribute scoreAttribute = visitQualifiedName(ctx.targetField, new UnresolvedAttribute(source, MetadataAttribute.SCORE));
        if (scoreAttribute.qualifier() != null) {
            throw qualifiersUnsupportedInFieldDefinitions(scoreAttribute.source(), ctx.targetField.getText());
        }

        return p -> {
            checkForRemoteClusters(p, source, "RERANK");
            return applyRerankOptions(new Rerank(source, p, queryText, rerankFields, scoreAttribute), ctx.commandNamedParameters());
        };
    }

    private Rerank applyRerankOptions(Rerank rerank, EsqlBaseParser.CommandNamedParametersContext ctx) {
        MapExpression optionExpression = visitCommandNamedParameters(ctx);

        if (optionExpression == null) {
            return rerank;
        }

        Map<String, Expression> optionsMap = optionExpression.keyFoldedMap();
        Expression inferenceId = optionsMap.remove(Rerank.INFERENCE_ID_OPTION_NAME);

        if (inferenceId != null) {
            rerank = applyInferenceId(rerank, inferenceId);
        }

        if (optionsMap.isEmpty() == false) {
            throw new ParsingException(
                source(ctx),
                "Inavalid option [{}] in RERANK, expected one of [{}]",
                optionsMap.keySet().stream().findAny().get(),
                rerank.validOptionNames()
            );
        }

        return rerank;
    }

    public PlanFactory visitCompletionCommand(EsqlBaseParser.CompletionCommandContext ctx) {
        Source source = source(ctx);
        Expression prompt = expression(ctx.prompt);
        Attribute targetField = visitQualifiedName(ctx.targetField, new UnresolvedAttribute(source, Completion.DEFAULT_OUTPUT_FIELD_NAME));

        if (targetField.qualifier() != null) {
            throw qualifiersUnsupportedInFieldDefinitions(targetField.source(), ctx.targetField.getText());
        }

        return p -> {
            checkForRemoteClusters(p, source, "COMPLETION");
            return applyCompletionOptions(new Completion(source, p, prompt, targetField), ctx.commandNamedParameters());
        };
    }

    private Completion applyCompletionOptions(Completion completion, EsqlBaseParser.CommandNamedParametersContext ctx) {
        MapExpression optionsExpression = ctx == null ? null : visitCommandNamedParameters(ctx);

        if (optionsExpression == null || optionsExpression.containsKey(Completion.INFERENCE_ID_OPTION_NAME) == false) {
            // Having a mandatory named parameter for inference_id is an antipattern, but it will be optional in the future when we have a
            // default LLM. It is better to keep inference_id as a named parameter and relax the syntax when it will become optional than
            // completely change the syntax in the future.
            throw new ParsingException(
                completion.source(),
                "Missing mandatory option [{}] in COMPLETION",
                Completion.INFERENCE_ID_OPTION_NAME
            );
        }

        Map<String, Expression> optionsMap = optionsExpression.keyFoldedMap();

        Expression inferenceId = optionsMap.remove(Completion.INFERENCE_ID_OPTION_NAME);
        if (inferenceId != null) {
            completion = applyInferenceId(completion, inferenceId);
        }

        if (optionsMap.isEmpty() == false) {
            throw new ParsingException(
                source(ctx),
                "Inavalid option [{}] in COMPLETION, expected one of [{}]",
                optionsMap.keySet().stream().findAny().get(),
                completion.validOptionNames()
            );
        }

        return completion;
    }

    private <InferencePlanType extends InferencePlan<InferencePlanType>> InferencePlanType applyInferenceId(
        InferencePlanType inferencePlan,
        Expression inferenceId
    ) {
        if ((inferenceId instanceof Literal && DataType.isString(inferenceId.dataType())) == false) {
            throw new ParsingException(
                inferenceId.source(),
                "Option [{}] must be a valid string, found [{}]",
                Completion.INFERENCE_ID_OPTION_NAME,
                inferenceId.source().text()
            );
        }

        return inferencePlan.withInferenceId(inferenceId);
    }

    public PlanFactory visitSampleCommand(EsqlBaseParser.SampleCommandContext ctx) {
        Source source = source(ctx);
        Object val = expression(ctx.probability).fold(FoldContext.small() /* TODO remove me */);
        if (val instanceof Double probability && probability > 0.0 && probability < 1.0) {
            return input -> new Sample(source, new Literal(source, probability, DataType.DOUBLE), input);
        } else {
            throw new ParsingException(
                source(ctx),
                "invalid value for SAMPLE probability [" + BytesRefs.toString(val) + "], expecting a number between 0 and 1, exclusive"
            );
        }
    }

    @Override
    public PlanFactory visitPromqlCommand(EsqlBaseParser.PromqlCommandContext ctx) {
        Source source = source(ctx);

        // Check if PromQL functionality is enabled
        if (PromqlFeatures.isEnabled() == false) {
            throw new ParsingException(
                source,
                "PROMQL command is not available. Requires snapshot build with capability [promql_vX] enabled"
            );
        }

        PromqlParams params = parsePromqlParams(ctx, source);

        // TODO: Perform type and value validation
        var queryCtx = ctx.promqlQueryPart();
        if (queryCtx == null || queryCtx.isEmpty()) {
            throw new ParsingException(source, "PromQL expression cannot be empty");
        }

        Token startToken = queryCtx.getFirst().start;
        Token stopToken = queryCtx.getLast().stop;
        // copy the query verbatim to avoid missing tokens interpreted by the enclosing lexer
        String promqlQuery = source(startToken, stopToken).text();
        if (promqlQuery.isBlank()) {
            throw new ParsingException(source, "PromQL expression cannot be empty");
        }

        int promqlStartLine = startToken.getLine();
        int promqlStartColumn = startToken.getCharPositionInLine();

        PromqlParser promqlParser = new PromqlParser();
        LogicalPlan promqlPlan;
        try {
            // The existing PromqlParser is used to parse the inner query
            promqlPlan = promqlParser.createStatement(
                promqlQuery,
                params.startLiteral(),
                params.endLiteral(),
                promqlStartLine,
                promqlStartColumn
            );
        } catch (ParsingException pe) {
            throw PromqlParserUtils.adjustParsingException(pe, promqlStartLine, promqlStartColumn);
        }

        return plan -> new PromqlCommand(
            source,
            plan,
            promqlPlan,
            params.startLiteral(),
            params.endLiteral(),
            params.stepLiteral(),
            new UnresolvedTimestamp(source)
        );
    }

    private PromqlParams parsePromqlParams(EsqlBaseParser.PromqlCommandContext ctx, Source source) {
        Instant time = null;
        Instant start = null;
        Instant end = null;
        Duration step = null;

        Set<String> paramsSeen = new HashSet<>();
        for (EsqlBaseParser.PromqlParamContext paramCtx : ctx.promqlParam()) {
            var paramNameCtx = paramCtx.name;
            String name = parseParamName(paramCtx.name);
            if (paramsSeen.add(name) == false) {
                throw new ParsingException(source(paramNameCtx), "[{}] already specified", name);
            }
            Source valueSource = source(paramCtx.value);
            String valueString = parseParamValue(paramCtx.value);
            switch (name) {
                case TIME -> time = PromqlParserUtils.parseDate(valueSource, valueString);
                case START -> start = PromqlParserUtils.parseDate(valueSource, valueString);
                case END -> end = PromqlParserUtils.parseDate(valueSource, valueString);
                case STEP -> {
                    try {
                        step = Duration.ofSeconds(Integer.parseInt(valueString));
                    } catch (NumberFormatException ignore) {
                        step = PromqlParserUtils.parseDuration(valueSource, valueString);
                    }
                }
                default -> {
                    String message = "Unknown parameter [{}]";
                    List<String> similar = StringUtils.findSimilar(name, PROMQL_ALLOWED_PARAMS);
                    if (CollectionUtils.isEmpty(similar) == false) {
                        message += ", did you mean " + (similar.size() == 1 ? "[" + similar.get(0) + "]" : "any of " + similar) + "?";
                    }
                    throw new ParsingException(source(paramNameCtx), message, name);
                }
            }
        }

        // Validation logic for time parameters
        if (time != null) {
            if (start != null || end != null || step != null) {
                throw new ParsingException(
                    source,
                    "Specify either [{}] for instant query or [{}], [{}] or [{}] for a range query",
                    TIME,
                    STEP,
                    START,
                    END
                );
            }
            start = time;
            end = time;
        } else if (step != null) {
            if (start != null || end != null) {
                if (start == null || end == null) {
                    throw new ParsingException(
                        source,
                        "Parameters [{}] and [{}] must either both be specified or both be omitted for a range query",
                        START,
                        END
                    );
                }
                if (end.isBefore(start)) {
                    throw new ParsingException(
                        source,
                        "invalid parameter \"end\": end timestamp must not be before start time",
                        end,
                        start
                    );
                }
            }
            if (step.isPositive() == false) {
                throw new ParsingException(
                    source,
                    "invalid parameter \"step\": zero or negative query resolution step widths are not accepted. "
                        + "Try a positive integer",
                    step
                );
            }
        } else {
            throw new ParsingException(source, "Parameter [{}] or [{}] is required", STEP, TIME);
        }
        return new PromqlParams(source, start, end, step);
    }

    private String parseParamName(EsqlBaseParser.PromqlParamContentContext ctx) {
        if (ctx.PROMQL_UNQUOTED_IDENTIFIER() != null) {
            return ctx.PROMQL_UNQUOTED_IDENTIFIER().getText();
        } else if (ctx.QUOTED_IDENTIFIER() != null) {
            return AbstractBuilder.unquote(ctx.QUOTED_IDENTIFIER().getText());
        } else {
            throw new ParsingException(source(ctx), "Parameter name [{}] must be an identifier", ctx.getText());
        }
    }

    private String parseParamValue(EsqlBaseParser.PromqlParamContentContext ctx) {
        if (ctx.PROMQL_UNQUOTED_IDENTIFIER() != null) {
            return ctx.PROMQL_UNQUOTED_IDENTIFIER().getText();
        } else if (ctx.QUOTED_STRING() != null) {
            return AbstractBuilder.unquote(ctx.QUOTED_STRING().getText());
        } else if (ctx.NAMED_OR_POSITIONAL_PARAM() != null) {
            QueryParam param = paramByNameOrPosition(ctx.NAMED_OR_POSITIONAL_PARAM());
            return param.value().toString();
        } else if (ctx.QUOTED_IDENTIFIER() != null) {
            throw new ParsingException(source(ctx), "Parameter value [{}] must not be a quoted identifier", ctx.getText());
        } else {
            throw new ParsingException(source(ctx), "Invalid parameter value [{}]", ctx.getText());
        }
    }

    /**
     * Container for PromQL command parameters:
     * <ul>
     *     <li>time for instant queries</li>
     *     <li>start, end, step for range queries</li>
     * </ul>
     * These can be specified in the {@linkplain PromqlCommand PROMQL command} like so:
     * <pre>
     *     # instant query
     *     PROMQL time "2025-10-31T00:00:00Z" (avg(foo))
     *     # range query with explicit start and end
     *     PROMQL start "2025-10-31T00:00:00Z" end "2025-10-31T01:00:00Z" step 1m (avg(foo))
     *     # range query with implicit time bounds, doesn't support calling {@code start()} or {@code end()} functions
     *     PROMQL step 5m (avg(foo))
     * </pre>
     *
     * @see <a href="https://prometheus.io/docs/prometheus/latest/querying/api/#expression-queries">PromQL API documentation</a>
     */
    public record PromqlParams(Source source, Instant start, Instant end, Duration step) {

        public Literal startLiteral() {
            if (start == null) {
                return Literal.NULL;
            }
            return Literal.dateTime(source, start);
        }

        public Literal endLiteral() {
            if (end == null) {
                return Literal.NULL;
            }
            return Literal.dateTime(source, end);
        }

        public Literal stepLiteral() {
            if (step == null) {
                return Literal.NULL;
            }
            return Literal.timeDuration(source, step);
        }
    }
}
