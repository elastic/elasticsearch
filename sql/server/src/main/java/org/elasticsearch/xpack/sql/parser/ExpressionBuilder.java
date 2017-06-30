/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.xpack.sql.expression.Alias;
import org.elasticsearch.xpack.sql.expression.Exists;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.Order;
import org.elasticsearch.xpack.sql.expression.ScalarSubquery;
import org.elasticsearch.xpack.sql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.sql.expression.UnresolvedStar;
import org.elasticsearch.xpack.sql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.Cast;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.Extract;
import org.elasticsearch.xpack.sql.expression.predicate.And;
import org.elasticsearch.xpack.sql.expression.predicate.Equals;
import org.elasticsearch.xpack.sql.expression.predicate.GreaterThan;
import org.elasticsearch.xpack.sql.expression.predicate.GreaterThanOrEqual;
import org.elasticsearch.xpack.sql.expression.predicate.In;
import org.elasticsearch.xpack.sql.expression.predicate.IsNull;
import org.elasticsearch.xpack.sql.expression.predicate.LessThan;
import org.elasticsearch.xpack.sql.expression.predicate.LessThanOrEqual;
import org.elasticsearch.xpack.sql.expression.predicate.Not;
import org.elasticsearch.xpack.sql.expression.predicate.Or;
import org.elasticsearch.xpack.sql.expression.predicate.Range;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.MatchQueryPredicate;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.MultiMatchQueryPredicate;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.StringQueryPredicate;
import org.elasticsearch.xpack.sql.expression.regex.Like;
import org.elasticsearch.xpack.sql.expression.regex.RLike;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.BooleanLiteralContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.CastContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ColumnExpressionContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ColumnReferenceContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ComparisonContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.DecimalLiteralContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.DereferenceContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ExistsContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ExtractContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.FunctionCallContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.IntegerLiteralContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.LogicalBinaryContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.LogicalNotContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.MatchQueryContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.MultiMatchQueryContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.NullLiteralContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.OrderByContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.ParenthesizedExpressionContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.PredicateContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.PredicatedContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.PrimitiveDataTypeContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.SelectExpressionContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.SingleExpressionContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.StarContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.StringLiteralContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.StringQueryContext;
import org.elasticsearch.xpack.sql.parser.SqlBaseParser.SubqueryExpressionContext;
import org.elasticsearch.xpack.sql.plan.TableIdentifier;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static java.lang.String.format;

abstract class ExpressionBuilder extends IdentifierBuilder {
    /**
     * Time zone in which to execute the query. Used by date time
     * functions and the rounding in date histograms.
     */
    private final TimeZone timeZone;

    ExpressionBuilder(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    /**
     * Time zone in which to execute the query. Used by date time
     * functions and the rounding in date histograms.
     */
    protected TimeZone timeZone() {
        return timeZone;
    }

    protected Expression expression(ParseTree ctx) {
        return typedParsing(ctx, Expression.class);
    }

    protected List<Expression> expressions(List<? extends ParserRuleContext> contexts) {
        return visitList(contexts, Expression.class);
    }

    @Override
    public Expression visitSingleExpression(SingleExpressionContext ctx) {
        return expression(ctx.expression());
    }


    @Override
    // Star will be resolved by the analyzer - currently just put a placeholder
    public Expression visitStar(StarContext ctx) {
        return new UnresolvedStar(source(ctx), ctx.qualifier != null ? visitColumnExpression(ctx.qualifier) : null);
    }
    

    @Override
    public Expression visitSelectExpression(SelectExpressionContext ctx) {
        Expression exp = expression(ctx.expression());
        String alias = visitIdentifier(ctx.identifier());
        if (alias != null) {
            exp = new Alias(source(ctx), alias, exp);
        }
        return exp;
    }

    @Override
    public Object visitDereference(DereferenceContext ctx) {
        String fieldName = visitIdentifier(ctx.fieldName);
        String qualifier = null;
        Expression base = expression(ctx.base);
        if (base != null) {
            if (base instanceof UnresolvedAttribute) {
                UnresolvedAttribute b = (UnresolvedAttribute) base;
                return new UnresolvedAttribute(source(ctx), b.name() + "." + fieldName, b.qualifier());
            }
            else {
                throw new UnsupportedOperationException(format(Locale.ROOT, "Uknown dereferencing using %s ", base.getClass()));
            }
        }
        return new UnresolvedAttribute(source(ctx), fieldName, qualifier);
    }

    @Override
    public UnresolvedAttribute visitColumnExpression(ColumnExpressionContext ctx) {
        String qualifier = null;
        if (ctx.alias != null) {
            qualifier = visitIdentifier(ctx.alias);
        }
        else if (ctx.table != null) {
            TableIdentifier table = visitTableIdentifier(ctx.table);
            qualifier = table.index() + "." + table.type();
        }
        return new UnresolvedAttribute(source(ctx), visitIdentifier(ctx.name), qualifier);
    }

    @Override
    public Object visitColumnReference(ColumnReferenceContext ctx) {
        return visitColumnExpression(ctx.columnExpression());
    }

    @Override
    public Expression visitExists(ExistsContext ctx) {
        return new Exists(source(ctx), plan(ctx.query()));
    }

    @Override
    public Expression visitComparison(ComparisonContext ctx) {
        Expression left = expression(ctx.left);
        Expression right = expression(ctx.right);
        TerminalNode op = (TerminalNode) ctx.comparisonOperator().getChild(0);

        Location loc = source(ctx);

        switch (op.getSymbol().getType()) {
            case SqlBaseParser.EQ:
                return new Equals(loc, left, right);
            case SqlBaseParser.NEQ:
                return new Not(loc, new Equals(loc, left, right));
            case SqlBaseParser.LT:
                return new LessThan(loc, left, right);
            case SqlBaseParser.LTE:
                return new LessThanOrEqual(loc, left, right);
            case SqlBaseParser.GT:
                return new GreaterThan(loc, left, right);
            case SqlBaseParser.GTE:
                return new GreaterThanOrEqual(loc, left, right);
            default:
                throw new ParsingException(loc, "Unknown operator %s", op.getSymbol().getText());
        }
    }

    @Override
    public Expression visitPredicated(PredicatedContext ctx) {
        Expression exp = expression(ctx.valueExpression());
        
        if (ctx.predicate() != null) {
            PredicateContext pCtx = ctx.predicate();
            Location loc = source(pCtx);

            Expression e = null;
            switch (pCtx.kind.getType()) {
                case SqlBaseParser.BETWEEN:
                    e = new Range(loc, exp, expression(pCtx.lower), true, expression(pCtx.upper), true);
                    break;
                case SqlBaseParser.IN:
                    if (pCtx.query() != null) {
                        throw new ParsingException(loc, "IN query not supported yet");
                    }
                    e = new In(loc, exp, expressions(pCtx.expression()));
                    break;
                case SqlBaseParser.LIKE:
                    e = new Like(loc, exp, expression(pCtx.pattern));
                    break;
                case SqlBaseParser.RLIKE:
                    e = new RLike(loc, exp, expression(pCtx.pattern));
                    break;
                case SqlBaseParser.NULL:;
                    e = new IsNull(loc, exp);
                    break;
                default:
                    throw new ParsingException(loc, "Unknown predicate %s", pCtx.kind.getText());
            }
            
            exp = pCtx.NOT() != null ? new Not(loc, e) : e;
        }
        
        return exp;
    }

    //
    // Full-text search predicates
    // 
    
    @Override
    public Object visitStringQuery(StringQueryContext ctx) {
        return new StringQueryPredicate(source(ctx), text(ctx.queryString), text(ctx.options));
    }

    @Override
    public Object visitMatchQuery(MatchQueryContext ctx) {
        return new MatchQueryPredicate(source(ctx), new UnresolvedAttribute(source(ctx.singleField), visitQualifiedName(ctx.singleField)), text(ctx.queryString), text(ctx.options));
    }

    @Override
    public Object visitMultiMatchQuery(MultiMatchQueryContext ctx) {
        return new MultiMatchQueryPredicate(source(ctx), text(ctx.multiFields), text(ctx.queryString), text(ctx.options));
    }

    @Override
    public Order visitOrderBy(OrderByContext ctx) {
        return new Order(source(ctx), expression(ctx.expression()), (ctx.DESC() != null ? Order.OrderDirection.DESC : Order.OrderDirection.ASC));
    }

    @Override
    public Object visitCast(CastContext ctx) {
        return new Cast(source(ctx), expression(ctx.expression()), typedParsing(ctx.dataType(), DataType.class));
    }

    @Override
    public DataType visitPrimitiveDataType(PrimitiveDataTypeContext ctx) {
        String type = visitIdentifier(ctx.identifier()).toLowerCase(Locale.ROOT);

        switch (type) {
            case "bit":
            case "bool":
            case "boolean":
                return DataTypes.BOOLEAN;
            case "tinyint":
            case "byte":
                return DataTypes.BYTE;
            case "smallint":
            case "short":
                return DataTypes.SHORT;
            case "int":
            case "integer":
                return DataTypes.INTEGER;
            case "long":
            case "bigint":
                return DataTypes.LONG;
            case "real":
                return DataTypes.FLOAT;
            case "float":
            case "double":
                return DataTypes.DOUBLE;
            case "date":
            case "timestamp":
                return DataTypes.DATE;
            case "char":
            case "varchar":
            case "string":
                return DataTypes.KEYWORD;
            default:
                throw new ParsingException(source(ctx), "Does not recognize type %s", type);
        }
    }

    @Override
    public Object visitFunctionCall(FunctionCallContext ctx) {
        String name = visitIdentifier(ctx.identifier());
        boolean isDistinct = false;
        if (ctx.setQuantifier() != null) {
            isDistinct = (ctx.setQuantifier().DISTINCT() != null);
        }
        return new UnresolvedFunction(source(ctx), name, isDistinct, timeZone, expressions(ctx.expression()));
    }

    @Override
    public Object visitExtract(ExtractContext ctx) {
        Location source = source(ctx);
        String fieldString = visitIdentifier(ctx.field);
        Extract extract = null;
        try {
            extract = Extract.valueOf(fieldString.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            throw new ParsingException(source, format(Locale.ROOT, "Invalid EXTRACT field %s", fieldString));
        }
        return extract.toFunction(source, expression(ctx.valueExpression()), timeZone);
    }

    @Override
    public Expression visitSubqueryExpression(SubqueryExpressionContext ctx) {
        return new ScalarSubquery(source(ctx), plan(ctx.query()));
    }

    @Override
    public Expression visitParenthesizedExpression(ParenthesizedExpressionContext ctx) {
        return expression(ctx.expression());
    }


    //
    // Logical constructs
    //

    @Override
    public Object visitLogicalNot(LogicalNotContext ctx) {
        return new Not(source(ctx), expression(ctx.booleanExpression()));
    }

    @Override
    public Object visitLogicalBinary(LogicalBinaryContext ctx) {
        int type = ctx.operator.getType();
        Location loc = source(ctx);
        Expression left = expression(ctx.left);
        Expression right = expression(ctx.right);

        if (type == SqlBaseParser.AND) {
            return new And(loc, left, right);
        }
        if (type == SqlBaseParser.OR) {
            return new Or(loc, left, right);
        }
        throw new ParsingException(loc, format(Locale.ROOT, "Don't know how to parse %s", ctx));
    }


    //
    // Literal
    //


    @Override
    public Expression visitNullLiteral(NullLiteralContext ctx) {
        return new Literal(source(ctx), null, DataTypes.NULL);
    }

    @Override
    public Expression visitBooleanLiteral(BooleanLiteralContext ctx) {
        return new Literal(source(ctx), Booleans.parseBoolean(ctx.getText().toLowerCase(Locale.ROOT), false), DataTypes.BOOLEAN);
    }

    @Override
    public Expression visitStringLiteral(StringLiteralContext ctx) {
        return new Literal(source(ctx), ctx.STRING().stream().map(AbstractBuilder::text).collect(Collectors.joining()), DataTypes.KEYWORD);
    }

    @Override
    public Object visitDecimalLiteral(DecimalLiteralContext ctx) {
        return new Literal(source(ctx), new BigDecimal(ctx.getText()).doubleValue(), DataTypes.DOUBLE);
    }

    @Override
    public Object visitIntegerLiteral(IntegerLiteralContext ctx) {
        BigDecimal bigD = new BigDecimal(ctx.getText());
        // TODO: this can be improved to use the smallest type available
        return new Literal(source(ctx), bigD.longValueExact(), DataTypes.INTEGER);
    }
}