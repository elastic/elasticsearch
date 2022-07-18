// Generated from /Users/lukas/elasticsearch-internal/x-pack/plugin/esql/src/main/antlr/EsqlBase.g4 by ANTLR 4.9.2

    package org.elasticsearch.xpack.esql;

import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link EsqlBaseParser}.
 */
public interface EsqlBaseListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link EsqlBaseParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterExpr(EsqlBaseParser.ExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link EsqlBaseParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitExpr(EsqlBaseParser.ExprContext ctx);
}