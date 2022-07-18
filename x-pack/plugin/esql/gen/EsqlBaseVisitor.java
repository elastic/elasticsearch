// Generated from /Users/lukas/elasticsearch-internal/x-pack/plugin/esql/src/main/antlr/EsqlBase.g4 by ANTLR 4.9.2

    package org.elasticsearch.xpack.esql;

import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link EsqlBaseParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface EsqlBaseVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link EsqlBaseParser#expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpr(EsqlBaseParser.ExprContext ctx);
}