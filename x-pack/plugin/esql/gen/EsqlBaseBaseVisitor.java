// Generated from /Users/lukas/elasticsearch-internal/x-pack/plugin/esql/src/main/antlr/EsqlBase.g4 by ANTLR 4.9.2

    package org.elasticsearch.xpack.esql;

import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;

/**
 * This class provides an empty implementation of {@link EsqlBaseVisitor},
 * which can be extended to create a visitor which only needs to handle a subset
 * of the available methods.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public class EsqlBaseBaseVisitor<T> extends AbstractParseTreeVisitor<T> implements EsqlBaseVisitor<T> {
	/**
	 * {@inheritDoc}
	 *
	 * <p>The default implementation returns the result of calling
	 * {@link #visitChildren} on {@code ctx}.</p>
	 */
	@Override public T visitExpr(EsqlBaseParser.ExprContext ctx) { return visitChildren(ctx); }
}