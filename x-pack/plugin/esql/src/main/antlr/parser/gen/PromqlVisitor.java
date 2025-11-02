// Generated from /Users/felixbarnsteiner/projects/github/elastic/elasticsearch/x-pack/plugin/esql/src/main/antlr/parser/Promql.g4 by ANTLR 4.13.2
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link Promql}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface PromqlVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link Promql#promqlCommand}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPromqlCommand(Promql.PromqlCommandContext ctx);
	/**
	 * Visit a parse tree produced by {@link Promql#promqlParam}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPromqlParam(Promql.PromqlParamContext ctx);
	/**
	 * Visit a parse tree produced by {@link Promql#promqlParamContent}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPromqlParamContent(Promql.PromqlParamContentContext ctx);
	/**
	 * Visit a parse tree produced by {@link Promql#promqlQueryPart}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPromqlQueryPart(Promql.PromqlQueryPartContext ctx);
}