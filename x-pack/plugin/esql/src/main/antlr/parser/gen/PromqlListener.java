// Generated from /Users/felixbarnsteiner/projects/github/elastic/elasticsearch/x-pack/plugin/esql/src/main/antlr/parser/Promql.g4 by ANTLR 4.13.2
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link Promql}.
 */
public interface PromqlListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link Promql#promqlCommand}.
	 * @param ctx the parse tree
	 */
	void enterPromqlCommand(Promql.PromqlCommandContext ctx);
	/**
	 * Exit a parse tree produced by {@link Promql#promqlCommand}.
	 * @param ctx the parse tree
	 */
	void exitPromqlCommand(Promql.PromqlCommandContext ctx);
	/**
	 * Enter a parse tree produced by {@link Promql#promqlParam}.
	 * @param ctx the parse tree
	 */
	void enterPromqlParam(Promql.PromqlParamContext ctx);
	/**
	 * Exit a parse tree produced by {@link Promql#promqlParam}.
	 * @param ctx the parse tree
	 */
	void exitPromqlParam(Promql.PromqlParamContext ctx);
	/**
	 * Enter a parse tree produced by {@link Promql#promqlParamContent}.
	 * @param ctx the parse tree
	 */
	void enterPromqlParamContent(Promql.PromqlParamContentContext ctx);
	/**
	 * Exit a parse tree produced by {@link Promql#promqlParamContent}.
	 * @param ctx the parse tree
	 */
	void exitPromqlParamContent(Promql.PromqlParamContentContext ctx);
	/**
	 * Enter a parse tree produced by {@link Promql#promqlQueryPart}.
	 * @param ctx the parse tree
	 */
	void enterPromqlQueryPart(Promql.PromqlQueryPartContext ctx);
	/**
	 * Exit a parse tree produced by {@link Promql#promqlQueryPart}.
	 * @param ctx the parse tree
	 */
	void exitPromqlQueryPart(Promql.PromqlQueryPartContext ctx);
}