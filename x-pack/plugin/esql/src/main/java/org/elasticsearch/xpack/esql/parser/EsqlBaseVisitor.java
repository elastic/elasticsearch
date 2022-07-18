// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.esql.parser;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link EsqlBaseParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
interface EsqlBaseVisitor<T> extends ParseTreeVisitor<T> {
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitStatement(EsqlBaseParser.StatementContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#query}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitQuery(EsqlBaseParser.QueryContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#sourceCmd}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSourceCmd(EsqlBaseParser.SourceCmdContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#rowCmd}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRowCmd(EsqlBaseParser.RowCmdContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#fields}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFields(EsqlBaseParser.FieldsContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#field}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitField(EsqlBaseParser.FieldContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExpression(EsqlBaseParser.ExpressionContext ctx);
  /**
   * Visit a parse tree produced by {@link EsqlBaseParser#identifier}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIdentifier(EsqlBaseParser.IdentifierContext ctx);
}
