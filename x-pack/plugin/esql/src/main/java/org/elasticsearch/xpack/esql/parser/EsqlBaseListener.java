// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.esql.parser;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link EsqlBaseParser}.
 */
interface EsqlBaseListener extends ParseTreeListener {
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#statement}.
   * @param ctx the parse tree
   */
  void enterStatement(EsqlBaseParser.StatementContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#statement}.
   * @param ctx the parse tree
   */
  void exitStatement(EsqlBaseParser.StatementContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#query}.
   * @param ctx the parse tree
   */
  void enterQuery(EsqlBaseParser.QueryContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#query}.
   * @param ctx the parse tree
   */
  void exitQuery(EsqlBaseParser.QueryContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#sourceCmd}.
   * @param ctx the parse tree
   */
  void enterSourceCmd(EsqlBaseParser.SourceCmdContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#sourceCmd}.
   * @param ctx the parse tree
   */
  void exitSourceCmd(EsqlBaseParser.SourceCmdContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#rowCmd}.
   * @param ctx the parse tree
   */
  void enterRowCmd(EsqlBaseParser.RowCmdContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#rowCmd}.
   * @param ctx the parse tree
   */
  void exitRowCmd(EsqlBaseParser.RowCmdContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#fields}.
   * @param ctx the parse tree
   */
  void enterFields(EsqlBaseParser.FieldsContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#fields}.
   * @param ctx the parse tree
   */
  void exitFields(EsqlBaseParser.FieldsContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#field}.
   * @param ctx the parse tree
   */
  void enterField(EsqlBaseParser.FieldContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#field}.
   * @param ctx the parse tree
   */
  void exitField(EsqlBaseParser.FieldContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#expression}.
   * @param ctx the parse tree
   */
  void enterExpression(EsqlBaseParser.ExpressionContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#expression}.
   * @param ctx the parse tree
   */
  void exitExpression(EsqlBaseParser.ExpressionContext ctx);
  /**
   * Enter a parse tree produced by {@link EsqlBaseParser#identifier}.
   * @param ctx the parse tree
   */
  void enterIdentifier(EsqlBaseParser.IdentifierContext ctx);
  /**
   * Exit a parse tree produced by {@link EsqlBaseParser#identifier}.
   * @param ctx the parse tree
   */
  void exitIdentifier(EsqlBaseParser.IdentifierContext ctx);
}
