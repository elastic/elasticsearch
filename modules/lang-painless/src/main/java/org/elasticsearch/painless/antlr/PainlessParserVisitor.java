// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.painless.antlr;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link PainlessParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
interface PainlessParserVisitor<T> extends ParseTreeVisitor<T> {
  /**
   * Visit a parse tree produced by {@link PainlessParser#sourceBlock}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSourceBlock(PainlessParser.SourceBlockContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#shortStatementBlock}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitShortStatementBlock(PainlessParser.ShortStatementBlockContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#longStatementBlock}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLongStatementBlock(PainlessParser.LongStatementBlockContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#statementBlock}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitStatementBlock(PainlessParser.StatementBlockContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#emptyStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitEmptyStatement(PainlessParser.EmptyStatementContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#shortStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitShortStatement(PainlessParser.ShortStatementContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#longStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLongStatement(PainlessParser.LongStatementContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#noTrailingStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNoTrailingStatement(PainlessParser.NoTrailingStatementContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#shortIfStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitShortIfStatement(PainlessParser.ShortIfStatementContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#longIfShortElseStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLongIfShortElseStatement(PainlessParser.LongIfShortElseStatementContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#longIfStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLongIfStatement(PainlessParser.LongIfStatementContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#shortWhileStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitShortWhileStatement(PainlessParser.ShortWhileStatementContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#longWhileStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLongWhileStatement(PainlessParser.LongWhileStatementContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#shortForStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitShortForStatement(PainlessParser.ShortForStatementContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#longForStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLongForStatement(PainlessParser.LongForStatementContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#doStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDoStatement(PainlessParser.DoStatementContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#declarationStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDeclarationStatement(PainlessParser.DeclarationStatementContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#continueStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitContinueStatement(PainlessParser.ContinueStatementContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#breakStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBreakStatement(PainlessParser.BreakStatementContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#returnStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitReturnStatement(PainlessParser.ReturnStatementContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#tryStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTryStatement(PainlessParser.TryStatementContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#throwStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitThrowStatement(PainlessParser.ThrowStatementContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#expressionStatement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExpressionStatement(PainlessParser.ExpressionStatementContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#forInitializer}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitForInitializer(PainlessParser.ForInitializerContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#forAfterthought}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitForAfterthought(PainlessParser.ForAfterthoughtContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#declarationType}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDeclarationType(PainlessParser.DeclarationTypeContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#type}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitType(PainlessParser.TypeContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#declarationVariable}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDeclarationVariable(PainlessParser.DeclarationVariableContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#catchBlock}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCatchBlock(PainlessParser.CatchBlockContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#delimiter}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDelimiter(PainlessParser.DelimiterContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#expressions}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExpressions(PainlessParser.ExpressionsContext ctx);
  /**
   * Visit a parse tree produced by the {@code single}
   * labeled alternative in {@link PainlessParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSingle(PainlessParser.SingleContext ctx);
  /**
   * Visit a parse tree produced by the {@code comp}
   * labeled alternative in {@link PainlessParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitComp(PainlessParser.CompContext ctx);
  /**
   * Visit a parse tree produced by the {@code bool}
   * labeled alternative in {@link PainlessParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBool(PainlessParser.BoolContext ctx);
  /**
   * Visit a parse tree produced by the {@code conditional}
   * labeled alternative in {@link PainlessParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitConditional(PainlessParser.ConditionalContext ctx);
  /**
   * Visit a parse tree produced by the {@code binary}
   * labeled alternative in {@link PainlessParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBinary(PainlessParser.BinaryContext ctx);
  /**
   * Visit a parse tree produced by the {@code pre}
   * labeled alternative in {@link PainlessParser#unary}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPre(PainlessParser.PreContext ctx);
  /**
   * Visit a parse tree produced by the {@code post}
   * labeled alternative in {@link PainlessParser#unary}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPost(PainlessParser.PostContext ctx);
  /**
   * Visit a parse tree produced by the {@code read}
   * labeled alternative in {@link PainlessParser#unary}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRead(PainlessParser.ReadContext ctx);
  /**
   * Visit a parse tree produced by the {@code numeric}
   * labeled alternative in {@link PainlessParser#unary}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNumeric(PainlessParser.NumericContext ctx);
  /**
   * Visit a parse tree produced by the {@code true}
   * labeled alternative in {@link PainlessParser#unary}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTrue(PainlessParser.TrueContext ctx);
  /**
   * Visit a parse tree produced by the {@code false}
   * labeled alternative in {@link PainlessParser#unary}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFalse(PainlessParser.FalseContext ctx);
  /**
   * Visit a parse tree produced by the {@code operator}
   * labeled alternative in {@link PainlessParser#unary}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitOperator(PainlessParser.OperatorContext ctx);
  /**
   * Visit a parse tree produced by the {@code cast}
   * labeled alternative in {@link PainlessParser#unary}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCast(PainlessParser.CastContext ctx);
  /**
   * Visit a parse tree produced by the {@code variableprimary}
   * labeled alternative in {@link PainlessParser#chain}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitVariableprimary(PainlessParser.VariableprimaryContext ctx);
  /**
   * Visit a parse tree produced by the {@code typeprimary}
   * labeled alternative in {@link PainlessParser#chain}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTypeprimary(PainlessParser.TypeprimaryContext ctx);
  /**
   * Visit a parse tree produced by the {@code arraycreation}
   * labeled alternative in {@link PainlessParser#chain}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitArraycreation(PainlessParser.ArraycreationContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#leftHandSide}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLeftHandSide(PainlessParser.LeftHandSideContext ctx);
  /**
   * Visit a parse tree produced by the {@code precedence}
   * labeled alternative in {@link PainlessParser#primary}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPrecedence(PainlessParser.PrecedenceContext ctx);
  /**
   * Visit a parse tree produced by the {@code string}
   * labeled alternative in {@link PainlessParser#primary}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitString(PainlessParser.StringContext ctx);
  /**
   * Visit a parse tree produced by the {@code variable}
   * labeled alternative in {@link PainlessParser#primary}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitVariable(PainlessParser.VariableContext ctx);
  /**
   * Visit a parse tree produced by the {@code newobject}
   * labeled alternative in {@link PainlessParser#primary}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNewobject(PainlessParser.NewobjectContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#newarray}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNewarray(PainlessParser.NewarrayContext ctx);
  /**
   * Visit a parse tree produced by the {@code callinvoke}
   * labeled alternative in {@link PainlessParser#secondary}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCallinvoke(PainlessParser.CallinvokeContext ctx);
  /**
   * Visit a parse tree produced by the {@code fieldaccess}
   * labeled alternative in {@link PainlessParser#secondary}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFieldaccess(PainlessParser.FieldaccessContext ctx);
  /**
   * Visit a parse tree produced by the {@code arrayaccess}
   * labeled alternative in {@link PainlessParser#secondary}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitArrayaccess(PainlessParser.ArrayaccessContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#arguments}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitArguments(PainlessParser.ArgumentsContext ctx);
}
