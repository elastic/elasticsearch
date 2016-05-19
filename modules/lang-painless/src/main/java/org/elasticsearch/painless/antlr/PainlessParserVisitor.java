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
   * Visit a parse tree produced by the {@code comp}
   * labeled alternative in {@link PainlessParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitComp(PainlessParser.CompContext ctx);
  /**
   * Visit a parse tree produced by the {@code read}
   * labeled alternative in {@link PainlessParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRead(PainlessParser.ReadContext ctx);
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
   * Visit a parse tree produced by the {@code assignment}
   * labeled alternative in {@link PainlessParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitAssignment(PainlessParser.AssignmentContext ctx);
  /**
   * Visit a parse tree produced by the {@code false}
   * labeled alternative in {@link PainlessParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFalse(PainlessParser.FalseContext ctx);
  /**
   * Visit a parse tree produced by the {@code numeric}
   * labeled alternative in {@link PainlessParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNumeric(PainlessParser.NumericContext ctx);
  /**
   * Visit a parse tree produced by the {@code unary}
   * labeled alternative in {@link PainlessParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitUnary(PainlessParser.UnaryContext ctx);
  /**
   * Visit a parse tree produced by the {@code precedence}
   * labeled alternative in {@link PainlessParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPrecedence(PainlessParser.PrecedenceContext ctx);
  /**
   * Visit a parse tree produced by the {@code preinc}
   * labeled alternative in {@link PainlessParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPreinc(PainlessParser.PreincContext ctx);
  /**
   * Visit a parse tree produced by the {@code postinc}
   * labeled alternative in {@link PainlessParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPostinc(PainlessParser.PostincContext ctx);
  /**
   * Visit a parse tree produced by the {@code cast}
   * labeled alternative in {@link PainlessParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCast(PainlessParser.CastContext ctx);
  /**
   * Visit a parse tree produced by the {@code null}
   * labeled alternative in {@link PainlessParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNull(PainlessParser.NullContext ctx);
  /**
   * Visit a parse tree produced by the {@code binary}
   * labeled alternative in {@link PainlessParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBinary(PainlessParser.BinaryContext ctx);
  /**
   * Visit a parse tree produced by the {@code true}
   * labeled alternative in {@link PainlessParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTrue(PainlessParser.TrueContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#chain}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitChain(PainlessParser.ChainContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#linkprec}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLinkprec(PainlessParser.LinkprecContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#linkcast}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLinkcast(PainlessParser.LinkcastContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#linkbrace}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLinkbrace(PainlessParser.LinkbraceContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#linkdot}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLinkdot(PainlessParser.LinkdotContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#linkcall}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLinkcall(PainlessParser.LinkcallContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#linkvar}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLinkvar(PainlessParser.LinkvarContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#linkfield}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLinkfield(PainlessParser.LinkfieldContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#linknew}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLinknew(PainlessParser.LinknewContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#linkstring}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLinkstring(PainlessParser.LinkstringContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#arguments}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitArguments(PainlessParser.ArgumentsContext ctx);
}
