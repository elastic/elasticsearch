// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.painless;
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
   * Visit a parse tree produced by {@link PainlessParser#source}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSource(PainlessParser.SourceContext ctx);
  /**
   * Visit a parse tree produced by the {@code if}
   * labeled alternative in {@link PainlessParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIf(PainlessParser.IfContext ctx);
  /**
   * Visit a parse tree produced by the {@code while}
   * labeled alternative in {@link PainlessParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitWhile(PainlessParser.WhileContext ctx);
  /**
   * Visit a parse tree produced by the {@code do}
   * labeled alternative in {@link PainlessParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDo(PainlessParser.DoContext ctx);
  /**
   * Visit a parse tree produced by the {@code for}
   * labeled alternative in {@link PainlessParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFor(PainlessParser.ForContext ctx);
  /**
   * Visit a parse tree produced by the {@code decl}
   * labeled alternative in {@link PainlessParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDecl(PainlessParser.DeclContext ctx);
  /**
   * Visit a parse tree produced by the {@code continue}
   * labeled alternative in {@link PainlessParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitContinue(PainlessParser.ContinueContext ctx);
  /**
   * Visit a parse tree produced by the {@code break}
   * labeled alternative in {@link PainlessParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBreak(PainlessParser.BreakContext ctx);
  /**
   * Visit a parse tree produced by the {@code return}
   * labeled alternative in {@link PainlessParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitReturn(PainlessParser.ReturnContext ctx);
  /**
   * Visit a parse tree produced by the {@code try}
   * labeled alternative in {@link PainlessParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTry(PainlessParser.TryContext ctx);
  /**
   * Visit a parse tree produced by the {@code throw}
   * labeled alternative in {@link PainlessParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitThrow(PainlessParser.ThrowContext ctx);
  /**
   * Visit a parse tree produced by the {@code expr}
   * labeled alternative in {@link PainlessParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExpr(PainlessParser.ExprContext ctx);
  /**
   * Visit a parse tree produced by the {@code multiple}
   * labeled alternative in {@link PainlessParser#block}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMultiple(PainlessParser.MultipleContext ctx);
  /**
   * Visit a parse tree produced by the {@code single}
   * labeled alternative in {@link PainlessParser#block}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSingle(PainlessParser.SingleContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#empty}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitEmpty(PainlessParser.EmptyContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#emptyscope}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitEmptyscope(PainlessParser.EmptyscopeContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#initializer}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitInitializer(PainlessParser.InitializerContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#afterthought}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitAfterthought(PainlessParser.AfterthoughtContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#declaration}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDeclaration(PainlessParser.DeclarationContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#decltype}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDecltype(PainlessParser.DecltypeContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#declvar}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDeclvar(PainlessParser.DeclvarContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#trap}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTrap(PainlessParser.TrapContext ctx);
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
   * Visit a parse tree produced by the {@code external}
   * labeled alternative in {@link PainlessParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExternal(PainlessParser.ExternalContext ctx);
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
   * Visit a parse tree produced by the {@code char}
   * labeled alternative in {@link PainlessParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitChar(PainlessParser.CharContext ctx);
  /**
   * Visit a parse tree produced by the {@code true}
   * labeled alternative in {@link PainlessParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTrue(PainlessParser.TrueContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#extstart}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtstart(PainlessParser.ExtstartContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#extprec}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtprec(PainlessParser.ExtprecContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#extcast}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtcast(PainlessParser.ExtcastContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#extbrace}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtbrace(PainlessParser.ExtbraceContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#extdot}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtdot(PainlessParser.ExtdotContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#exttype}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExttype(PainlessParser.ExttypeContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#extcall}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtcall(PainlessParser.ExtcallContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#extvar}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtvar(PainlessParser.ExtvarContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#extfield}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtfield(PainlessParser.ExtfieldContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#extnew}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtnew(PainlessParser.ExtnewContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#extstring}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtstring(PainlessParser.ExtstringContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#arguments}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitArguments(PainlessParser.ArgumentsContext ctx);
  /**
   * Visit a parse tree produced by {@link PainlessParser#increment}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIncrement(PainlessParser.IncrementContext ctx);
}
