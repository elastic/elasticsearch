// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.plan.a;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link PlanAParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
interface PlanAParserVisitor<T> extends ParseTreeVisitor<T> {
  /**
   * Visit a parse tree produced by {@link PlanAParser#source}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSource(PlanAParser.SourceContext ctx);
  /**
   * Visit a parse tree produced by the {@code if}
   * labeled alternative in {@link PlanAParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIf(PlanAParser.IfContext ctx);
  /**
   * Visit a parse tree produced by the {@code while}
   * labeled alternative in {@link PlanAParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitWhile(PlanAParser.WhileContext ctx);
  /**
   * Visit a parse tree produced by the {@code do}
   * labeled alternative in {@link PlanAParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDo(PlanAParser.DoContext ctx);
  /**
   * Visit a parse tree produced by the {@code for}
   * labeled alternative in {@link PlanAParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFor(PlanAParser.ForContext ctx);
  /**
   * Visit a parse tree produced by the {@code decl}
   * labeled alternative in {@link PlanAParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDecl(PlanAParser.DeclContext ctx);
  /**
   * Visit a parse tree produced by the {@code continue}
   * labeled alternative in {@link PlanAParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitContinue(PlanAParser.ContinueContext ctx);
  /**
   * Visit a parse tree produced by the {@code break}
   * labeled alternative in {@link PlanAParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBreak(PlanAParser.BreakContext ctx);
  /**
   * Visit a parse tree produced by the {@code return}
   * labeled alternative in {@link PlanAParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitReturn(PlanAParser.ReturnContext ctx);
  /**
   * Visit a parse tree produced by the {@code try}
   * labeled alternative in {@link PlanAParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTry(PlanAParser.TryContext ctx);
  /**
   * Visit a parse tree produced by the {@code throw}
   * labeled alternative in {@link PlanAParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitThrow(PlanAParser.ThrowContext ctx);
  /**
   * Visit a parse tree produced by the {@code expr}
   * labeled alternative in {@link PlanAParser#statement}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExpr(PlanAParser.ExprContext ctx);
  /**
   * Visit a parse tree produced by the {@code multiple}
   * labeled alternative in {@link PlanAParser#block}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMultiple(PlanAParser.MultipleContext ctx);
  /**
   * Visit a parse tree produced by the {@code single}
   * labeled alternative in {@link PlanAParser#block}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSingle(PlanAParser.SingleContext ctx);
  /**
   * Visit a parse tree produced by {@link PlanAParser#empty}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitEmpty(PlanAParser.EmptyContext ctx);
  /**
   * Visit a parse tree produced by {@link PlanAParser#initializer}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitInitializer(PlanAParser.InitializerContext ctx);
  /**
   * Visit a parse tree produced by {@link PlanAParser#afterthought}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitAfterthought(PlanAParser.AfterthoughtContext ctx);
  /**
   * Visit a parse tree produced by {@link PlanAParser#declaration}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDeclaration(PlanAParser.DeclarationContext ctx);
  /**
   * Visit a parse tree produced by {@link PlanAParser#decltype}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDecltype(PlanAParser.DecltypeContext ctx);
  /**
   * Visit a parse tree produced by {@link PlanAParser#declvar}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDeclvar(PlanAParser.DeclvarContext ctx);
  /**
   * Visit a parse tree produced by the {@code comp}
   * labeled alternative in {@link PlanAParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitComp(PlanAParser.CompContext ctx);
  /**
   * Visit a parse tree produced by the {@code bool}
   * labeled alternative in {@link PlanAParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBool(PlanAParser.BoolContext ctx);
  /**
   * Visit a parse tree produced by the {@code conditional}
   * labeled alternative in {@link PlanAParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitConditional(PlanAParser.ConditionalContext ctx);
  /**
   * Visit a parse tree produced by the {@code assignment}
   * labeled alternative in {@link PlanAParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitAssignment(PlanAParser.AssignmentContext ctx);
  /**
   * Visit a parse tree produced by the {@code false}
   * labeled alternative in {@link PlanAParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFalse(PlanAParser.FalseContext ctx);
  /**
   * Visit a parse tree produced by the {@code numeric}
   * labeled alternative in {@link PlanAParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNumeric(PlanAParser.NumericContext ctx);
  /**
   * Visit a parse tree produced by the {@code unary}
   * labeled alternative in {@link PlanAParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitUnary(PlanAParser.UnaryContext ctx);
  /**
   * Visit a parse tree produced by the {@code precedence}
   * labeled alternative in {@link PlanAParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPrecedence(PlanAParser.PrecedenceContext ctx);
  /**
   * Visit a parse tree produced by the {@code preinc}
   * labeled alternative in {@link PlanAParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPreinc(PlanAParser.PreincContext ctx);
  /**
   * Visit a parse tree produced by the {@code postinc}
   * labeled alternative in {@link PlanAParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPostinc(PlanAParser.PostincContext ctx);
  /**
   * Visit a parse tree produced by the {@code cast}
   * labeled alternative in {@link PlanAParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCast(PlanAParser.CastContext ctx);
  /**
   * Visit a parse tree produced by the {@code external}
   * labeled alternative in {@link PlanAParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExternal(PlanAParser.ExternalContext ctx);
  /**
   * Visit a parse tree produced by the {@code null}
   * labeled alternative in {@link PlanAParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNull(PlanAParser.NullContext ctx);
  /**
   * Visit a parse tree produced by the {@code binary}
   * labeled alternative in {@link PlanAParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBinary(PlanAParser.BinaryContext ctx);
  /**
   * Visit a parse tree produced by the {@code char}
   * labeled alternative in {@link PlanAParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitChar(PlanAParser.CharContext ctx);
  /**
   * Visit a parse tree produced by the {@code true}
   * labeled alternative in {@link PlanAParser#expression}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTrue(PlanAParser.TrueContext ctx);
  /**
   * Visit a parse tree produced by {@link PlanAParser#extstart}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtstart(PlanAParser.ExtstartContext ctx);
  /**
   * Visit a parse tree produced by {@link PlanAParser#extprec}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtprec(PlanAParser.ExtprecContext ctx);
  /**
   * Visit a parse tree produced by {@link PlanAParser#extcast}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtcast(PlanAParser.ExtcastContext ctx);
  /**
   * Visit a parse tree produced by {@link PlanAParser#extbrace}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtbrace(PlanAParser.ExtbraceContext ctx);
  /**
   * Visit a parse tree produced by {@link PlanAParser#extdot}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtdot(PlanAParser.ExtdotContext ctx);
  /**
   * Visit a parse tree produced by {@link PlanAParser#exttype}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExttype(PlanAParser.ExttypeContext ctx);
  /**
   * Visit a parse tree produced by {@link PlanAParser#extcall}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtcall(PlanAParser.ExtcallContext ctx);
  /**
   * Visit a parse tree produced by {@link PlanAParser#extvar}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtvar(PlanAParser.ExtvarContext ctx);
  /**
   * Visit a parse tree produced by {@link PlanAParser#extfield}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtfield(PlanAParser.ExtfieldContext ctx);
  /**
   * Visit a parse tree produced by {@link PlanAParser#extnew}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtnew(PlanAParser.ExtnewContext ctx);
  /**
   * Visit a parse tree produced by {@link PlanAParser#extstring}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtstring(PlanAParser.ExtstringContext ctx);
  /**
   * Visit a parse tree produced by {@link PlanAParser#arguments}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitArguments(PlanAParser.ArgumentsContext ctx);
  /**
   * Visit a parse tree produced by {@link PlanAParser#increment}.
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIncrement(PlanAParser.IncrementContext ctx);
}
