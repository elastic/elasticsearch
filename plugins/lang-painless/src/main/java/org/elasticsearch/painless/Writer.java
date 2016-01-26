/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.elasticsearch.painless.Definition.Cast;
import org.elasticsearch.painless.Definition.Constructor;
import org.elasticsearch.painless.Definition.Field;
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Transform;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.script.ScoreAccessor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.painless.PainlessParser.ADD;
import static org.elasticsearch.painless.PainlessParser.AfterthoughtContext;
import static org.elasticsearch.painless.PainlessParser.ArgumentsContext;
import static org.elasticsearch.painless.PainlessParser.AssignmentContext;
import static org.elasticsearch.painless.PainlessParser.BWAND;
import static org.elasticsearch.painless.PainlessParser.BWOR;
import static org.elasticsearch.painless.PainlessParser.BWXOR;
import static org.elasticsearch.painless.PainlessParser.BinaryContext;
import static org.elasticsearch.painless.PainlessParser.BlockContext;
import static org.elasticsearch.painless.PainlessParser.BoolContext;
import static org.elasticsearch.painless.PainlessParser.BreakContext;
import static org.elasticsearch.painless.PainlessParser.CastContext;
import static org.elasticsearch.painless.PainlessParser.CharContext;
import static org.elasticsearch.painless.PainlessParser.CompContext;
import static org.elasticsearch.painless.PainlessParser.ConditionalContext;
import static org.elasticsearch.painless.PainlessParser.ContinueContext;
import static org.elasticsearch.painless.PainlessParser.DIV;
import static org.elasticsearch.painless.PainlessParser.DeclContext;
import static org.elasticsearch.painless.PainlessParser.DeclarationContext;
import static org.elasticsearch.painless.PainlessParser.DecltypeContext;
import static org.elasticsearch.painless.PainlessParser.DeclvarContext;
import static org.elasticsearch.painless.PainlessParser.DoContext;
import static org.elasticsearch.painless.PainlessParser.EmptyContext;
import static org.elasticsearch.painless.PainlessParser.EmptyscopeContext;
import static org.elasticsearch.painless.PainlessParser.ExprContext;
import static org.elasticsearch.painless.PainlessParser.ExpressionContext;
import static org.elasticsearch.painless.PainlessParser.ExtbraceContext;
import static org.elasticsearch.painless.PainlessParser.ExtcallContext;
import static org.elasticsearch.painless.PainlessParser.ExtcastContext;
import static org.elasticsearch.painless.PainlessParser.ExtdotContext;
import static org.elasticsearch.painless.PainlessParser.ExternalContext;
import static org.elasticsearch.painless.PainlessParser.ExtfieldContext;
import static org.elasticsearch.painless.PainlessParser.ExtnewContext;
import static org.elasticsearch.painless.PainlessParser.ExtprecContext;
import static org.elasticsearch.painless.PainlessParser.ExtstartContext;
import static org.elasticsearch.painless.PainlessParser.ExtstringContext;
import static org.elasticsearch.painless.PainlessParser.ExttypeContext;
import static org.elasticsearch.painless.PainlessParser.ExtvarContext;
import static org.elasticsearch.painless.PainlessParser.FalseContext;
import static org.elasticsearch.painless.PainlessParser.ForContext;
import static org.elasticsearch.painless.PainlessParser.IfContext;
import static org.elasticsearch.painless.PainlessParser.IncrementContext;
import static org.elasticsearch.painless.PainlessParser.InitializerContext;
import static org.elasticsearch.painless.PainlessParser.LSH;
import static org.elasticsearch.painless.PainlessParser.MUL;
import static org.elasticsearch.painless.PainlessParser.MultipleContext;
import static org.elasticsearch.painless.PainlessParser.NullContext;
import static org.elasticsearch.painless.PainlessParser.NumericContext;
import static org.elasticsearch.painless.PainlessParser.PostincContext;
import static org.elasticsearch.painless.PainlessParser.PrecedenceContext;
import static org.elasticsearch.painless.PainlessParser.PreincContext;
import static org.elasticsearch.painless.PainlessParser.REM;
import static org.elasticsearch.painless.PainlessParser.RSH;
import static org.elasticsearch.painless.PainlessParser.ReturnContext;
import static org.elasticsearch.painless.PainlessParser.SUB;
import static org.elasticsearch.painless.PainlessParser.SingleContext;
import static org.elasticsearch.painless.PainlessParser.SourceContext;
import static org.elasticsearch.painless.PainlessParser.StatementContext;
import static org.elasticsearch.painless.PainlessParser.ThrowContext;
import static org.elasticsearch.painless.PainlessParser.TrapContext;
import static org.elasticsearch.painless.PainlessParser.TrueContext;
import static org.elasticsearch.painless.PainlessParser.TryContext;
import static org.elasticsearch.painless.PainlessParser.USH;
import static org.elasticsearch.painless.PainlessParser.UnaryContext;
import static org.elasticsearch.painless.PainlessParser.WhileContext;

class Writer extends PainlessParserBaseVisitor<Void> {
    private static class Branch {
        final ParserRuleContext source;

        Label begin = null;
        Label end = null;
        Label tru = null;
        Label fals = null;

        private Branch(final ParserRuleContext source) {
            this.source = source;
        }
    }

    final static String BASE_CLASS_NAME = Executable.class.getName();
    final static String CLASS_NAME = BASE_CLASS_NAME + "$CompiledPainlessExecutable";
    private final static org.objectweb.asm.Type BASE_CLASS_TYPE = org.objectweb.asm.Type.getType(Executable.class);
    private final static org.objectweb.asm.Type CLASS_TYPE =
        org.objectweb.asm.Type.getType("L" + CLASS_NAME.replace(".", "/") + ";");

    private final static org.objectweb.asm.Type PAINLESS_ERROR_TYPE = org.objectweb.asm.Type.getType(PainlessError.class);

    private final static org.objectweb.asm.commons.Method CONSTRUCTOR = org.objectweb.asm.commons.Method.getMethod(
        "void <init>(org.elasticsearch.painless.Definition, java.lang.String, java.lang.String)");
    private final static org.objectweb.asm.commons.Method EXECUTE = org.objectweb.asm.commons.Method.getMethod(
        "java.lang.Object execute(java.util.Map)");
    private final static String SIGNATURE = "(Ljava/util/Map<Ljava/lang/String;Ljava/lang/Object;>;)Ljava/lang/Object;";

    private final static org.objectweb.asm.Type DEFINITION_TYPE = org.objectweb.asm.Type.getType(Definition.class);

    private final static org.objectweb.asm.Type MAP_TYPE = org.objectweb.asm.Type.getType(Map.class);
    private final static org.objectweb.asm.commons.Method MAP_GET =
        org.objectweb.asm.commons.Method.getMethod("Object get(Object)");

    private final static org.objectweb.asm.Type SCORE_ACCESSOR_TYPE = org.objectweb.asm.Type.getType(ScoreAccessor.class);
    private final static org.objectweb.asm.commons.Method SCORE_ACCESSOR_FLOAT =
        org.objectweb.asm.commons.Method.getMethod("float floatValue()");

    private final static org.objectweb.asm.commons.Method DEF_METHOD_CALL = org.objectweb.asm.commons.Method.getMethod(
        "java.lang.Object methodCall(java.lang.Object, java.lang.String, " +
            "org.elasticsearch.painless.Definition, java.lang.Object[], boolean[])");
    private final static org.objectweb.asm.commons.Method DEF_ARRAY_STORE = org.objectweb.asm.commons.Method.getMethod(
        "void arrayStore(java.lang.Object, java.lang.Object, java.lang.Object, " +
            "org.elasticsearch.painless.Definition, boolean, boolean)");
    private final static org.objectweb.asm.commons.Method DEF_ARRAY_LOAD = org.objectweb.asm.commons.Method.getMethod(
        "java.lang.Object arrayLoad(java.lang.Object, java.lang.Object, " +
            "org.elasticsearch.painless.Definition, boolean)");
    private final static org.objectweb.asm.commons.Method DEF_FIELD_STORE = org.objectweb.asm.commons.Method.getMethod(
        "void fieldStore(java.lang.Object, java.lang.Object, java.lang.String, " +
            "org.elasticsearch.painless.Definition, boolean)");
    private final static org.objectweb.asm.commons.Method DEF_FIELD_LOAD = org.objectweb.asm.commons.Method.getMethod(
        "java.lang.Object fieldLoad(java.lang.Object, java.lang.String, org.elasticsearch.painless.Definition)");

    private final static org.objectweb.asm.commons.Method DEF_NOT_CALL = org.objectweb.asm.commons.Method.getMethod(
        "java.lang.Object not(java.lang.Object)");
    private final static org.objectweb.asm.commons.Method DEF_NEG_CALL = org.objectweb.asm.commons.Method.getMethod(
        "java.lang.Object neg(java.lang.Object)");
    private final static org.objectweb.asm.commons.Method DEF_MUL_CALL = org.objectweb.asm.commons.Method.getMethod(
        "java.lang.Object mul(java.lang.Object, java.lang.Object)");
    private final static org.objectweb.asm.commons.Method DEF_DIV_CALL = org.objectweb.asm.commons.Method.getMethod(
        "java.lang.Object div(java.lang.Object, java.lang.Object)");
    private final static org.objectweb.asm.commons.Method DEF_REM_CALL = org.objectweb.asm.commons.Method.getMethod(
        "java.lang.Object rem(java.lang.Object, java.lang.Object)");
    private final static org.objectweb.asm.commons.Method DEF_ADD_CALL = org.objectweb.asm.commons.Method.getMethod(
        "java.lang.Object add(java.lang.Object, java.lang.Object)");
    private final static org.objectweb.asm.commons.Method DEF_SUB_CALL = org.objectweb.asm.commons.Method.getMethod(
        "java.lang.Object sub(java.lang.Object, java.lang.Object)");
    private final static org.objectweb.asm.commons.Method DEF_LSH_CALL = org.objectweb.asm.commons.Method.getMethod(
        "java.lang.Object lsh(java.lang.Object, java.lang.Object)");
    private final static org.objectweb.asm.commons.Method DEF_RSH_CALL = org.objectweb.asm.commons.Method.getMethod(
        "java.lang.Object rsh(java.lang.Object, java.lang.Object)");
    private final static org.objectweb.asm.commons.Method DEF_USH_CALL = org.objectweb.asm.commons.Method.getMethod(
        "java.lang.Object ush(java.lang.Object, java.lang.Object)");
    private final static org.objectweb.asm.commons.Method DEF_AND_CALL = org.objectweb.asm.commons.Method.getMethod(
        "java.lang.Object and(java.lang.Object, java.lang.Object)");
    private final static org.objectweb.asm.commons.Method DEF_XOR_CALL = org.objectweb.asm.commons.Method.getMethod(
        "java.lang.Object xor(java.lang.Object, java.lang.Object)");
    private final static org.objectweb.asm.commons.Method DEF_OR_CALL = org.objectweb.asm.commons.Method.getMethod(
        "java.lang.Object or(java.lang.Object, java.lang.Object)");
    private final static org.objectweb.asm.commons.Method DEF_EQ_CALL = org.objectweb.asm.commons.Method.getMethod(
        "boolean eq(java.lang.Object, java.lang.Object)");
    private final static org.objectweb.asm.commons.Method DEF_LT_CALL = org.objectweb.asm.commons.Method.getMethod(
        "boolean lt(java.lang.Object, java.lang.Object)");
    private final static org.objectweb.asm.commons.Method DEF_LTE_CALL = org.objectweb.asm.commons.Method.getMethod(
        "boolean lte(java.lang.Object, java.lang.Object)");
    private final static org.objectweb.asm.commons.Method DEF_GT_CALL = org.objectweb.asm.commons.Method.getMethod(
        "boolean gt(java.lang.Object, java.lang.Object)");
    private final static org.objectweb.asm.commons.Method DEF_GTE_CALL = org.objectweb.asm.commons.Method.getMethod(
        "boolean gte(java.lang.Object, java.lang.Object)");

    private final static org.objectweb.asm.Type STRINGBUILDER_TYPE = org.objectweb.asm.Type.getType(StringBuilder.class);

    private final static org.objectweb.asm.commons.Method STRINGBUILDER_CONSTRUCTOR =
        org.objectweb.asm.commons.Method.getMethod("void <init>()");
    private final static org.objectweb.asm.commons.Method STRINGBUILDER_APPEND_BOOLEAN =
        org.objectweb.asm.commons.Method.getMethod("java.lang.StringBuilder append(boolean)");
    private final static org.objectweb.asm.commons.Method STRINGBUILDER_APPEND_CHAR =
        org.objectweb.asm.commons.Method.getMethod("java.lang.StringBuilder append(char)");
    private final static org.objectweb.asm.commons.Method STRINGBUILDER_APPEND_INT =
        org.objectweb.asm.commons.Method.getMethod("java.lang.StringBuilder append(int)");
    private final static org.objectweb.asm.commons.Method STRINGBUILDER_APPEND_LONG =
        org.objectweb.asm.commons.Method.getMethod("java.lang.StringBuilder append(long)");
    private final static org.objectweb.asm.commons.Method STRINGBUILDER_APPEND_FLOAT =
        org.objectweb.asm.commons.Method.getMethod("java.lang.StringBuilder append(float)");
    private final static org.objectweb.asm.commons.Method STRINGBUILDER_APPEND_DOUBLE =
        org.objectweb.asm.commons.Method.getMethod("java.lang.StringBuilder append(double)");
    private final static org.objectweb.asm.commons.Method STRINGBUILDER_APPEND_STRING =
        org.objectweb.asm.commons.Method.getMethod("java.lang.StringBuilder append(java.lang.String)");
    private final static org.objectweb.asm.commons.Method STRINGBUILDER_APPEND_OBJECT =
        org.objectweb.asm.commons.Method.getMethod("java.lang.StringBuilder append(java.lang.Object)");
    private final static org.objectweb.asm.commons.Method STRINGBUILDER_TOSTRING =
        org.objectweb.asm.commons.Method.getMethod("java.lang.String toString()");

    private final static org.objectweb.asm.commons.Method TOINTEXACT_LONG =
        org.objectweb.asm.commons.Method.getMethod("int toIntExact(long)");
    private final static org.objectweb.asm.commons.Method NEGATEEXACT_INT =
        org.objectweb.asm.commons.Method.getMethod("int negateExact(int)");
    private final static org.objectweb.asm.commons.Method NEGATEEXACT_LONG =
        org.objectweb.asm.commons.Method.getMethod("long negateExact(long)");
    private final static org.objectweb.asm.commons.Method MULEXACT_INT =
        org.objectweb.asm.commons.Method.getMethod("int multiplyExact(int, int)");
    private final static org.objectweb.asm.commons.Method MULEXACT_LONG =
        org.objectweb.asm.commons.Method.getMethod("long multiplyExact(long, long)");
    private final static org.objectweb.asm.commons.Method ADDEXACT_INT =
        org.objectweb.asm.commons.Method.getMethod("int addExact(int, int)");
    private final static org.objectweb.asm.commons.Method ADDEXACT_LONG =
        org.objectweb.asm.commons.Method.getMethod("long addExact(long, long)");
    private final static org.objectweb.asm.commons.Method SUBEXACT_INT =
        org.objectweb.asm.commons.Method.getMethod("int subtractExact(int, int)");
    private final static org.objectweb.asm.commons.Method SUBEXACT_LONG =
        org.objectweb.asm.commons.Method.getMethod("long subtractExact(long, long)");

    private final static org.objectweb.asm.commons.Method CHECKEQUALS =
        org.objectweb.asm.commons.Method.getMethod("boolean checkEquals(java.lang.Object, java.lang.Object)");
    private final static org.objectweb.asm.commons.Method TOBYTEEXACT_INT =
        org.objectweb.asm.commons.Method.getMethod("byte toByteExact(int)");
    private final static org.objectweb.asm.commons.Method TOBYTEEXACT_LONG =
        org.objectweb.asm.commons.Method.getMethod("byte toByteExact(long)");
    private final static org.objectweb.asm.commons.Method TOBYTEWOOVERFLOW_FLOAT =
        org.objectweb.asm.commons.Method.getMethod("byte toByteWithoutOverflow(float)");
    private final static org.objectweb.asm.commons.Method TOBYTEWOOVERFLOW_DOUBLE =
        org.objectweb.asm.commons.Method.getMethod("byte toByteWithoutOverflow(double)");
    private final static org.objectweb.asm.commons.Method TOSHORTEXACT_INT =
        org.objectweb.asm.commons.Method.getMethod("short toShortExact(int)");
    private final static org.objectweb.asm.commons.Method TOSHORTEXACT_LONG =
        org.objectweb.asm.commons.Method.getMethod("short toShortExact(long)");
    private final static org.objectweb.asm.commons.Method TOSHORTWOOVERFLOW_FLOAT =
        org.objectweb.asm.commons.Method.getMethod("short toShortWithoutOverflow(float)");
    private final static org.objectweb.asm.commons.Method TOSHORTWOOVERFLOW_DOUBLE =
        org.objectweb.asm.commons.Method.getMethod("short toShortWithoutOverflow(double)");
    private final static org.objectweb.asm.commons.Method TOCHAREXACT_INT =
        org.objectweb.asm.commons.Method.getMethod("char toCharExact(int)");
    private final static org.objectweb.asm.commons.Method TOCHAREXACT_LONG =
        org.objectweb.asm.commons.Method.getMethod("char toCharExact(long)");
    private final static org.objectweb.asm.commons.Method TOCHARWOOVERFLOW_FLOAT =
        org.objectweb.asm.commons.Method.getMethod("char toCharWithoutOverflow(float)");
    private final static org.objectweb.asm.commons.Method TOCHARWOOVERFLOW_DOUBLE =
        org.objectweb.asm.commons.Method.getMethod("char toCharWithoutOverflow(double)");
    private final static org.objectweb.asm.commons.Method TOINTWOOVERFLOW_FLOAT =
        org.objectweb.asm.commons.Method.getMethod("int toIntWithoutOverflow(float)");
    private final static org.objectweb.asm.commons.Method TOINTWOOVERFLOW_DOUBLE =
        org.objectweb.asm.commons.Method.getMethod("int toIntWithoutOverflow(double)");
    private final static org.objectweb.asm.commons.Method TOLONGWOOVERFLOW_FLOAT =
        org.objectweb.asm.commons.Method.getMethod("long toLongExactWithoutOverflow(float)");
    private final static org.objectweb.asm.commons.Method TOLONGWOOVERFLOW_DOUBLE =
        org.objectweb.asm.commons.Method.getMethod("long toLongExactWithoutOverflow(double)");
    private final static org.objectweb.asm.commons.Method TOFLOATWOOVERFLOW_DOUBLE =
        org.objectweb.asm.commons.Method.getMethod("float toFloatWithoutOverflow(double)");
    private final static org.objectweb.asm.commons.Method MULWOOVERLOW_FLOAT =
        org.objectweb.asm.commons.Method.getMethod("float multiplyWithoutOverflow(float, float)");
    private final static org.objectweb.asm.commons.Method MULWOOVERLOW_DOUBLE =
        org.objectweb.asm.commons.Method.getMethod("double multiplyWithoutOverflow(double, double)");
    private final static org.objectweb.asm.commons.Method DIVWOOVERLOW_INT =
        org.objectweb.asm.commons.Method.getMethod("int divideWithoutOverflow(int, int)");
    private final static org.objectweb.asm.commons.Method DIVWOOVERLOW_LONG =
        org.objectweb.asm.commons.Method.getMethod("long divideWithoutOverflow(long, long)");
    private final static org.objectweb.asm.commons.Method DIVWOOVERLOW_FLOAT =
        org.objectweb.asm.commons.Method.getMethod("float divideWithoutOverflow(float, float)");
    private final static org.objectweb.asm.commons.Method DIVWOOVERLOW_DOUBLE =
        org.objectweb.asm.commons.Method.getMethod("double divideWithoutOverflow(double, double)");
    private final static org.objectweb.asm.commons.Method REMWOOVERLOW_FLOAT =
        org.objectweb.asm.commons.Method.getMethod("float remainderWithoutOverflow(float, float)");
    private final static org.objectweb.asm.commons.Method REMWOOVERLOW_DOUBLE =
        org.objectweb.asm.commons.Method.getMethod("double remainderWithoutOverflow(double, double)");
    private final static org.objectweb.asm.commons.Method ADDWOOVERLOW_FLOAT =
        org.objectweb.asm.commons.Method.getMethod("float addWithoutOverflow(float, float)");
    private final static org.objectweb.asm.commons.Method ADDWOOVERLOW_DOUBLE =
        org.objectweb.asm.commons.Method.getMethod("double addWithoutOverflow(double, double)");
    private final static org.objectweb.asm.commons.Method SUBWOOVERLOW_FLOAT =
        org.objectweb.asm.commons.Method.getMethod("float subtractWithoutOverflow(float, float)");
    private final static org.objectweb.asm.commons.Method SUBWOOVERLOW_DOUBLE =
        org.objectweb.asm.commons.Method.getMethod("double subtractWithoutOverflow(double, double)");

    static byte[] write(Metadata metadata) {
        Writer writer = new Writer(metadata);

        return writer.getBytes();
    }

    private final Metadata metadata;
    private final Definition definition;
    private final ParseTree root;
    private final String source;
    private final CompilerSettings settings;

    private final Map<ParserRuleContext, Branch> branches = new HashMap<>();
    private final Deque<Branch> jumps = new ArrayDeque<>();
    private final Set<ParserRuleContext> strings = new HashSet<>();

    private ClassWriter writer;
    private GeneratorAdapter execute;

    private Writer(final Metadata metadata) {
        this.metadata = metadata;
        definition = metadata.definition;
        root = metadata.root;
        source = metadata.source;
        settings = metadata.settings;

        writeBegin();
        writeConstructor();
        writeExecute();
        writeEnd();
    }

    private Branch markBranch(final ParserRuleContext source, final ParserRuleContext... nodes) {
        final Branch branch = new Branch(source);

        for (final ParserRuleContext node : nodes) {
            branches.put(node, branch);
        }

        return branch;
    }

    private void copyBranch(final Branch branch, final ParserRuleContext... nodes) {
        for (final ParserRuleContext node : nodes) {
            branches.put(node, branch);
        }
    }

    private Branch getBranch(final ParserRuleContext source) {
        return branches.get(source);
    }

    private void writeBegin() {
        final int compute = ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS;
        final int version = Opcodes.V1_7;
        final int access = Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | Opcodes.ACC_FINAL | Opcodes.ACC_SYNTHETIC;
        final String base = BASE_CLASS_TYPE.getInternalName();
        final String name = CLASS_TYPE.getInternalName();

        writer = new ClassWriter(compute);
        writer.visit(version, access, name, null, base, null);
        writer.visitSource(source, null);
    }

    private void writeConstructor() {
        final int access = Opcodes.ACC_PUBLIC | Opcodes.ACC_SYNTHETIC;
        final GeneratorAdapter constructor = new GeneratorAdapter(access, CONSTRUCTOR, null, null, writer);
        constructor.loadThis();
        constructor.loadArgs();
        constructor.invokeConstructor(org.objectweb.asm.Type.getType(Executable.class), CONSTRUCTOR);
        constructor.returnValue();
        constructor.endMethod();
    }

    private void writeExecute() {
        final int access = Opcodes.ACC_PUBLIC | Opcodes.ACC_SYNTHETIC;
        execute = new GeneratorAdapter(access, EXECUTE, SIGNATURE, null, writer);

        final Label fals = new Label();
        final Label end = new Label();
        execute.visitVarInsn(Opcodes.ALOAD, metadata.inputValueSlot);
        execute.push("#score");
        execute.invokeInterface(MAP_TYPE, MAP_GET);
        execute.dup();
        execute.ifNull(fals);
        execute.checkCast(SCORE_ACCESSOR_TYPE);
        execute.invokeVirtual(SCORE_ACCESSOR_TYPE, SCORE_ACCESSOR_FLOAT);
        execute.goTo(end);
        execute.mark(fals);
        execute.pop();
        execute.push(0F);
        execute.mark(end);
        execute.visitVarInsn(Opcodes.FSTORE, metadata.scoreValueSlot);

        execute.push(settings.getMaxLoopCounter());
        execute.visitVarInsn(Opcodes.ISTORE, metadata.loopCounterSlot);

        visit(root);
        execute.endMethod();
    }

    @Override
    public Void visitSource(final SourceContext ctx) {
        final Metadata.StatementMetadata sourcesmd = metadata.getStatementMetadata(ctx);

        for (final StatementContext sctx : ctx.statement()) {
            visit(sctx);
        }

        if (!sourcesmd.methodEscape) {
            execute.visitInsn(Opcodes.ACONST_NULL);
            execute.returnValue();
        }

        return null;
    }

    @Override
    public Void visitIf(final IfContext ctx) {
        final ExpressionContext exprctx = ctx.expression();
        final boolean els = ctx.ELSE() != null;
        final Branch branch = markBranch(ctx, exprctx);
        branch.end = new Label();
        branch.fals = els ? new Label() : branch.end;

        visit(exprctx);

        final BlockContext blockctx0 = ctx.block(0);
        final Metadata.StatementMetadata blockmd0 = metadata.getStatementMetadata(blockctx0);
        visit(blockctx0);

        if (els) {
            if (!blockmd0.allLast) {
                execute.goTo(branch.end);
            }

            execute.mark(branch.fals);
            visit(ctx.block(1));
        }

        execute.mark(branch.end);

        return null;
    }

    @Override
    public Void visitWhile(final WhileContext ctx) {
        final ExpressionContext exprctx = ctx.expression();
        final Branch branch = markBranch(ctx, exprctx);
        branch.begin = new Label();
        branch.end = new Label();
        branch.fals = branch.end;

        jumps.push(branch);
        execute.mark(branch.begin);
        visit(exprctx);

        final BlockContext blockctx = ctx.block();
        boolean allLast = false;

        if (blockctx != null) {
            final Metadata.StatementMetadata blocksmd = metadata.getStatementMetadata(blockctx);
            allLast = blocksmd.allLast;
            writeLoopCounter(blocksmd.count > 0 ? blocksmd.count : 1);
            visit(blockctx);
        } else if (ctx.empty() != null) {
            writeLoopCounter(1);
        } else {
            throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
        }

        if (!allLast) {
            execute.goTo(branch.begin);
        }

        execute.mark(branch.end);
        jumps.pop();

        return null;
    }

    @Override
    public Void visitDo(final DoContext ctx) {
        final ExpressionContext exprctx = ctx.expression();
        final Branch branch = markBranch(ctx, exprctx);
        Label start = new Label();
        branch.begin = new Label();
        branch.end = new Label();
        branch.fals = branch.end;

        final BlockContext blockctx = ctx.block();
        final Metadata.StatementMetadata blocksmd = metadata.getStatementMetadata(blockctx);

        jumps.push(branch);
        execute.mark(start);
        visit(blockctx);
        execute.mark(branch.begin);
        visit(exprctx);
        writeLoopCounter(blocksmd.count > 0 ? blocksmd.count : 1);
        execute.goTo(start);
        execute.mark(branch.end);
        jumps.pop();

        return null;
    }

    @Override
    public Void visitFor(final ForContext ctx) {
        final ExpressionContext exprctx = ctx.expression();
        final AfterthoughtContext atctx = ctx.afterthought();
        final Branch branch = markBranch(ctx, exprctx);
        final Label start = new Label();
        branch.begin = atctx == null ? start : new Label();
        branch.end = new Label();
        branch.fals = branch.end;

        jumps.push(branch);

        if (ctx.initializer() != null) {
            visit(ctx.initializer());
        }

        execute.mark(start);

        if (exprctx != null) {
            visit(exprctx);
        }

        final BlockContext blockctx = ctx.block();
        boolean allLast = false;

        if (blockctx != null) {
            Metadata.StatementMetadata blocksmd = metadata.getStatementMetadata(blockctx);
            allLast = blocksmd.allLast;

            int count = blocksmd.count > 0 ? blocksmd.count : 1;

            if (atctx != null) {
                ++count;
            }

            writeLoopCounter(count);
            visit(blockctx);
        } else if (ctx.empty() != null) {
            writeLoopCounter(1);
        } else {
            throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
        }

        if (atctx != null) {
            execute.mark(branch.begin);
            visit(atctx);
        }

        if (atctx != null || !allLast) {
            execute.goTo(start);
        }

        execute.mark(branch.end);
        jumps.pop();

        return null;
    }

    @Override
    public Void visitDecl(final DeclContext ctx) {
        visit(ctx.declaration());

        return null;
    }

    @Override
    public Void visitContinue(final ContinueContext ctx) {
        final Branch jump = jumps.peek();
        execute.goTo(jump.begin);

        return null;
    }

    @Override
    public Void visitBreak(final BreakContext ctx) {
        final Branch jump = jumps.peek();
        execute.goTo(jump.end);

        return null;
    }

    @Override
    public Void visitReturn(final ReturnContext ctx) {
        visit(ctx.expression());
        execute.returnValue();

        return null;
    }

    @Override
    public Void visitTry(final TryContext ctx) {
        final TrapContext[] trapctxs = new TrapContext[ctx.trap().size()];
        ctx.trap().toArray(trapctxs);
        final Branch branch = markBranch(ctx, trapctxs);

        Label end = new Label();
        branch.begin = new Label();
        branch.end = new Label();
        branch.tru = trapctxs.length > 1 ? end : null;

        execute.mark(branch.begin);

        final BlockContext blockctx = ctx.block();
        final Metadata.StatementMetadata blocksmd = metadata.getStatementMetadata(blockctx);
        visit(blockctx);

        if (!blocksmd.allLast) {
            execute.goTo(end);
        }

        execute.mark(branch.end);

        for (final TrapContext trapctx : trapctxs) {
            visit(trapctx);
        }

        if (!blocksmd.allLast || trapctxs.length > 1) {
            execute.mark(end);
        }

        return null;
    }

    @Override
    public Void visitThrow(final ThrowContext ctx) {
        visit(ctx.expression());
        execute.throwException();

        return null;
    }

    @Override
    public Void visitExpr(final ExprContext ctx) {
        final Metadata.StatementMetadata exprsmd = metadata.getStatementMetadata(ctx);
        final ExpressionContext exprctx = ctx.expression();
        final Metadata.ExpressionMetadata expremd = metadata.getExpressionMetadata(exprctx);
        visit(exprctx);

        if (exprsmd.methodEscape) {
            execute.returnValue();
        } else {
            writePop(expremd.to.type.getSize());
        }

        return null;
    }

    @Override
    public Void visitMultiple(final MultipleContext ctx) {
        for (final StatementContext sctx : ctx.statement()) {
            visit(sctx);
        }

        return null;
    }

    @Override
    public Void visitSingle(final SingleContext ctx) {
        visit(ctx.statement());

        return null;
    }

    @Override
    public Void visitEmpty(final EmptyContext ctx) {
        throw new UnsupportedOperationException(Metadata.error(ctx) + "Unexpected writer state.");
    }

    @Override
    public Void visitInitializer(InitializerContext ctx) {
        final DeclarationContext declctx = ctx.declaration();
        final ExpressionContext exprctx = ctx.expression();

        if (declctx != null) {
            visit(declctx);
        } else if (exprctx != null) {
            final Metadata.ExpressionMetadata expremd = metadata.getExpressionMetadata(exprctx);
            visit(exprctx);
            writePop(expremd.to.type.getSize());
        } else {
            throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
        }

        return null;
    }

    @Override
    public Void visitAfterthought(AfterthoughtContext ctx) {
        final ExpressionContext exprctx = ctx.expression();
        final Metadata.ExpressionMetadata expremd = metadata.getExpressionMetadata(exprctx);
        visit(ctx.expression());
        writePop(expremd.to.type.getSize());

        return null;
    }

    @Override
    public Void visitDeclaration(DeclarationContext ctx) {
        for (final DeclvarContext declctx : ctx.declvar()) {
            visit(declctx);
        }

        return null;
    }

    @Override
    public Void visitDecltype(final DecltypeContext ctx) {
        throw new UnsupportedOperationException(Metadata.error(ctx) + "Unexpected writer state.");
    }

    @Override
    public Void visitDeclvar(final DeclvarContext ctx) {
        final Metadata.ExpressionMetadata declvaremd = metadata.getExpressionMetadata(ctx);
        final org.objectweb.asm.Type type = declvaremd.to.type;
        final Sort sort = declvaremd.to.sort;
        final int slot = (int)declvaremd.postConst;

        final ExpressionContext exprctx = ctx.expression();
        final boolean initialize = exprctx == null;

        if (!initialize) {
            visit(exprctx);
        }

        switch (sort) {
            case VOID:   throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
            case BOOL:
            case BYTE:
            case SHORT:
            case CHAR:
            case INT:    if (initialize) execute.push(0);    break;
            case LONG:   if (initialize) execute.push(0L);   break;
            case FLOAT:  if (initialize) execute.push(0.0F); break;
            case DOUBLE: if (initialize) execute.push(0.0);  break;
            default:     if (initialize) execute.visitInsn(Opcodes.ACONST_NULL);
        }

        execute.visitVarInsn(type.getOpcode(Opcodes.ISTORE), slot);

        return null;
    }

    @Override
    public Void visitTrap(final TrapContext ctx) {
        final Metadata.StatementMetadata trapsmd = metadata.getStatementMetadata(ctx);

        final Branch branch = getBranch(ctx);
        final Label jump = new Label();

        final BlockContext blockctx = ctx.block();
        final EmptyscopeContext emptyctx = ctx.emptyscope();

        execute.mark(jump);
        writeLoadStoreVariable(ctx, true, trapsmd.exception, trapsmd.slot);

        if (blockctx != null) {
            visit(ctx.block());
        } else if (emptyctx == null) {
            throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
        }

        execute.visitTryCatchBlock(branch.begin, branch.end, jump, trapsmd.exception.type.getInternalName());

        if (branch.tru != null && !trapsmd.allLast) {
            execute.goTo(branch.tru);
        }

        return null;
    }

    @Override
    public Void visitPrecedence(final PrecedenceContext ctx) {
        throw new UnsupportedOperationException(Metadata.error(ctx) + "Unexpected writer state.");
    }

    @Override
    public Void visitNumeric(final NumericContext ctx) {
        final Metadata.ExpressionMetadata numericemd = metadata.getExpressionMetadata(ctx);
        final Object postConst = numericemd.postConst;

        if (postConst == null) {
            writeNumeric(ctx, numericemd.preConst);
            checkWriteCast(numericemd);
        } else {
            writeConstant(ctx, postConst);
        }

        checkWriteBranch(ctx);

        return null;
    }

    @Override
    public Void visitChar(final CharContext ctx) {
        final Metadata.ExpressionMetadata charemd = metadata.getExpressionMetadata(ctx);
        final Object postConst = charemd.postConst;

        if (postConst == null) {
            writeNumeric(ctx, (int)(char)charemd.preConst);
            checkWriteCast(charemd);
        } else {
            writeConstant(ctx, postConst);
        }

        checkWriteBranch(ctx);

        return null;
    }

    @Override
    public Void visitTrue(final TrueContext ctx) {
        final Metadata.ExpressionMetadata trueemd = metadata.getExpressionMetadata(ctx);
        final Object postConst = trueemd.postConst;
        final Branch branch = getBranch(ctx);

        if (branch == null) {
            if (postConst == null) {
                writeBoolean(ctx, true);
                checkWriteCast(trueemd);
            } else {
                writeConstant(ctx, postConst);
            }
        } else if (branch.tru != null) {
            execute.goTo(branch.tru);
        }

        return null;
    }

    @Override
    public Void visitFalse(final FalseContext ctx) {
        final Metadata.ExpressionMetadata falseemd = metadata.getExpressionMetadata(ctx);
        final Object postConst = falseemd.postConst;
        final Branch branch = getBranch(ctx);

        if (branch == null) {
            if (postConst == null) {
                writeBoolean(ctx, false);
                checkWriteCast(falseemd);
            } else {
                writeConstant(ctx, postConst);
            }
        } else if (branch.fals != null) {
            execute.goTo(branch.fals);
        }

        return null;
    }

    @Override
    public Void visitNull(final NullContext ctx) {
        final Metadata.ExpressionMetadata nullemd = metadata.getExpressionMetadata(ctx);

        execute.visitInsn(Opcodes.ACONST_NULL);
        checkWriteCast(nullemd);
        checkWriteBranch(ctx);

        return null;
    }

    @Override
    public Void visitExternal(final ExternalContext ctx) {
        final Metadata.ExpressionMetadata expremd = metadata.getExpressionMetadata(ctx);
        visit(ctx.extstart());
        checkWriteCast(expremd);
        checkWriteBranch(ctx);

        return null;
    }


    @Override
    public Void visitPostinc(final PostincContext ctx) {
        final Metadata.ExpressionMetadata expremd = metadata.getExpressionMetadata(ctx);
        visit(ctx.extstart());
        checkWriteCast(expremd);
        checkWriteBranch(ctx);

        return null;
    }

    @Override
    public Void visitPreinc(final PreincContext ctx) {
        final Metadata.ExpressionMetadata expremd = metadata.getExpressionMetadata(ctx);
        visit(ctx.extstart());
        checkWriteCast(expremd);
        checkWriteBranch(ctx);

        return null;
    }

    @Override
    public Void visitUnary(final UnaryContext ctx) {
        final Metadata.ExpressionMetadata unaryemd = metadata.getExpressionMetadata(ctx);
        final Object postConst = unaryemd.postConst;
        final Object preConst = unaryemd.preConst;
        final Branch branch = getBranch(ctx);

        if (postConst != null) {
            if (ctx.BOOLNOT() != null) {
                if (branch == null) {
                    writeConstant(ctx, postConst);
                } else {
                    if ((boolean)postConst && branch.tru != null) {
                        execute.goTo(branch.tru);
                    } else if (!(boolean)postConst && branch.fals != null) {
                        execute.goTo(branch.fals);
                    }
                }
            } else {
                writeConstant(ctx, postConst);
                checkWriteBranch(ctx);
            }
        } else if (preConst != null) {
            if (branch == null) {
                writeConstant(ctx, preConst);
                checkWriteCast(unaryemd);
            } else {
                throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
            }
        } else {
            final ExpressionContext exprctx = ctx.expression();

            if (ctx.BOOLNOT() != null) {
                final Branch local = markBranch(ctx, exprctx);

                if (branch == null) {
                    local.fals = new Label();
                    final Label aend = new Label();

                    visit(exprctx);

                    execute.push(false);
                    execute.goTo(aend);
                    execute.mark(local.fals);
                    execute.push(true);
                    execute.mark(aend);

                    checkWriteCast(unaryemd);
                } else {
                    local.tru = branch.fals;
                    local.fals = branch.tru;

                    visit(exprctx);
                }
            } else {
                final org.objectweb.asm.Type type = unaryemd.from.type;
                final Sort sort = unaryemd.from.sort;

                visit(exprctx);

                if (ctx.BWNOT() != null) {
                    if (sort == Sort.DEF) {
                        execute.invokeStatic(definition.defobjType.type, DEF_NOT_CALL);
                    } else {
                        if (sort == Sort.INT) {
                            writeConstant(ctx, -1);
                        } else if (sort == Sort.LONG) {
                            writeConstant(ctx, -1L);
                        } else {
                            throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
                        }

                        execute.math(GeneratorAdapter.XOR, type);
                    }
                } else if (ctx.SUB() != null) {
                    if (sort == Sort.DEF) {
                        execute.invokeStatic(definition.defobjType.type, DEF_NEG_CALL);
                    } else {
                        if (settings.getNumericOverflow()) {
                            execute.math(GeneratorAdapter.NEG, type);
                        } else {
                            if (sort == Sort.INT) {
                                execute.invokeStatic(definition.mathType.type, NEGATEEXACT_INT);
                            } else if (sort == Sort.LONG) {
                                execute.invokeStatic(definition.mathType.type, NEGATEEXACT_LONG);
                            } else {
                                throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
                            }
                        }
                    }
                } else if (ctx.ADD() == null) {
                    throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
                }

                checkWriteCast(unaryemd);
                checkWriteBranch(ctx);
            }
        }

        return null;
    }

    @Override
    public Void visitCast(final CastContext ctx) {
        final Metadata.ExpressionMetadata castemd = metadata.getExpressionMetadata(ctx);
        final Object postConst = castemd.postConst;

        if (postConst == null) {
            visit(ctx.expression());
            checkWriteCast(castemd);
        } else {
            writeConstant(ctx, postConst);
        }

        checkWriteBranch(ctx);

        return null;
    }

    @Override
    public Void visitBinary(final BinaryContext ctx) {
        final Metadata.ExpressionMetadata binaryemd = metadata.getExpressionMetadata(ctx);
        final Object postConst = binaryemd.postConst;
        final Object preConst = binaryemd.preConst;
        final Branch branch = getBranch(ctx);

        if (postConst != null) {
            writeConstant(ctx, postConst);
        } else if (preConst != null) {
            if (branch == null) {
                writeConstant(ctx, preConst);
                checkWriteCast(binaryemd);
            } else {
                throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
            }
        } else if (binaryemd.from.sort == Sort.STRING) {
            final boolean marked = strings.contains(ctx);

            if (!marked) {
                writeNewStrings();
            }

            final ExpressionContext exprctx0 = ctx.expression(0);
            final Metadata.ExpressionMetadata expremd0 = metadata.getExpressionMetadata(exprctx0);
            strings.add(exprctx0);
            visit(exprctx0);

            if (strings.contains(exprctx0)) {
                writeAppendStrings(expremd0.from.sort);
                strings.remove(exprctx0);
            }

            final ExpressionContext exprctx1 = ctx.expression(1);
            final Metadata.ExpressionMetadata expremd1 = metadata.getExpressionMetadata(exprctx1);
            strings.add(exprctx1);
            visit(exprctx1);

            if (strings.contains(exprctx1)) {
                writeAppendStrings(expremd1.from.sort);
                strings.remove(exprctx1);
            }

            if (marked) {
                strings.remove(ctx);
            } else {
                writeToStrings();
            }

            checkWriteCast(binaryemd);
        } else {
            final ExpressionContext exprctx0 = ctx.expression(0);
            final ExpressionContext exprctx1 = ctx.expression(1);

            visit(exprctx0);
            visit(exprctx1);

            final Type type = binaryemd.from;

            if      (ctx.MUL()   != null) writeBinaryInstruction(ctx, type, MUL);
            else if (ctx.DIV()   != null) writeBinaryInstruction(ctx, type, DIV);
            else if (ctx.REM()   != null) writeBinaryInstruction(ctx, type, REM);
            else if (ctx.ADD()   != null) writeBinaryInstruction(ctx, type, ADD);
            else if (ctx.SUB()   != null) writeBinaryInstruction(ctx, type, SUB);
            else if (ctx.LSH()   != null) writeBinaryInstruction(ctx, type, LSH);
            else if (ctx.USH()   != null) writeBinaryInstruction(ctx, type, USH);
            else if (ctx.RSH()   != null) writeBinaryInstruction(ctx, type, RSH);
            else if (ctx.BWAND() != null) writeBinaryInstruction(ctx, type, BWAND);
            else if (ctx.BWXOR() != null) writeBinaryInstruction(ctx, type, BWXOR);
            else if (ctx.BWOR()  != null) writeBinaryInstruction(ctx, type, BWOR);
            else {
                throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
            }

            checkWriteCast(binaryemd);
        }

        checkWriteBranch(ctx);

        return null;
    }

    @Override
    public Void visitComp(final CompContext ctx) {
        final Metadata.ExpressionMetadata compemd = metadata.getExpressionMetadata(ctx);
        final Object postConst = compemd.postConst;
        final Object preConst = compemd.preConst;
        final Branch branch = getBranch(ctx);

        if (postConst != null) {
            if (branch == null) {
                writeConstant(ctx, postConst);
            } else {
                if ((boolean)postConst && branch.tru != null) {
                    execute.mark(branch.tru);
                } else if (!(boolean)postConst && branch.fals != null) {
                    execute.mark(branch.fals);
                }
            }
        } else if (preConst != null) {
            if (branch == null) {
                writeConstant(ctx, preConst);
                checkWriteCast(compemd);
            } else {
                throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
            }
        } else {
            final ExpressionContext exprctx0 = ctx.expression(0);
            final Metadata.ExpressionMetadata expremd0 = metadata.getExpressionMetadata(exprctx0);

            final ExpressionContext exprctx1 = ctx.expression(1);
            final Metadata.ExpressionMetadata expremd1 = metadata.getExpressionMetadata(exprctx1);
            final org.objectweb.asm.Type type = expremd1.to.type;
            final Sort sort1 = expremd1.to.sort;

            visit(exprctx0);

            if (!expremd1.isNull) {
                visit(exprctx1);
            }

            final boolean tru = branch != null && branch.tru != null;
            final boolean fals = branch != null && branch.fals != null;
            final Label jump = tru ? branch.tru : fals ? branch.fals : new Label();
            final Label end = new Label();

            final boolean eq = (ctx.EQ() != null || ctx.EQR() != null) && (tru || !fals) ||
                (ctx.NE() != null || ctx.NER() != null) && fals;
            final boolean ne = (ctx.NE() != null || ctx.NER() != null) && (tru || !fals) ||
                (ctx.EQ() != null || ctx.EQR() != null) && fals;
            final boolean lt  = ctx.LT()  != null && (tru || !fals) || ctx.GTE() != null && fals;
            final boolean lte = ctx.LTE() != null && (tru || !fals) || ctx.GT()  != null && fals;
            final boolean gt  = ctx.GT()  != null && (tru || !fals) || ctx.LTE() != null && fals;
            final boolean gte = ctx.GTE() != null && (tru || !fals) || ctx.LT()  != null && fals;

            boolean writejump = true;

            switch (sort1) {
                case VOID:
                case BYTE:
                case SHORT:
                case CHAR:
                    throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
                case BOOL:
                    if      (eq) execute.ifZCmp(GeneratorAdapter.EQ, jump);
                    else if (ne) execute.ifZCmp(GeneratorAdapter.NE, jump);
                    else {
                        throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
                    }

                    break;
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                    if      (eq)  execute.ifCmp(type, GeneratorAdapter.EQ, jump);
                    else if (ne)  execute.ifCmp(type, GeneratorAdapter.NE, jump);
                    else if (lt)  execute.ifCmp(type, GeneratorAdapter.LT, jump);
                    else if (lte) execute.ifCmp(type, GeneratorAdapter.LE, jump);
                    else if (gt)  execute.ifCmp(type, GeneratorAdapter.GT, jump);
                    else if (gte) execute.ifCmp(type, GeneratorAdapter.GE, jump);
                    else {
                        throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
                    }

                    break;
                case DEF:
                    if (eq) {
                        if (expremd1.isNull) {
                            execute.ifNull(jump);
                        } else if (!expremd0.isNull && ctx.EQ() != null) {
                            execute.invokeStatic(definition.defobjType.type, DEF_EQ_CALL);
                        } else {
                            execute.ifCmp(type, GeneratorAdapter.EQ, jump);
                        }
                    } else if (ne) {
                        if (expremd1.isNull) {
                            execute.ifNonNull(jump);
                        } else if (!expremd0.isNull && ctx.NE() != null) {
                            execute.invokeStatic(definition.defobjType.type, DEF_EQ_CALL);
                            execute.ifZCmp(GeneratorAdapter.EQ, jump);
                        } else {
                            execute.ifCmp(type, GeneratorAdapter.NE, jump);
                        }
                    } else if (lt) {
                        execute.invokeStatic(definition.defobjType.type, DEF_LT_CALL);
                    } else if (lte) {
                        execute.invokeStatic(definition.defobjType.type, DEF_LTE_CALL);
                    } else if (gt) {
                        execute.invokeStatic(definition.defobjType.type, DEF_GT_CALL);
                    } else if (gte) {
                        execute.invokeStatic(definition.defobjType.type, DEF_GTE_CALL);
                    } else {
                        throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
                    }

                    writejump = expremd1.isNull || ne || ctx.EQR() != null;

                    if (branch != null && !writejump) {
                        execute.ifZCmp(GeneratorAdapter.NE, jump);
                    }

                    break;
                default:
                    if (eq) {
                        if (expremd1.isNull) {
                            execute.ifNull(jump);
                        } else if (ctx.EQ() != null) {
                            execute.invokeStatic(definition.utilityType.type, CHECKEQUALS);

                            if (branch != null) {
                                execute.ifZCmp(GeneratorAdapter.NE, jump);
                            }

                            writejump = false;
                        } else {
                            execute.ifCmp(type, GeneratorAdapter.EQ, jump);
                        }
                    } else if (ne) {
                        if (expremd1.isNull) {
                            execute.ifNonNull(jump);
                        } else if (ctx.NE() != null) {
                            execute.invokeStatic(definition.utilityType.type, CHECKEQUALS);
                            execute.ifZCmp(GeneratorAdapter.EQ, jump);
                        } else {
                            execute.ifCmp(type, GeneratorAdapter.NE, jump);
                        }
                    } else {
                        throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
                    }
            }

            if (branch == null) {
                if (writejump) {
                    execute.push(false);
                    execute.goTo(end);
                    execute.mark(jump);
                    execute.push(true);
                    execute.mark(end);
                }

                checkWriteCast(compemd);
            }
        }

        return null;
    }

    @Override
    public Void visitBool(final BoolContext ctx) {
        final Metadata.ExpressionMetadata boolemd = metadata.getExpressionMetadata(ctx);
        final Object postConst = boolemd.postConst;
        final Object preConst = boolemd.preConst;
        final Branch branch = getBranch(ctx);

        if (postConst != null) {
            if (branch == null) {
                writeConstant(ctx, postConst);
            } else {
                if ((boolean)postConst && branch.tru != null) {
                    execute.mark(branch.tru);
                } else if (!(boolean)postConst && branch.fals != null) {
                    execute.mark(branch.fals);
                }
            }
        } else if (preConst != null) {
            if (branch == null) {
                writeConstant(ctx, preConst);
                checkWriteCast(boolemd);
            } else {
                throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
            }
        } else {
            final ExpressionContext exprctx0 = ctx.expression(0);
            final ExpressionContext exprctx1 = ctx.expression(1);

            if (branch == null) {
                if (ctx.BOOLAND() != null) {
                    final Branch local = markBranch(ctx, exprctx0, exprctx1);
                    local.fals = new Label();
                    final Label end = new Label();

                    visit(exprctx0);
                    visit(exprctx1);

                    execute.push(true);
                    execute.goTo(end);
                    execute.mark(local.fals);
                    execute.push(false);
                    execute.mark(end);
                } else if (ctx.BOOLOR() != null) {
                    final Branch branch0 = markBranch(ctx, exprctx0);
                    branch0.tru = new Label();
                    final Branch branch1 = markBranch(ctx, exprctx1);
                    branch1.fals = new Label();
                    final Label aend = new Label();

                    visit(exprctx0);
                    visit(exprctx1);

                    execute.mark(branch0.tru);
                    execute.push(true);
                    execute.goTo(aend);
                    execute.mark(branch1.fals);
                    execute.push(false);
                    execute.mark(aend);
                } else {
                    throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
                }

                checkWriteCast(boolemd);
            } else {
                if (ctx.BOOLAND() != null) {
                    final Branch branch0 = markBranch(ctx, exprctx0);
                    branch0.fals = branch.fals == null ? new Label() : branch.fals;
                    final Branch branch1 = markBranch(ctx, exprctx1);
                    branch1.tru = branch.tru;
                    branch1.fals = branch.fals;

                    visit(exprctx0);
                    visit(exprctx1);

                    if (branch.fals == null) {
                        execute.mark(branch0.fals);
                    }
                } else if (ctx.BOOLOR() != null) {
                    final Branch branch0 = markBranch(ctx, exprctx0);
                    branch0.tru = branch.tru == null ? new Label() : branch.tru;
                    final Branch branch1 = markBranch(ctx, exprctx1);
                    branch1.tru = branch.tru;
                    branch1.fals = branch.fals;

                    visit(exprctx0);
                    visit(exprctx1);

                    if (branch.tru == null) {
                        execute.mark(branch0.tru);
                    }
                } else {
                    throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
                }
            }
        }

        return null;
    }

    @Override
    public Void visitConditional(final ConditionalContext ctx) {
        final Metadata.ExpressionMetadata condemd = metadata.getExpressionMetadata(ctx);
        final Branch branch = getBranch(ctx);

        final ExpressionContext expr0 = ctx.expression(0);
        final ExpressionContext expr1 = ctx.expression(1);
        final ExpressionContext expr2 = ctx.expression(2);

        final Branch local = markBranch(ctx, expr0);
        local.fals = new Label();
        local.end = new Label();

        if (branch != null) {
            copyBranch(branch, expr1, expr2);
        }

        visit(expr0);
        visit(expr1);
        execute.goTo(local.end);
        execute.mark(local.fals);
        visit(expr2);
        execute.mark(local.end);

        if (branch == null) {
            checkWriteCast(condemd);
        }

        return null;
    }

    @Override
    public Void visitAssignment(final AssignmentContext ctx) {
        final Metadata.ExpressionMetadata expremd = metadata.getExpressionMetadata(ctx);
        visit(ctx.extstart());
        checkWriteCast(expremd);
        checkWriteBranch(ctx);

        return null;
    }

    @Override
    public Void visitExtstart(ExtstartContext ctx) {
        final Metadata.ExternalMetadata startemd = metadata.getExternalMetadata(ctx);

        if (startemd.token == ADD) {
            final Metadata.ExpressionMetadata storeemd = metadata.getExpressionMetadata(startemd.storeExpr);

            if (startemd.current.sort == Sort.STRING || storeemd.from.sort == Sort.STRING) {
                writeNewStrings();
                strings.add(startemd.storeExpr);
            }
        }

        final ExtprecContext precctx = ctx.extprec();
        final ExtcastContext castctx = ctx.extcast();
        final ExttypeContext typectx = ctx.exttype();
        final ExtvarContext varctx = ctx.extvar();
        final ExtnewContext newctx = ctx.extnew();
        final ExtstringContext stringctx = ctx.extstring();

        if (precctx != null) {
            visit(precctx);
        } else if (castctx != null) {
            visit(castctx);
        } else if (typectx != null) {
            visit(typectx);
        } else if (varctx != null) {
            visit(varctx);
        } else if (newctx != null) {
            visit(newctx);
        } else if (stringctx != null) {
            visit(stringctx);
        } else {
            throw new IllegalStateException();
        }

        return null;
    }

    @Override
    public Void visitExtprec(final ExtprecContext ctx) {
        final ExtprecContext precctx = ctx.extprec();
        final ExtcastContext castctx = ctx.extcast();
        final ExttypeContext typectx = ctx.exttype();
        final ExtvarContext varctx = ctx.extvar();
        final ExtnewContext newctx = ctx.extnew();
        final ExtstringContext stringctx = ctx.extstring();

        if (precctx != null) {
            visit(precctx);
        } else if (castctx != null) {
            visit(castctx);
        } else if (typectx != null) {
            visit(typectx);
        } else if (varctx != null) {
            visit(varctx);
        } else if (newctx != null) {
            visit(newctx);
        } else if (stringctx != null) {
            visit(stringctx);
        } else {
            throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
        }

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        if (dotctx != null) {
            visit(dotctx);
        } else if (bracectx != null) {
            visit(bracectx);
        }

        return null;
    }

    @Override
    public Void visitExtcast(final ExtcastContext ctx) {
        Metadata.ExtNodeMetadata castenmd = metadata.getExtNodeMetadata(ctx);

        final ExtprecContext precctx = ctx.extprec();
        final ExtcastContext castctx = ctx.extcast();
        final ExttypeContext typectx = ctx.exttype();
        final ExtvarContext varctx = ctx.extvar();
        final ExtnewContext newctx = ctx.extnew();
        final ExtstringContext stringctx = ctx.extstring();

        if (precctx != null) {
            visit(precctx);
        } else if (castctx != null) {
            visit(castctx);
        } else if (typectx != null) {
            visit(typectx);
        } else if (varctx != null) {
            visit(varctx);
        } else if (newctx != null) {
            visit(newctx);
        } else if (stringctx != null) {
            visit(stringctx);
        } else {
            throw new IllegalStateException(Metadata.error(ctx) + "Unexpected writer state.");
        }

        checkWriteCast(ctx, castenmd.castTo);

        return null;
    }

    @Override
    public Void visitExtbrace(final ExtbraceContext ctx) {
        final ExpressionContext exprctx = metadata.updateExpressionTree(ctx.expression());

        visit(exprctx);
        writeLoadStoreExternal(ctx);

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        if (dotctx != null) {
            visit(dotctx);
        } else if (bracectx != null) {
            visit(bracectx);
        }

        return null;
    }

    @Override
    public Void visitExtdot(final ExtdotContext ctx) {
        final ExtcallContext callctx = ctx.extcall();
        final ExtfieldContext fieldctx = ctx.extfield();

        if (callctx != null) {
            visit(callctx);
        } else if (fieldctx != null) {
            visit(fieldctx);
        }

        return null;
    }

    @Override
    public Void visitExttype(final ExttypeContext ctx) {
        visit(ctx.extdot());

        return null;
    }

    @Override
    public Void visitExtcall(final ExtcallContext ctx) {
        writeCallExternal(ctx);

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        if (dotctx != null) {
            visit(dotctx);
        } else if (bracectx != null) {
            visit(bracectx);
        }

        return null;
    }

    @Override
    public Void visitExtvar(final ExtvarContext ctx) {
        writeLoadStoreExternal(ctx);

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        if (dotctx != null) {
            visit(dotctx);
        } else if (bracectx != null) {
            visit(bracectx);
        }

        return null;
    }

    @Override
    public Void visitExtfield(final ExtfieldContext ctx) {
        writeLoadStoreExternal(ctx);

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        if (dotctx != null) {
            visit(dotctx);
        } else if (bracectx != null) {
            visit(bracectx);
        }

        return null;
    }

    @Override
    public Void visitExtnew(ExtnewContext ctx) {
        writeNewExternal(ctx);

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        if (dotctx != null) {
            visit(dotctx);
        } else if (bracectx != null) {
            visit(bracectx);
        }

        return null;
    }

    @Override
    public Void visitExtstring(ExtstringContext ctx) {
        final Metadata.ExtNodeMetadata stringenmd = metadata.getExtNodeMetadata(ctx);

        writeConstant(ctx, stringenmd.target);

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        if (dotctx != null) {
            visit(dotctx);
        } else if (bracectx != null) {
            visit(bracectx);
        }

        return null;
    }

    @Override
    public Void visitArguments(final ArgumentsContext ctx) {
        throw new UnsupportedOperationException(Metadata.error(ctx) + "Unexpected writer state.");
    }

    @Override
    public Void visitIncrement(IncrementContext ctx) {
        final Metadata.ExpressionMetadata incremd = metadata.getExpressionMetadata(ctx);
        final Object postConst = incremd.postConst;

        if (postConst == null) {
            writeNumeric(ctx, incremd.preConst);
            checkWriteCast(incremd);
        } else {
            writeConstant(ctx, postConst);
        }

        checkWriteBranch(ctx);

        return null;
    }

    private void writeLoopCounter(final int count) {
        final Label end = new Label();

        execute.iinc(metadata.loopCounterSlot, -count);
        execute.visitVarInsn(Opcodes.ILOAD, metadata.loopCounterSlot);
        execute.push(0);
        execute.ifICmp(GeneratorAdapter.GT, end);
        execute.throwException(PAINLESS_ERROR_TYPE,
            "The maximum number of statements that can be executed in a loop has been reached.");
        execute.mark(end);
    }

    private void writeConstant(final ParserRuleContext source, final Object constant) {
        if (constant instanceof Number) {
            writeNumeric(source, constant);
        } else if (constant instanceof Character) {
            writeNumeric(source, (int)(char)constant);
        } else if (constant instanceof String) {
            writeString(source, constant);
        } else if (constant instanceof Boolean) {
            writeBoolean(source, constant);
        } else if (constant != null) {
            throw new IllegalStateException(Metadata.error(source) + "Unexpected writer state.");
        }
    }

    private void writeNumeric(final ParserRuleContext source, final Object numeric) {
        if (numeric instanceof Double) {
            execute.push((double)numeric);
        } else if (numeric instanceof Float) {
            execute.push((float)numeric);
        } else if (numeric instanceof Long) {
            execute.push((long)numeric);
        } else if (numeric instanceof Number) {
            execute.push(((Number)numeric).intValue());
        } else {
            throw new IllegalStateException(Metadata.error(source) + "Unexpected writer state.");
        }
    }

    private void writeString(final ParserRuleContext source, final Object string) {
        if (string instanceof String) {
            execute.push((String)string);
        } else {
            throw new IllegalStateException(Metadata.error(source) + "Unexpected writer state.");
        }
    }

    private void writeBoolean(final ParserRuleContext source, final Object bool) {
        if (bool instanceof Boolean) {
            execute.push((boolean)bool);
        } else {
            throw new IllegalStateException(Metadata.error(source) + "Unexpected writer state.");
        }
    }

    private void writeNewStrings() {
        execute.newInstance(STRINGBUILDER_TYPE);
        execute.dup();
        execute.invokeConstructor(STRINGBUILDER_TYPE, STRINGBUILDER_CONSTRUCTOR);
    }

    private void writeAppendStrings(final Sort sort) {
        switch (sort) {
            case BOOL:   execute.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_BOOLEAN); break;
            case CHAR:   execute.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_CHAR);    break;
            case BYTE:
            case SHORT:
            case INT:    execute.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_INT);     break;
            case LONG:   execute.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_LONG);    break;
            case FLOAT:  execute.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_FLOAT);   break;
            case DOUBLE: execute.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_DOUBLE);  break;
            case STRING: execute.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_STRING);  break;
            default:     execute.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_OBJECT);
        }
    }

    private void writeToStrings() {
        execute.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_TOSTRING);
    }

    private void writeBinaryInstruction(final ParserRuleContext source, final Type type, final int token) {
        final Sort sort = type.sort;
        final boolean exact = !settings.getNumericOverflow() &&
            ((sort == Sort.INT || sort == Sort.LONG) &&
                (token == MUL || token == DIV || token == ADD || token == SUB) ||
                (sort == Sort.FLOAT || sort == Sort.DOUBLE) &&
                    (token == MUL || token == DIV || token == REM || token == ADD || token == SUB));

        // if its a 64-bit shift, fixup the lastSource argument to truncate to 32-bits
        // note unlike java, this means we still do binary promotion of shifts,
        // but it keeps things simple -- this check works because we promote shifts.
        if (sort == Sort.LONG && (token == LSH || token == USH || token == RSH)) {
            execute.cast(org.objectweb.asm.Type.LONG_TYPE, org.objectweb.asm.Type.INT_TYPE);
        }

        if (exact) {
            switch (sort) {
                case INT:
                    switch (token) {
                        case MUL: execute.invokeStatic(definition.mathType.type,    MULEXACT_INT);     break;
                        case DIV: execute.invokeStatic(definition.utilityType.type, DIVWOOVERLOW_INT); break;
                        case ADD: execute.invokeStatic(definition.mathType.type,    ADDEXACT_INT);     break;
                        case SUB: execute.invokeStatic(definition.mathType.type,    SUBEXACT_INT);     break;
                        default:
                            throw new IllegalStateException(Metadata.error(source) + "Unexpected writer state.");
                    }

                    break;
                case LONG:
                    switch (token) {
                        case MUL: execute.invokeStatic(definition.mathType.type,    MULEXACT_LONG);     break;
                        case DIV: execute.invokeStatic(definition.utilityType.type, DIVWOOVERLOW_LONG); break;
                        case ADD: execute.invokeStatic(definition.mathType.type,    ADDEXACT_LONG);     break;
                        case SUB: execute.invokeStatic(definition.mathType.type,    SUBEXACT_LONG);     break;
                        default:
                            throw new IllegalStateException(Metadata.error(source) + "Unexpected writer state.");
                    }

                    break;
                case FLOAT:
                    switch (token) {
                        case MUL: execute.invokeStatic(definition.utilityType.type, MULWOOVERLOW_FLOAT); break;
                        case DIV: execute.invokeStatic(definition.utilityType.type, DIVWOOVERLOW_FLOAT); break;
                        case REM: execute.invokeStatic(definition.utilityType.type, REMWOOVERLOW_FLOAT); break;
                        case ADD: execute.invokeStatic(definition.utilityType.type, ADDWOOVERLOW_FLOAT); break;
                        case SUB: execute.invokeStatic(definition.utilityType.type, SUBWOOVERLOW_FLOAT); break;
                        default:
                            throw new IllegalStateException(Metadata.error(source) + "Unexpected writer state.");
                    }

                    break;
                case DOUBLE:
                    switch (token) {
                        case MUL: execute.invokeStatic(definition.utilityType.type, MULWOOVERLOW_DOUBLE); break;
                        case DIV: execute.invokeStatic(definition.utilityType.type, DIVWOOVERLOW_DOUBLE); break;
                        case REM: execute.invokeStatic(definition.utilityType.type, REMWOOVERLOW_DOUBLE); break;
                        case ADD: execute.invokeStatic(definition.utilityType.type, ADDWOOVERLOW_DOUBLE); break;
                        case SUB: execute.invokeStatic(definition.utilityType.type, SUBWOOVERLOW_DOUBLE); break;
                        default:
                            throw new IllegalStateException(Metadata.error(source) + "Unexpected writer state.");
                    }

                    break;
                default:
                    throw new IllegalStateException(Metadata.error(source) + "Unexpected writer state.");
            }
        } else {
            if ((sort == Sort.FLOAT || sort == Sort.DOUBLE) &&
                (token == LSH || token == USH || token == RSH || token == BWAND || token == BWXOR || token == BWOR)) {
                throw new IllegalStateException(Metadata.error(source) + "Unexpected writer state.");
            }

            if (sort == Sort.DEF) {
                switch (token) {
                    case MUL:   execute.invokeStatic(definition.defobjType.type, DEF_MUL_CALL); break;
                    case DIV:   execute.invokeStatic(definition.defobjType.type, DEF_DIV_CALL); break;
                    case REM:   execute.invokeStatic(definition.defobjType.type, DEF_REM_CALL); break;
                    case ADD:   execute.invokeStatic(definition.defobjType.type, DEF_ADD_CALL); break;
                    case SUB:   execute.invokeStatic(definition.defobjType.type, DEF_SUB_CALL); break;
                    case LSH:   execute.invokeStatic(definition.defobjType.type, DEF_LSH_CALL); break;
                    case USH:   execute.invokeStatic(definition.defobjType.type, DEF_RSH_CALL); break;
                    case RSH:   execute.invokeStatic(definition.defobjType.type, DEF_USH_CALL); break;
                    case BWAND: execute.invokeStatic(definition.defobjType.type, DEF_AND_CALL); break;
                    case BWXOR: execute.invokeStatic(definition.defobjType.type, DEF_XOR_CALL); break;
                    case BWOR:  execute.invokeStatic(definition.defobjType.type, DEF_OR_CALL);  break;
                    default:
                        throw new IllegalStateException(Metadata.error(source) + "Unexpected writer state.");
                }
            } else {
                switch (token) {
                    case MUL:   execute.math(GeneratorAdapter.MUL,  type.type); break;
                    case DIV:   execute.math(GeneratorAdapter.DIV,  type.type); break;
                    case REM:   execute.math(GeneratorAdapter.REM,  type.type); break;
                    case ADD:   execute.math(GeneratorAdapter.ADD,  type.type); break;
                    case SUB:   execute.math(GeneratorAdapter.SUB,  type.type); break;
                    case LSH:   execute.math(GeneratorAdapter.SHL,  type.type); break;
                    case USH:   execute.math(GeneratorAdapter.USHR, type.type); break;
                    case RSH:   execute.math(GeneratorAdapter.SHR,  type.type); break;
                    case BWAND: execute.math(GeneratorAdapter.AND,  type.type); break;
                    case BWXOR: execute.math(GeneratorAdapter.XOR,  type.type); break;
                    case BWOR:  execute.math(GeneratorAdapter.OR,   type.type); break;
                    default:
                        throw new IllegalStateException(Metadata.error(source) + "Unexpected writer state.");
                }
            }
        }
    }

    /**
     * Called for any compound assignment (including increment/decrement instructions).
     * We have to be stricter than writeBinary, and do overflow checks against the original type's size
     * instead of the promoted type's size, since the result will be implicitly cast back.
     *
     * @return true if an instruction is written, false otherwise
     */
    private boolean writeExactInstruction(final Sort osort, final Sort psort) {
        if (psort == Sort.DOUBLE) {
            if (osort == Sort.FLOAT) {
                execute.invokeStatic(definition.utilityType.type, TOFLOATWOOVERFLOW_DOUBLE);
            } else if (osort == Sort.FLOAT_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOFLOATWOOVERFLOW_DOUBLE);
                execute.checkCast(definition.floatobjType.type);
            } else if (osort == Sort.LONG) {
                execute.invokeStatic(definition.utilityType.type, TOLONGWOOVERFLOW_DOUBLE);
            } else if (osort == Sort.LONG_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOLONGWOOVERFLOW_DOUBLE);
                execute.checkCast(definition.longobjType.type);
            } else if (osort == Sort.INT) {
                execute.invokeStatic(definition.utilityType.type, TOINTWOOVERFLOW_DOUBLE);
            } else if (osort == Sort.INT_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOINTWOOVERFLOW_DOUBLE);
                execute.checkCast(definition.intobjType.type);
            } else if (osort == Sort.CHAR) {
                execute.invokeStatic(definition.utilityType.type, TOCHARWOOVERFLOW_DOUBLE);
            } else if (osort == Sort.CHAR_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOCHARWOOVERFLOW_DOUBLE);
                execute.checkCast(definition.charobjType.type);
            } else if (osort == Sort.SHORT) {
                execute.invokeStatic(definition.utilityType.type, TOSHORTWOOVERFLOW_DOUBLE);
            } else if (osort == Sort.SHORT_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOSHORTWOOVERFLOW_DOUBLE);
                execute.checkCast(definition.shortobjType.type);
            } else if (osort == Sort.BYTE) {
                execute.invokeStatic(definition.utilityType.type, TOBYTEWOOVERFLOW_DOUBLE);
            } else if (osort == Sort.BYTE_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOBYTEWOOVERFLOW_DOUBLE);
                execute.checkCast(definition.byteobjType.type);
            } else {
                return false;
            }
        } else if (psort == Sort.FLOAT) {
            if (osort == Sort.LONG) {
                execute.invokeStatic(definition.utilityType.type, TOLONGWOOVERFLOW_FLOAT);
            } else if (osort == Sort.LONG_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOLONGWOOVERFLOW_FLOAT);
                execute.checkCast(definition.longobjType.type);
            } else if (osort == Sort.INT) {
                execute.invokeStatic(definition.utilityType.type, TOINTWOOVERFLOW_FLOAT);
            } else if (osort == Sort.INT_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOINTWOOVERFLOW_FLOAT);
                execute.checkCast(definition.intobjType.type);
            } else if (osort == Sort.CHAR) {
                execute.invokeStatic(definition.utilityType.type, TOCHARWOOVERFLOW_FLOAT);
            } else if (osort == Sort.CHAR_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOCHARWOOVERFLOW_FLOAT);
                execute.checkCast(definition.charobjType.type);
            } else if (osort == Sort.SHORT) {
                execute.invokeStatic(definition.utilityType.type, TOSHORTWOOVERFLOW_FLOAT);
            } else if (osort == Sort.SHORT_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOSHORTWOOVERFLOW_FLOAT);
                execute.checkCast(definition.shortobjType.type);
            } else if (osort == Sort.BYTE) {
                execute.invokeStatic(definition.utilityType.type, TOBYTEWOOVERFLOW_FLOAT);
            } else if (osort == Sort.BYTE_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOBYTEWOOVERFLOW_FLOAT);
                execute.checkCast(definition.byteobjType.type);
            } else {
                return false;
            }
        } else if (psort == Sort.LONG) {
            if (osort == Sort.INT) {
                execute.invokeStatic(definition.mathType.type, TOINTEXACT_LONG);
            } else if (osort == Sort.INT_OBJ) {
                execute.invokeStatic(definition.mathType.type, TOINTEXACT_LONG);
                execute.checkCast(definition.intobjType.type);
            } else if (osort == Sort.CHAR) {
                execute.invokeStatic(definition.utilityType.type, TOCHAREXACT_LONG);
            } else if (osort == Sort.CHAR_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOCHAREXACT_LONG);
                execute.checkCast(definition.charobjType.type);
            } else if (osort == Sort.SHORT) {
                execute.invokeStatic(definition.utilityType.type, TOSHORTEXACT_LONG);
            } else if (osort == Sort.SHORT_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOSHORTEXACT_LONG);
                execute.checkCast(definition.shortobjType.type);
            } else if (osort == Sort.BYTE) {
                execute.invokeStatic(definition.utilityType.type, TOBYTEEXACT_LONG);
            } else if (osort == Sort.BYTE_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOBYTEEXACT_LONG);
                execute.checkCast(definition.byteobjType.type);
            } else {
                return false;
            }
        } else if (psort == Sort.INT) {
            if (osort == Sort.CHAR) {
                execute.invokeStatic(definition.utilityType.type, TOCHAREXACT_INT);
            } else if (osort == Sort.CHAR_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOCHAREXACT_INT);
                execute.checkCast(definition.charobjType.type);
            } else if (osort == Sort.SHORT) {
                execute.invokeStatic(definition.utilityType.type, TOSHORTEXACT_INT);
            } else if (osort == Sort.SHORT_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOSHORTEXACT_INT);
                execute.checkCast(definition.shortobjType.type);
            } else if (osort == Sort.BYTE) {
                execute.invokeStatic(definition.utilityType.type, TOBYTEEXACT_INT);
            } else if (osort == Sort.BYTE_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOBYTEEXACT_INT);
                execute.checkCast(definition.byteobjType.type);
            } else {
                return false;
            }
        } else {
            return false;
        }

        return true;
    }

    private void writeLoadStoreExternal(final ParserRuleContext source) {
        final Metadata.ExtNodeMetadata sourceenmd = metadata.getExtNodeMetadata(source);
        final Metadata.ExternalMetadata parentemd = metadata.getExternalMetadata(sourceenmd.parent);

        final boolean length = "#length".equals(sourceenmd.target);
        final boolean array = "#brace".equals(sourceenmd.target);
        final boolean name = sourceenmd.target instanceof String && !length && !array;
        final boolean variable = sourceenmd.target instanceof Integer;
        final boolean field = sourceenmd.target instanceof Field;
        final boolean shortcut = sourceenmd.target instanceof Object[];

        if (!length && !variable && !field && !array && !name && !shortcut) {
            throw new IllegalStateException(Metadata.error(source) + "Target not found for load/store.");
        }

        final boolean maplist = shortcut && (boolean)((Object[])sourceenmd.target)[2];
        final Object constant = shortcut ? ((Object[])sourceenmd.target)[3] : null;

        final boolean x1 = field || name || (shortcut && !maplist);
        final boolean x2 = array || (shortcut && maplist);

        if (length) {
            execute.arrayLength();
        } else if (sourceenmd.last && parentemd.storeExpr != null) {
            final Metadata.ExpressionMetadata expremd = metadata.getExpressionMetadata(parentemd.storeExpr);
            final boolean cat = strings.contains(parentemd.storeExpr);

            if (cat) {
                if (field || name || shortcut) {
                    execute.dupX1();
                } else if (array) {
                    execute.dup2X1();
                }

                if (maplist) {
                    if (constant != null) {
                        writeConstant(source, constant);
                    }

                    execute.dupX2();
                }

                writeLoadStoreInstruction(source, false, variable, field, name, array, shortcut);
                writeAppendStrings(sourceenmd.type.sort);
                visit(parentemd.storeExpr);

                if (strings.contains(parentemd.storeExpr)) {
                    writeAppendStrings(expremd.to.sort);
                    strings.remove(parentemd.storeExpr);
                }

                writeToStrings();
                checkWriteCast(source, sourceenmd.castTo);

                if (parentemd.read) {
                    writeDup(sourceenmd.type.sort.size, x1, x2);
                }

                writeLoadStoreInstruction(source, true, variable, field, name, array, shortcut);
            } else if (parentemd.token > 0) {
                final int token = parentemd.token;

                if (field || name || shortcut) {
                    execute.dup();
                } else if (array) {
                    execute.dup2();
                }

                if (maplist) {
                    if (constant != null) {
                        writeConstant(source, constant);
                    }

                    execute.dupX1();
                }

                writeLoadStoreInstruction(source, false, variable, field, name, array, shortcut);

                if (parentemd.read && parentemd.post) {
                    writeDup(sourceenmd.type.sort.size, x1, x2);
                }

                checkWriteCast(source, sourceenmd.castFrom);
                visit(parentemd.storeExpr);

                writeBinaryInstruction(source, sourceenmd.promote, token);

                boolean exact = false;

                if (!settings.getNumericOverflow() && expremd.typesafe && sourceenmd.type.sort != Sort.DEF &&
                    (token == MUL || token == DIV || token == REM || token == ADD || token == SUB)) {
                    exact = writeExactInstruction(sourceenmd.type.sort, sourceenmd.promote.sort);
                }

                if (!exact) {
                    checkWriteCast(source, sourceenmd.castTo);
                }

                if (parentemd.read && !parentemd.post) {
                    writeDup(sourceenmd.type.sort.size, x1, x2);
                }

                writeLoadStoreInstruction(source, true, variable, field, name, array, shortcut);
            } else {
                if (constant != null) {
                    writeConstant(source, constant);
                }

                visit(parentemd.storeExpr);

                if (parentemd.read) {
                    writeDup(sourceenmd.type.sort.size, x1, x2);
                }

                writeLoadStoreInstruction(source, true, variable, field, name, array, shortcut);
            }
        } else {
            if (constant != null) {
                writeConstant(source, constant);
            }

            writeLoadStoreInstruction(source, false, variable, field, name, array, shortcut);
        }
    }

    private void writeLoadStoreInstruction(final ParserRuleContext source,
                                           final boolean store, final boolean variable,
                                           final boolean field, final boolean name,
                                           final boolean array, final boolean shortcut) {
        final Metadata.ExtNodeMetadata sourceemd = metadata.getExtNodeMetadata(source);

        if (variable) {
            writeLoadStoreVariable(source, store, sourceemd.type, (int)sourceemd.target);
        } else if (field) {
            writeLoadStoreField(store, (Field)sourceemd.target);
        } else if (name) {
            writeLoadStoreField(source, store, (String)sourceemd.target);
        } else if (array) {
            writeLoadStoreArray(source, store, sourceemd.type);
        } else if (shortcut) {
            Object[] targets = (Object[])sourceemd.target;
            writeLoadStoreShortcut(store, (Method)targets[0], (Method)targets[1]);
        } else {
            throw new IllegalStateException(Metadata.error(source) + "Load/Store requires a variable, field, or array.");
        }
    }

    private void writeLoadStoreVariable(final ParserRuleContext source, final boolean store,
                                        final Type type, final int slot) {
        if (type.sort == Sort.VOID) {
            throw new IllegalStateException(Metadata.error(source) + "Cannot load/store void type.");
        }

        if (store) {
            execute.visitVarInsn(type.type.getOpcode(Opcodes.ISTORE), slot);
        } else {
            execute.visitVarInsn(type.type.getOpcode(Opcodes.ILOAD), slot);
        }
    }

    private void writeLoadStoreField(final boolean store, final Field field) {
        if (java.lang.reflect.Modifier.isStatic(field.reflect.getModifiers())) {
            if (store) {
                execute.putStatic(field.owner.type, field.reflect.getName(), field.type.type);
            } else {
                execute.getStatic(field.owner.type, field.reflect.getName(), field.type.type);

                if (!field.generic.clazz.equals(field.type.clazz)) {
                    execute.checkCast(field.generic.type);
                }
            }
        } else {
            if (store) {
                execute.putField(field.owner.type, field.reflect.getName(), field.type.type);
            } else {
                execute.getField(field.owner.type, field.reflect.getName(), field.type.type);

                if (!field.generic.clazz.equals(field.type.clazz)) {
                    execute.checkCast(field.generic.type);
                }
            }
        }
    }

    private void writeLoadStoreField(final ParserRuleContext source, final boolean store, final String name) {
        if (store) {
            final Metadata.ExtNodeMetadata sourceemd = metadata.getExtNodeMetadata(source);
            final Metadata.ExternalMetadata parentemd = metadata.getExternalMetadata(sourceemd.parent);
            final Metadata.ExpressionMetadata expremd = metadata.getExpressionMetadata(parentemd.storeExpr);

            execute.push(name);
            execute.loadThis();
            execute.getField(CLASS_TYPE, "definition", DEFINITION_TYPE);
            execute.push(parentemd.token == 0 && expremd.typesafe);
            execute.invokeStatic(definition.defobjType.type, DEF_FIELD_STORE);
        } else {
            execute.push(name);
            execute.loadThis();
            execute.getField(CLASS_TYPE, "definition", DEFINITION_TYPE);
            execute.invokeStatic(definition.defobjType.type, DEF_FIELD_LOAD);
        }
    }

    private void writeLoadStoreArray(final ParserRuleContext source, final boolean store, final Type type) {
        if (type.sort == Sort.VOID) {
            throw new IllegalStateException(Metadata.error(source) + "Cannot load/store void type.");
        }

        if (type.sort == Sort.DEF) {
            final ExtbraceContext bracectx = (ExtbraceContext)source;
            final Metadata.ExpressionMetadata expremd0 = metadata.getExpressionMetadata(bracectx.expression());

            if (store) {
                final Metadata.ExtNodeMetadata braceenmd = metadata.getExtNodeMetadata(bracectx);
                final Metadata.ExternalMetadata parentemd = metadata.getExternalMetadata(braceenmd.parent);
                final Metadata.ExpressionMetadata expremd1 = metadata.getExpressionMetadata(parentemd.storeExpr);

                execute.loadThis();
                execute.getField(CLASS_TYPE, "definition", DEFINITION_TYPE);
                execute.push(expremd0.typesafe);
                execute.push(parentemd.token == 0 && expremd1.typesafe);
                execute.invokeStatic(definition.defobjType.type, DEF_ARRAY_STORE);
            } else {
                execute.loadThis();
                execute.getField(CLASS_TYPE, "definition", DEFINITION_TYPE);
                execute.push(expremd0.typesafe);
                execute.invokeStatic(definition.defobjType.type, DEF_ARRAY_LOAD);
            }
        } else {
            if (store) {
                execute.arrayStore(type.type);
            } else {
                execute.arrayLoad(type.type);
            }
        }
    }

    private void writeLoadStoreShortcut(final boolean store, final Method getter, final Method setter) {
        final Method method = store ? setter : getter;

        if (java.lang.reflect.Modifier.isInterface(getter.owner.clazz.getModifiers())) {
            execute.invokeInterface(method.owner.type, method.method);
        } else {
            execute.invokeVirtual(method.owner.type, method.method);
        }

        if (store) {
            writePop(method.rtn.type.getSize());
        } else if (!method.rtn.clazz.equals(method.handle.type().returnType())) {
            execute.checkCast(method.rtn.type);
        }
    }

    private void writeDup(final int size, final boolean x1, final boolean x2) {
        if (size == 1) {
            if (x2) {
                execute.dupX2();
            } else if (x1) {
                execute.dupX1();
            } else {
                execute.dup();
            }
        } else if (size == 2) {
            if (x2) {
                execute.dup2X2();
            } else if (x1) {
                execute.dup2X1();
            } else {
                execute.dup2();
            }
        }
    }

    private void writeNewExternal(final ExtnewContext source) {
        final Metadata.ExtNodeMetadata sourceenmd = metadata.getExtNodeMetadata(source);
        final Metadata.ExternalMetadata parentemd = metadata.getExternalMetadata(sourceenmd.parent);

        final boolean makearray = "#makearray".equals(sourceenmd.target);
        final boolean constructor = sourceenmd.target instanceof Constructor;

        if (!makearray && !constructor) {
            throw new IllegalStateException(Metadata.error(source) + "Target not found for new call.");
        }

        if (makearray) {
            for (final ExpressionContext exprctx : source.expression()) {
                visit(exprctx);
            }

            if (sourceenmd.type.sort == Sort.ARRAY) {
                execute.visitMultiANewArrayInsn(sourceenmd.type.type.getDescriptor(), sourceenmd.type.type.getDimensions());
            } else {
                execute.newArray(sourceenmd.type.type);
            }
        } else {
            execute.newInstance(sourceenmd.type.type);

            if (parentemd.read) {
                execute.dup();
            }

            for (final ExpressionContext exprctx : source.arguments().expression()) {
                visit(exprctx);
            }

            final Constructor target = (Constructor)sourceenmd.target;
            execute.invokeConstructor(target.owner.type, target.method);
        }
    }

    private void writeCallExternal(final ExtcallContext source) {
        final Metadata.ExtNodeMetadata sourceenmd = metadata.getExtNodeMetadata(source);

        final boolean method = sourceenmd.target instanceof Method;
        final boolean def = sourceenmd.target instanceof String;

        if (!method && !def) {
            throw new IllegalStateException(Metadata.error(source) + "Target not found for call.");
        }

        final List<ExpressionContext> arguments = source.arguments().expression();

        if (method) {
            for (final ExpressionContext exprctx : arguments) {
                visit(exprctx);
            }

            final Method target = (Method)sourceenmd.target;

            if (java.lang.reflect.Modifier.isStatic(target.reflect.getModifiers())) {
                execute.invokeStatic(target.owner.type, target.method);
            } else if (java.lang.reflect.Modifier.isInterface(target.owner.clazz.getModifiers())) {
                execute.invokeInterface(target.owner.type, target.method);
            } else {
                execute.invokeVirtual(target.owner.type, target.method);
            }

            if (!target.rtn.clazz.equals(target.handle.type().returnType())) {
                execute.checkCast(target.rtn.type);
            }
        } else {
            execute.push((String)sourceenmd.target);
            execute.loadThis();
            execute.getField(CLASS_TYPE, "definition", DEFINITION_TYPE);

            execute.push(arguments.size());
            execute.newArray(definition.defType.type);

            for (int argument = 0; argument < arguments.size(); ++argument) {
                execute.dup();
                execute.push(argument);
                visit(arguments.get(argument));
                execute.arrayStore(definition.defType.type);
            }

            execute.push(arguments.size());
            execute.newArray(definition.booleanType.type);

            for (int argument = 0; argument < arguments.size(); ++argument) {
                execute.dup();
                execute.push(argument);
                execute.push(metadata.getExpressionMetadata(arguments.get(argument)).typesafe);
                execute.arrayStore(definition.booleanType.type);
            }

            execute.invokeStatic(definition.defobjType.type, DEF_METHOD_CALL);
        }
    }

    private void writePop(final int size) {
        if (size == 1) {
            execute.pop();
        } else if (size == 2) {
            execute.pop2();
        }
    }

    private void checkWriteCast(final Metadata.ExpressionMetadata sort) {
        checkWriteCast(sort.source, sort.cast);
    }

    private void checkWriteCast(final ParserRuleContext source, final Cast cast) {
        if (cast instanceof Transform) {
            writeTransform((Transform)cast);
        } else if (cast != null) {
            writeCast(cast);
        } else {
            throw new IllegalStateException(Metadata.error(source) + "Unexpected cast object.");
        }
    }

    private void writeCast(final Cast cast) {
        final Type from = cast.from;
        final Type to = cast.to;

        if (from.equals(to)) {
            return;
        }

        if (from.sort.numeric && from.sort.primitive && to.sort.numeric && to.sort.primitive) {
            execute.cast(from.type, to.type);
        } else {
            try {
                from.clazz.asSubclass(to.clazz);
            } catch (ClassCastException exception) {
                execute.checkCast(to.type);
            }
        }
    }

    private void writeTransform(final Transform transform) {
        if (transform.upcast != null) {
            execute.checkCast(transform.upcast.type);
        }

        if (java.lang.reflect.Modifier.isStatic(transform.method.reflect.getModifiers())) {
            execute.invokeStatic(transform.method.owner.type, transform.method.method);
        } else if (java.lang.reflect.Modifier.isInterface(transform.method.owner.clazz.getModifiers())) {
            execute.invokeInterface(transform.method.owner.type, transform.method.method);
        } else {
            execute.invokeVirtual(transform.method.owner.type, transform.method.method);
        }

        if (transform.downcast != null) {
            execute.checkCast(transform.downcast.type);
        }
    }

    void checkWriteBranch(final ParserRuleContext source) {
        final Branch branch = getBranch(source);

        if (branch != null) {
            if (branch.tru != null) {
                execute.visitJumpInsn(Opcodes.IFNE, branch.tru);
            } else if (branch.fals != null) {
                execute.visitJumpInsn(Opcodes.IFEQ, branch.fals);
            }
        }
    }

    private void writeEnd() {
        writer.visitEnd();
    }

    private byte[] getBytes() {
        return writer.toByteArray();
    }
}
