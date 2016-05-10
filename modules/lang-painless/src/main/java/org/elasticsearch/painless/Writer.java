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

import org.antlr.v4.runtime.tree.ParseTree;
import org.elasticsearch.painless.PainlessParser.AfterthoughtContext;
import org.elasticsearch.painless.PainlessParser.ArgumentsContext;
import org.elasticsearch.painless.PainlessParser.AssignmentContext;
import org.elasticsearch.painless.PainlessParser.BinaryContext;
import org.elasticsearch.painless.PainlessParser.BoolContext;
import org.elasticsearch.painless.PainlessParser.BreakContext;
import org.elasticsearch.painless.PainlessParser.CastContext;
import org.elasticsearch.painless.PainlessParser.CompContext;
import org.elasticsearch.painless.PainlessParser.ConditionalContext;
import org.elasticsearch.painless.PainlessParser.ContinueContext;
import org.elasticsearch.painless.PainlessParser.DeclContext;
import org.elasticsearch.painless.PainlessParser.DeclarationContext;
import org.elasticsearch.painless.PainlessParser.DecltypeContext;
import org.elasticsearch.painless.PainlessParser.DeclvarContext;
import org.elasticsearch.painless.PainlessParser.DoContext;
import org.elasticsearch.painless.PainlessParser.EmptyContext;
import org.elasticsearch.painless.PainlessParser.EmptyscopeContext;
import org.elasticsearch.painless.PainlessParser.ExprContext;
import org.elasticsearch.painless.PainlessParser.ExtbraceContext;
import org.elasticsearch.painless.PainlessParser.ExtcallContext;
import org.elasticsearch.painless.PainlessParser.ExtcastContext;
import org.elasticsearch.painless.PainlessParser.ExtdotContext;
import org.elasticsearch.painless.PainlessParser.ExternalContext;
import org.elasticsearch.painless.PainlessParser.ExtfieldContext;
import org.elasticsearch.painless.PainlessParser.ExtnewContext;
import org.elasticsearch.painless.PainlessParser.ExtprecContext;
import org.elasticsearch.painless.PainlessParser.ExtstartContext;
import org.elasticsearch.painless.PainlessParser.ExtstringContext;
import org.elasticsearch.painless.PainlessParser.ExtvarContext;
import org.elasticsearch.painless.PainlessParser.FalseContext;
import org.elasticsearch.painless.PainlessParser.ForContext;
import org.elasticsearch.painless.PainlessParser.GenericContext;
import org.elasticsearch.painless.PainlessParser.IdentifierContext;
import org.elasticsearch.painless.PainlessParser.IfContext;
import org.elasticsearch.painless.PainlessParser.IncrementContext;
import org.elasticsearch.painless.PainlessParser.InitializerContext;
import org.elasticsearch.painless.PainlessParser.MultipleContext;
import org.elasticsearch.painless.PainlessParser.NullContext;
import org.elasticsearch.painless.PainlessParser.NumericContext;
import org.elasticsearch.painless.PainlessParser.PostincContext;
import org.elasticsearch.painless.PainlessParser.PrecedenceContext;
import org.elasticsearch.painless.PainlessParser.PreincContext;
import org.elasticsearch.painless.PainlessParser.ReturnContext;
import org.elasticsearch.painless.PainlessParser.SingleContext;
import org.elasticsearch.painless.PainlessParser.SourceContext;
import org.elasticsearch.painless.PainlessParser.ThrowContext;
import org.elasticsearch.painless.PainlessParser.TrapContext;
import org.elasticsearch.painless.PainlessParser.TrueContext;
import org.elasticsearch.painless.PainlessParser.TryContext;
import org.elasticsearch.painless.PainlessParser.UnaryContext;
import org.elasticsearch.painless.PainlessParser.WhileContext;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.GeneratorAdapter;

import static org.elasticsearch.painless.WriterConstants.BASE_CLASS_TYPE;
import static org.elasticsearch.painless.WriterConstants.CLASS_TYPE;
import static org.elasticsearch.painless.WriterConstants.CONSTRUCTOR;
import static org.elasticsearch.painless.WriterConstants.EXECUTE;
import static org.elasticsearch.painless.WriterConstants.MAP_GET;
import static org.elasticsearch.painless.WriterConstants.MAP_TYPE;
import static org.elasticsearch.painless.WriterConstants.SCORE_ACCESSOR_FLOAT;
import static org.elasticsearch.painless.WriterConstants.SCORE_ACCESSOR_TYPE;

class Writer extends PainlessParserBaseVisitor<Void> {
    static byte[] write(Metadata metadata) {
        final Writer writer = new Writer(metadata);

        return writer.getBytes();
    }

    private final Metadata metadata;
    private final ParseTree root;
    private final String source;
    private final CompilerSettings settings;

    private final ClassWriter writer;
    private final GeneratorAdapter execute;

    private final WriterStatement statement;
    private final WriterExpression expression;
    private final WriterExternal external;

    private Writer(final Metadata metadata) {
        this.metadata = metadata;
        root = metadata.root;
        source = metadata.source;
        settings = metadata.settings;

        writer = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);

        writeBegin();
        writeConstructor();

        execute = new GeneratorAdapter(Opcodes.ACC_PUBLIC, EXECUTE, null, null, writer);

        final WriterUtility utility = new WriterUtility(metadata, execute);
        final WriterCaster caster = new WriterCaster(execute);

        statement = new WriterStatement(metadata, execute, this, utility);
        expression = new WriterExpression(metadata, execute, this, utility, caster);
        external = new WriterExternal(metadata, execute, this, utility, caster);

        writeExecute();
        writeEnd();
    }

    private void writeBegin() {
        final int version = Opcodes.V1_7;
        final int access = Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | Opcodes.ACC_FINAL;
        final String base = BASE_CLASS_TYPE.getInternalName();
        final String name = CLASS_TYPE.getInternalName();

        // apply marker interface NeedsScore if we use the score!
        final String interfaces[];
        if (metadata.scoreValueUsed) {
            interfaces = new String[] { WriterConstants.NEEDS_SCORE_TYPE.getInternalName() };
        } else {
            interfaces = null;
        }
        writer.visit(version, access, name, null, base, interfaces);
        writer.visitSource(source, null);
    }

    private void writeConstructor() {
        final GeneratorAdapter constructor = new GeneratorAdapter(Opcodes.ACC_PUBLIC, CONSTRUCTOR, null, null, writer);
        constructor.loadThis();
        constructor.loadArgs();
        constructor.invokeConstructor(org.objectweb.asm.Type.getType(Executable.class), CONSTRUCTOR);
        constructor.returnValue();
        constructor.endMethod();
    }

    private void writeExecute() {
        final Label fals = new Label();
        final Label end = new Label();

        if (metadata.scoreValueUsed) {
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
        }

        execute.push(settings.getMaxLoopCounter());
        execute.visitVarInsn(Opcodes.ISTORE, metadata.loopCounterSlot);

        visit(root);
        execute.endMethod();
    }

    private void writeEnd() {
        writer.visitEnd();
    }

    private byte[] getBytes() {
        return writer.toByteArray();
    }

    @Override
    public Void visitSource(final SourceContext ctx) {
        statement.processSource(ctx);

        return null;
    }

    @Override
    public Void visitIf(final IfContext ctx) {
        statement.processIf(ctx);

        return null;
    }

    @Override
    public Void visitWhile(final WhileContext ctx) {
        statement.processWhile(ctx);

        return null;
    }

    @Override
    public Void visitDo(final DoContext ctx) {
        statement.processDo(ctx);

        return null;
    }

    @Override
    public Void visitFor(final ForContext ctx) {
        statement.processFor(ctx);

        return null;
    }

    @Override
    public Void visitDecl(final DeclContext ctx) {
        statement.processDecl(ctx);

        return null;
    }

    @Override
    public Void visitContinue(final ContinueContext ctx) {
        statement.processContinue();

        return null;
    }

    @Override
    public Void visitBreak(final BreakContext ctx) {
        statement.processBreak();

        return null;
    }

    @Override
    public Void visitReturn(final ReturnContext ctx) {
        statement.processReturn(ctx);

        return null;
    }

    @Override
    public Void visitTry(final TryContext ctx) {
        statement.processTry(ctx);

        return null;
    }

    @Override
    public Void visitThrow(final ThrowContext ctx) {
        statement.processThrow(ctx);

        return null;
    }

    @Override
    public Void visitExpr(final ExprContext ctx) {
        statement.processExpr(ctx);

        return null;
    }

    @Override
    public Void visitMultiple(final MultipleContext ctx) {
        statement.processMultiple(ctx);

        return null;
    }

    @Override
    public Void visitSingle(final SingleContext ctx) {
        statement.processSingle(ctx);

        return null;
    }

    @Override
    public Void visitEmpty(final EmptyContext ctx) {
        throw new UnsupportedOperationException(WriterUtility.error(ctx) + "Unexpected state.");
    }

    @Override
    public Void visitEmptyscope(final EmptyscopeContext ctx) {
        throw new UnsupportedOperationException(WriterUtility.error(ctx) + "Unexpected state.");
    }

    @Override
    public Void visitInitializer(final InitializerContext ctx) {
        statement.processInitializer(ctx);

        return null;
    }

    @Override
    public Void visitAfterthought(final AfterthoughtContext ctx) {
        statement.processAfterthought(ctx);

        return null;
    }

    @Override
    public Void visitDeclaration(DeclarationContext ctx) {
        statement.processDeclaration(ctx);

        return null;
    }

    @Override
    public Void visitDecltype(final DecltypeContext ctx) {
        throw new UnsupportedOperationException(WriterUtility.error(ctx) + "Unexpected state.");
    }

    @Override
    public Void visitDeclvar(final DeclvarContext ctx) {
        statement.processDeclvar(ctx);

        return null;
    }

    @Override
    public Void visitTrap(final TrapContext ctx) {
        statement.processTrap(ctx);

        return null;
    }

    @Override
    public Void visitIdentifier(IdentifierContext ctx) {
        throw new UnsupportedOperationException(WriterUtility.error(ctx) + "Unexpected state.");
    }

    @Override
    public Void visitGeneric(GenericContext ctx) {
        throw new UnsupportedOperationException(WriterUtility.error(ctx) + "Unexpected state.");
    }

    @Override
    public Void visitPrecedence(final PrecedenceContext ctx) {
        throw new UnsupportedOperationException(WriterUtility.error(ctx) + "Unexpected state.");
    }

    @Override
    public Void visitNumeric(final NumericContext ctx) {
        expression.processNumeric(ctx);

        return null;
    }

    @Override
    public Void visitTrue(final TrueContext ctx) {
        expression.processTrue(ctx);

        return null;
    }

    @Override
    public Void visitFalse(final FalseContext ctx) {
        expression.processFalse(ctx);

        return null;
    }

    @Override
    public Void visitNull(final NullContext ctx) {
        expression.processNull(ctx);

        return null;
    }

    @Override
    public Void visitExternal(final ExternalContext ctx) {
        expression.processExternal(ctx);

        return null;
    }


    @Override
    public Void visitPostinc(final PostincContext ctx) {
        expression.processPostinc(ctx);

        return null;
    }

    @Override
    public Void visitPreinc(final PreincContext ctx) {
        expression.processPreinc(ctx);

        return null;
    }

    @Override
    public Void visitUnary(final UnaryContext ctx) {
        expression.processUnary(ctx);

        return null;
    }

    @Override
    public Void visitCast(final CastContext ctx) {
        expression.processCast(ctx);

        return null;
    }

    @Override
    public Void visitBinary(final BinaryContext ctx) {
        expression.processBinary(ctx);

        return null;
    }

    @Override
    public Void visitComp(final CompContext ctx) {
        expression.processComp(ctx);

        return null;
    }

    @Override
    public Void visitBool(final BoolContext ctx) {
        expression.processBool(ctx);

        return null;
    }

    @Override
    public Void visitConditional(final ConditionalContext ctx) {
        expression.processConditional(ctx);

        return null;
    }

    @Override
    public Void visitAssignment(final AssignmentContext ctx) {
        expression.processAssignment(ctx);

        return null;
    }

    @Override
    public Void visitExtstart(final ExtstartContext ctx) {
        external.processExtstart(ctx);

        return null;
    }

    @Override
    public Void visitExtprec(final ExtprecContext ctx) {
        external.processExtprec(ctx);

        return null;
    }

    @Override
    public Void visitExtcast(final ExtcastContext ctx) {
        external.processExtcast(ctx);

        return null;
    }

    @Override
    public Void visitExtbrace(final ExtbraceContext ctx) {
        external.processExtbrace(ctx);

        return null;
    }

    @Override
    public Void visitExtdot(final ExtdotContext ctx) {
        external.processExtdot(ctx);

        return null;
    }

    @Override
    public Void visitExtcall(final ExtcallContext ctx) {
        external.processExtcall(ctx);

        return null;
    }

    @Override
    public Void visitExtvar(final ExtvarContext ctx) {
        external.processExtvar(ctx);

        return null;
    }

    @Override
    public Void visitExtfield(final ExtfieldContext ctx) {
        external.processExtfield(ctx);

        return null;
    }

    @Override
    public Void visitExtnew(final ExtnewContext ctx) {
        external.processExtnew(ctx);

        return null;
    }

    @Override
    public Void visitExtstring(final ExtstringContext ctx) {
        external.processExtstring(ctx);

        return null;
    }

    @Override
    public Void visitArguments(final ArgumentsContext ctx) {
        throw new UnsupportedOperationException(WriterUtility.error(ctx) + "Unexpected state.");
    }

    @Override
    public Void visitIncrement(final IncrementContext ctx) {
        expression.processIncrement(ctx);

        return null;
    }
}
