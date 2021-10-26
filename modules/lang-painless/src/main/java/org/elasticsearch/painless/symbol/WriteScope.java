/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.symbol;

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;

import java.util.HashMap;
import java.util.Map;

public class WriteScope {

    public static class Variable {

        protected final Class<?> type;
        protected final Type asmType;
        protected final String name;
        protected final int slot;

        public Variable(Class<?> type, String name, int slot) {
            this.type = type;
            this.asmType = MethodWriter.getType(type);
            this.name = name;
            this.slot = slot;
        }

        public Class<?> getType() {
            return type;
        }

        public String getCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(type);
        }

        public Type getAsmType() {
            return asmType;
        }

        public String getName() {
            return name;
        }

        public int getSlot() {
            return slot;
        }
    }

    protected final WriteScope parent;
    protected final ClassWriter classWriter;
    protected final MethodWriter methodWriter;
    protected final Label continueLabel;
    protected final Label breakLabel;
    protected final Label tryBeginLabel;
    protected final Label tryEndLabel;
    protected final Label catchesEndLabel;
    protected final Map<String, Variable> variables = new HashMap<>();
    protected int nextSlot;

    protected WriteScope() {
        this.parent = null;
        this.classWriter = null;
        this.methodWriter = null;
        this.continueLabel = null;
        this.breakLabel = null;
        this.tryBeginLabel = null;
        this.tryEndLabel = null;
        this.catchesEndLabel = null;
        this.nextSlot = 0;
    }

    protected WriteScope(WriteScope parent, ClassWriter classWriter) {
        this.parent = parent;
        this.classWriter = classWriter;
        this.methodWriter = parent.methodWriter;
        this.continueLabel = parent.continueLabel;
        this.breakLabel = parent.breakLabel;
        this.tryBeginLabel = parent.tryBeginLabel;
        this.tryEndLabel = parent.tryEndLabel;
        this.catchesEndLabel = parent.catchesEndLabel;
        this.nextSlot = parent.nextSlot;
    }

    protected WriteScope(WriteScope parent, MethodWriter methodWriter) {
        this.parent = parent;
        this.classWriter = parent.classWriter;
        this.methodWriter = methodWriter;
        this.continueLabel = parent.continueLabel;
        this.breakLabel = parent.breakLabel;
        this.tryBeginLabel = parent.tryBeginLabel;
        this.tryEndLabel = parent.tryEndLabel;
        this.catchesEndLabel = parent.catchesEndLabel;
        this.nextSlot = parent.nextSlot;
    }

    protected WriteScope(WriteScope parent, Label continueLabel, Label breakLabel) {
        this.parent = parent;
        this.classWriter = parent.classWriter;
        this.methodWriter = parent.methodWriter;
        this.continueLabel = continueLabel;
        this.breakLabel = breakLabel;
        this.tryBeginLabel = parent.tryBeginLabel;
        this.tryEndLabel = parent.tryEndLabel;
        this.catchesEndLabel = parent.catchesEndLabel;
        this.nextSlot = parent.nextSlot;
    }

    protected WriteScope(WriteScope parent, Label tryBeginLabel, Label tryEndLabel, Label catchesEndLabel) {
        this.parent = parent;
        this.classWriter = parent.classWriter;
        this.methodWriter = parent.methodWriter;
        this.continueLabel = parent.continueLabel;
        this.breakLabel = parent.breakLabel;
        this.tryBeginLabel = tryBeginLabel;
        this.tryEndLabel = tryEndLabel;
        this.catchesEndLabel = catchesEndLabel;
        this.nextSlot = parent.nextSlot;
    }

    protected WriteScope(WriteScope parent, boolean isTryBlock) {
        this.parent = parent;
        this.classWriter = parent.classWriter;
        this.methodWriter = parent.methodWriter;
        this.continueLabel = parent.continueLabel;
        this.breakLabel = parent.breakLabel;
        this.tryBeginLabel = isTryBlock ? null : parent.tryBeginLabel;
        this.tryEndLabel = isTryBlock ? null : parent.tryEndLabel;
        this.catchesEndLabel = isTryBlock ? null : parent.catchesEndLabel;
        this.nextSlot = parent.nextSlot;
    }

     /** Creates a script scope as the top-level scope with no labels and parameters. */
    public static WriteScope newScriptScope() {
        return new WriteScope();
    }

    /** Creates a class scope with the script scope as a parent. */
    public WriteScope newClassScope(ClassWriter classWriter) {
        return new WriteScope(this, classWriter);
    }

    /** Creates a method scope with the class scope as a parent and parameters from the method signature. */
    public WriteScope newMethodScope(MethodWriter methodWriter) {
        return new WriteScope(this, methodWriter);
    }

    /** Creates a loop scope with labels for where continue and break instructions should jump to. */
    public WriteScope newLoopScope(Label continueLabel, Label breakLabel) {
        return new WriteScope(this, continueLabel, breakLabel);
    }

    /** Creates a try scope with labels for where and exception should jump to. */
    public WriteScope newTryScope(Label tryBeginLabel, Label tryEndLabel, Label catchesEndLabel) {
        return new WriteScope(this, tryBeginLabel, tryEndLabel, catchesEndLabel);
    }

    /** Creates a block scope where if the block is for a try block the catch labels are removed. */
    public WriteScope newBlockScope(boolean isTryBlock) {
        return new WriteScope(this, isTryBlock);
    }

    /** Creates a block scope to encapsulate variable declarations appropriately. */
    public WriteScope newBlockScope() {
        return newBlockScope(false);
    }

    public ClassWriter getClassWriter() {
        return classWriter;
    }

    public MethodWriter getMethodWriter() {
        return methodWriter;
    }

    public Label getContinueLabel() {
        return continueLabel;
    }

    public Label getBreakLabel() {
        return breakLabel;
    }

    public Label getTryBeginLabel() {
        return tryBeginLabel;
    }

    public Label getTryEndLabel() {
        return tryEndLabel;
    }

    public Label getCatchesEndLabel() {
        return catchesEndLabel;
    }

    public Variable defineVariable(Class<?> type, String name) {
        Variable variable = new Variable(type, name, nextSlot);
        nextSlot += variable.getAsmType().getSize();
        variables.put(name, variable);

        return variable;
    }

    /**
     * Prepends the character '#' to the variable name. The '#' is
     * reserved and ensures that these internal variables aren't
     * accessed by a normal consumer.
     */
    public Variable defineInternalVariable(Class<?> type, String name) {
        return defineVariable(type, "#" + name);
    }

    public Variable getVariable(String name) {
        Variable variable = variables.get(name);

        if (variable == null && parent != null) {
            variable = parent.getVariable(name);
        }

        return variable;
    }

    /**
     * Prepends the character '#' to the variable name. The '#' is
     * reserved and ensures that these internal variables aren't
     * accessed by a normal consumer.
     */
    public Variable getInternalVariable(String name) {
        return getVariable("#" + name);
    }
}
