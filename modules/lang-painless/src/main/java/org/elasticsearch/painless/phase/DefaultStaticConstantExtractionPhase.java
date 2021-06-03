/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.phase;

import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ConstantNode;
import org.elasticsearch.painless.ir.FieldNode;
import org.elasticsearch.painless.symbol.IRDecorations.IRDConstant;
import org.elasticsearch.painless.symbol.IRDecorations.IRDConstantFieldName;
import org.elasticsearch.painless.symbol.IRDecorations.IRDExpressionType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDFieldType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDModifiers;
import org.elasticsearch.painless.symbol.IRDecorations.IRDName;
import org.elasticsearch.painless.symbol.ScriptScope;

import java.lang.reflect.Modifier;

/**
 * Looks for {@link ConstantNode}s that can't be pushed into the constant pool
 * and creates a {@code static} constant member that is injected using reflection
 * on construction.
 */
public class DefaultStaticConstantExtractionPhase extends IRTreeBaseVisitor<ScriptScope> {
    private ClassNode classNode;

    @Override
    public void visitClass(ClassNode irClassNode, ScriptScope scope) {
        this.classNode = irClassNode;
        super.visitClass(irClassNode, scope);
    }

    @Override
    public void visitConstant(ConstantNode irConstantNode, ScriptScope scope) {
        super.visitConstant(irConstantNode, scope);
        Object constant = irConstantNode.getDecorationValue(IRDConstant.class);
        if (constant instanceof String
            || constant instanceof Double
            || constant instanceof Float
            || constant instanceof Long
            || constant instanceof Integer
            || constant instanceof Character
            || constant instanceof Short
            || constant instanceof Byte
            || constant instanceof Boolean) {
            /*
             * Constant can be loaded into the constant pool so we let the byte
             * code generation phase do that.
             */
            return;
        }
        /*
         * The constant *can't* be loaded into the constant pool so we make it
         * a static constant and register the value with ScriptScope. The byte
         * code generation will load the static constant.
         */
        String fieldName = scope.getNextSyntheticName("constant");
        scope.addStaticConstant(fieldName, constant);

        FieldNode constantField = new FieldNode(irConstantNode.getLocation());
        constantField.attachDecoration(new IRDModifiers(Modifier.PUBLIC | Modifier.STATIC));
        constantField.attachDecoration(irConstantNode.getDecoration(IRDConstant.class));
        Class<?> type = irConstantNode.getDecorationValue(IRDExpressionType.class);
        constantField.attachDecoration(new IRDFieldType(type));
        constantField.attachDecoration(new IRDName(fieldName));
        classNode.addFieldNode(constantField);

        irConstantNode.attachDecoration(new IRDConstantFieldName(fieldName));
    }
}
