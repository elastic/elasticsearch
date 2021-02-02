/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
