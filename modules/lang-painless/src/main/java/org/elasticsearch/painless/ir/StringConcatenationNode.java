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

package org.elasticsearch.painless.ir;

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.phase.IRTreeVisitor;
import org.elasticsearch.painless.symbol.WriteScope;

public class StringConcatenationNode extends ArgumentsNode {

    /* ---- begin node data ---- */

    private boolean cat;

    public void setCat(boolean cat) {
        this.cat = cat;
    }

    public boolean getCat() {
        return cat;
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitStringConcatenation(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {

    }

    /* ---- end visitor ---- */

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, WriteScope writeScope) {
        methodWriter.writeDebugInfo(location);

        if (cat == false) {
            methodWriter.writeNewStrings();
        }

        ExpressionNode leftNode = getArgumentNodes().get(0);
        leftNode.write(classWriter, methodWriter, writeScope);

        if (leftNode instanceof StringConcatenationNode == false || ((StringConcatenationNode)leftNode).getCat() == false) {
            methodWriter.writeAppendStrings(leftNode.getExpressionType());
        }

        ExpressionNode rightNode = getArgumentNodes().get(1);
        rightNode.write(classWriter, methodWriter, writeScope);

        if (rightNode instanceof StringConcatenationNode == false || ((StringConcatenationNode)rightNode).getCat() == false) {
            methodWriter.writeAppendStrings(rightNode.getExpressionType());
        }

        if (cat == false) {
            methodWriter.writeToStrings();
        }
    }
}
