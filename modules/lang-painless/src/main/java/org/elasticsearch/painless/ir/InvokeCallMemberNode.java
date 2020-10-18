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

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.lookup.PainlessClassBinding;
import org.elasticsearch.painless.lookup.PainlessInstanceBinding;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.phase.IRTreeVisitor;
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;

public class InvokeCallMemberNode extends ArgumentsNode {

    /* ---- begin node data ---- */

    private LocalFunction localFunction;
    private PainlessMethod importedMethod;
    private PainlessClassBinding classBinding;
    private int classBindingOffset;
    private PainlessInstanceBinding instanceBinding;
    private String bindingName;

    public void setLocalFunction(LocalFunction localFunction) {
        this.localFunction = localFunction;
    }

    public LocalFunction getLocalFunction() {
        return localFunction;
    }

    public void setImportedMethod(PainlessMethod importedMethod) {
        this.importedMethod = importedMethod;
    }

    public PainlessMethod getImportedMethod() {
        return importedMethod;
    }

    public void setClassBinding(PainlessClassBinding classBinding) {
        this.classBinding = classBinding;
    }

    public PainlessClassBinding getClassBinding() {
        return classBinding;
    }

    public void setClassBindingOffset(int classBindingOffset) {
        this.classBindingOffset = classBindingOffset;
    }

    public int getClassBindingOffset() {
        return classBindingOffset;
    }

    public void setInstanceBinding(PainlessInstanceBinding instanceBinding) {
        this.instanceBinding = instanceBinding;
    }

    public PainlessInstanceBinding getInstanceBinding() {
        return instanceBinding;
    }

    public void setBindingName(String bindingName) {
        this.bindingName = bindingName;
    }

    public String getBindingName() {
        return bindingName;
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitInvokeCallMember(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        for (ExpressionNode argumentNode : getArgumentNodes()) {
            argumentNode.visit(irTreeVisitor, scope);
        }
    }

    /* ---- end visitor ---- */

    public InvokeCallMemberNode(Location location) {
        super(location);
    }

}
