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
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.MethodWriter;

public class NewArrayNode extends ArgumentsNode {

    /* ---- begin node data ---- */

    protected boolean initialize;

    public NewArrayNode setInitialize(boolean initialize) {
        this.initialize = initialize;
        return this;
    }

    public boolean getInitialize() {
        return initialize;
    }

    /* ---- end node data ---- */

    public NewArrayNode() {
        // do nothing
    }

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        if (initialize) {
            methodWriter.push(argumentNodes.size());
            methodWriter.newArray(MethodWriter.getType(getType().getComponentType()));

            for (int index = 0; index < argumentNodes.size(); ++index) {
                ExpressionNode argumentNode = argumentNodes.get(index);

                methodWriter.dup();
                methodWriter.push(index);
                argumentNode.write(classWriter, methodWriter, globals);
                methodWriter.arrayStore(MethodWriter.getType(getType().getComponentType()));
            }
        } else {
            for (ExpressionNode argumentNode : argumentNodes) {
                argumentNode.write(classWriter, methodWriter, globals);
            }

            if (argumentNodes.size() > 1) {
                methodWriter.visitMultiANewArrayInsn(MethodWriter.getType(getType()).getDescriptor(), argumentNodes.size());
            } else {
                methodWriter.newArray(MethodWriter.getType(getType().getComponentType()));
            }
        }
    }
}
