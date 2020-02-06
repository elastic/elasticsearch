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
import org.elasticsearch.painless.symbol.ScopeTable;

public class ConstantNode extends ExpressionNode {

    /* ---- begin node data ---- */

    private Object constant;

    public void setConstant(Object constant) {
        this.constant = constant;
    }

    public Object getConstant() {
        return constant;
    }

    /* ---- end node data ---- */

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals, ScopeTable scopeTable) {
        if      (constant instanceof String)    methodWriter.push((String)constant);
        else if (constant instanceof Double)    methodWriter.push((double)constant);
        else if (constant instanceof Float)     methodWriter.push((float)constant);
        else if (constant instanceof Long)      methodWriter.push((long)constant);
        else if (constant instanceof Integer)   methodWriter.push((int)constant);
        else if (constant instanceof Character) methodWriter.push((char)constant);
        else if (constant instanceof Short)     methodWriter.push((short)constant);
        else if (constant instanceof Byte)      methodWriter.push((byte)constant);
        else if (constant instanceof Boolean)   methodWriter.push((boolean)constant);
        else {
            throw new IllegalStateException("unexpected constant [" + constant + "]");
        }
    }
}
