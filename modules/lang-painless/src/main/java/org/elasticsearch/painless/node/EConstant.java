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

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.ScriptRoot;

import java.util.Set;

/**
 * Represents a constant inserted into the tree replacing
 * other constants during constant folding.  (Internal only.)
 */
final class EConstant extends AExpression {

    EConstant(Location location, Object constant) {
        super(location);

        this.constant = constant;
    }

    @Override
    void extractVariables(Set<String> variables) {
        throw new IllegalStateException("Illegal tree structure.");
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Locals locals) {
        if (constant instanceof String) {
            actual = String.class;
        } else if (constant instanceof Double) {
            actual = double.class;
        } else if (constant instanceof Float) {
            actual = float.class;
        } else if (constant instanceof Long) {
            actual = long.class;
        } else if (constant instanceof Integer) {
            actual = int.class;
        } else if (constant instanceof Character) {
            actual = char.class;
        } else if (constant instanceof Short) {
            actual = short.class;
        } else if (constant instanceof Byte) {
            actual = byte.class;
        } else if (constant instanceof Boolean) {
            actual = boolean.class;
        } else {
            throw createError(new IllegalStateException("Illegal tree structure."));
        }
    }

    @Override
    void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        if      (actual == String.class) methodWriter.push((String)constant);
        else if (actual == double.class) methodWriter.push((double)constant);
        else if (actual == float.class) methodWriter.push((float)constant);
        else if (actual == long.class) methodWriter.push((long)constant);
        else if (actual == int.class) methodWriter.push((int)constant);
        else if (actual == char.class) methodWriter.push((char)constant);
        else if (actual == short.class) methodWriter.push((short)constant);
        else if (actual == byte.class) methodWriter.push((byte)constant);
        else if (actual == boolean.class) methodWriter.push((boolean)constant);
        else {
            throw createError(new IllegalStateException("Illegal tree structure."));
        }
    }

    @Override
    public String toString() {
        String c = constant.toString();
        if (constant instanceof String) {
            c = "'" + c + "'";
        }
        return singleLineToString(constant.getClass().getSimpleName(), c);
    }
}
