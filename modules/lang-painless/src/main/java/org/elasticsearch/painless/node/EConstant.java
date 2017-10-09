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

import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Globals;

import java.util.Set;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.MethodWriter;

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
    void analyze(Locals locals) {
        if (constant instanceof String) {
            actual = Definition.STRING_TYPE;
        } else if (constant instanceof Double) {
            actual = Definition.DOUBLE_TYPE;
        } else if (constant instanceof Float) {
            actual = Definition.FLOAT_TYPE;
        } else if (constant instanceof Long) {
            actual = Definition.LONG_TYPE;
        } else if (constant instanceof Integer) {
            actual = Definition.INT_TYPE;
        } else if (constant instanceof Character) {
            actual = Definition.CHAR_TYPE;
        } else if (constant instanceof Short) {
            actual = Definition.SHORT_TYPE;
        } else if (constant instanceof Byte) {
            actual = Definition.BYTE_TYPE;
        } else if (constant instanceof Boolean) {
            actual = Definition.BOOLEAN_TYPE;
        } else {
            throw createError(new IllegalStateException("Illegal tree structure."));
        }
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        if      (actual.clazz == String.class) writer.push((String)constant);
        else if (actual.clazz == double.class) writer.push((double)constant);
        else if (actual.clazz == float.class) writer.push((float)constant);
        else if (actual.clazz == long.class) writer.push((long)constant);
        else if (actual.clazz == int.class) writer.push((int)constant);
        else if (actual.clazz == char.class) writer.push((char)constant);
        else if (actual.clazz == short.class) writer.push((short)constant);
        else if (actual.clazz == byte.class) writer.push((byte)constant);
        else if (actual.clazz == boolean.class) writer.push((boolean)constant);
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
