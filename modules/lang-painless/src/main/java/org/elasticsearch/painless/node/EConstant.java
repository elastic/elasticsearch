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
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.MethodWriter;

/**
 * Represents a constant.  Note this replaces any other expression
 * node with a constant value set during a cast.  (Internal only.)
 */
final class EConstant extends AExpression {

    EConstant(Location location, Object constant) {
        super(location);

        this.constant = constant;
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
    void write(MethodWriter writer) {
        Sort sort = actual.sort;

        switch (sort) {
            case STRING: writer.push((String)constant);  break;
            case DOUBLE: writer.push((double)constant);  break;
            case FLOAT:  writer.push((float)constant);   break;
            case LONG:   writer.push((long)constant);    break;
            case INT:    writer.push((int)constant);     break;
            case CHAR:   writer.push((char)constant);    break;
            case SHORT:  writer.push((short)constant);   break;
            case BYTE:   writer.push((byte)constant);    break;
            case BOOL:
                if (tru != null && (boolean)constant) {
                    writer.goTo(tru);
                } else if (fals != null && !(boolean)constant) {
                    writer.goTo(fals);
                } else if (tru == null && fals == null) {
                    writer.push((boolean)constant);
                }

                break;
            default:
                throw createError(new IllegalStateException("Illegal tree structure."));
        }

        if (sort != Sort.BOOL) {
            writer.writeBranch(tru, fals);
        }
    }
}
