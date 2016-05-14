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

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Variables;
import org.elasticsearch.painless.WriterUtility;
import org.objectweb.asm.commons.GeneratorAdapter;

/**
 * Respresents a constant.  Note this replaces any other expression
 * node with a constant value set during a cast.  (Internal only.)
 */
final class EConstant extends AExpression {

    EConstant(final int line, final String location, final Object constant) {
        super(line, location);

        this.constant = constant;
    }

    @Override
    void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        if (constant instanceof String) {
            actual = definition.stringType;
        } else if (constant instanceof Double) {
            actual = definition.doubleType;
        } else if (constant instanceof Float) {
            actual = definition.floatType;
        } else if (constant instanceof Long) {
            actual = definition.longType;
        } else if (constant instanceof Integer) {
            actual = definition.intType;
        } else if (constant instanceof Character) {
            actual = definition.charType;
        } else if (constant instanceof Short) {
            actual = definition.shortType;
        } else if (constant instanceof Byte) {
            actual = definition.byteType;
        } else if (constant instanceof Boolean) {
            actual = definition.booleanType;
        } else {
            throw new IllegalStateException(error("Illegal tree structure."));
        }
    }

    @Override
    void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        final Sort sort = actual.sort;

        switch (sort) {
            case STRING: adapter.push((String)constant);  break;
            case DOUBLE: adapter.push((double)constant);  break;
            case FLOAT:  adapter.push((float)constant);   break;
            case LONG:   adapter.push((long)constant);    break;
            case INT:    adapter.push((int)constant);     break;
            case CHAR:   adapter.push((char)constant);    break;
            case SHORT:  adapter.push((short)constant);   break;
            case BYTE:   adapter.push((byte)constant);    break;
            case BOOL:
                if (tru != null && (boolean)constant) {
                    adapter.goTo(tru);
                } else if (fals != null && !(boolean)constant) {
                    adapter.goTo(fals);
                } else if (tru == null && fals == null) {
                    adapter.push((boolean)constant);
                }

                break;
            default:
                throw new IllegalStateException(error("Illegal tree structure."));
        }

        if (sort != Sort.BOOL) {
            WriterUtility.writeBranch(adapter, tru, fals);
        }
    }
}
