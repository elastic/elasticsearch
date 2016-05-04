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

package org.elasticsearch.painless.tree.writer;

import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Cast;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Transform;
import org.elasticsearch.painless.Definition.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import static org.elasticsearch.painless.tree.writer.Constants.STRINGBUILDER_APPEND_BOOLEAN;
import static org.elasticsearch.painless.tree.writer.Constants.STRINGBUILDER_APPEND_CHAR;
import static org.elasticsearch.painless.tree.writer.Constants.STRINGBUILDER_APPEND_DOUBLE;
import static org.elasticsearch.painless.tree.writer.Constants.STRINGBUILDER_APPEND_FLOAT;
import static org.elasticsearch.painless.tree.writer.Constants.STRINGBUILDER_APPEND_INT;
import static org.elasticsearch.painless.tree.writer.Constants.STRINGBUILDER_APPEND_LONG;
import static org.elasticsearch.painless.tree.writer.Constants.STRINGBUILDER_APPEND_OBJECT;
import static org.elasticsearch.painless.tree.writer.Constants.STRINGBUILDER_APPEND_STRING;
import static org.elasticsearch.painless.tree.writer.Constants.STRINGBUILDER_CONSTRUCTOR;
import static org.elasticsearch.painless.tree.writer.Constants.STRINGBUILDER_TOSTRING;
import static org.elasticsearch.painless.tree.writer.Constants.STRINGBUILDER_TYPE;

import static org.elasticsearch.painless.tree.writer.Constants.TOBYTEEXACT_INT;
import static org.elasticsearch.painless.tree.writer.Constants.TOBYTEEXACT_LONG;
import static org.elasticsearch.painless.tree.writer.Constants.TOBYTEWOOVERFLOW_DOUBLE;
import static org.elasticsearch.painless.tree.writer.Constants.TOBYTEWOOVERFLOW_FLOAT;
import static org.elasticsearch.painless.tree.writer.Constants.TOCHAREXACT_INT;
import static org.elasticsearch.painless.tree.writer.Constants.TOCHAREXACT_LONG;
import static org.elasticsearch.painless.tree.writer.Constants.TOCHARWOOVERFLOW_DOUBLE;
import static org.elasticsearch.painless.tree.writer.Constants.TOCHARWOOVERFLOW_FLOAT;
import static org.elasticsearch.painless.tree.writer.Constants.TOFLOATWOOVERFLOW_DOUBLE;
import static org.elasticsearch.painless.tree.writer.Constants.TOINTEXACT_LONG;
import static org.elasticsearch.painless.tree.writer.Constants.TOINTWOOVERFLOW_DOUBLE;
import static org.elasticsearch.painless.tree.writer.Constants.TOINTWOOVERFLOW_FLOAT;
import static org.elasticsearch.painless.tree.writer.Constants.TOLONGWOOVERFLOW_DOUBLE;
import static org.elasticsearch.painless.tree.writer.Constants.TOLONGWOOVERFLOW_FLOAT;
import static org.elasticsearch.painless.tree.writer.Constants.TOSHORTEXACT_INT;
import static org.elasticsearch.painless.tree.writer.Constants.TOSHORTEXACT_LONG;
import static org.elasticsearch.painless.tree.writer.Constants.TOSHORTWOOVERFLOW_DOUBLE;
import static org.elasticsearch.painless.tree.writer.Constants.TOSHORTWOOVERFLOW_FLOAT;

public class Shared {
    public static void writeCast(final GeneratorAdapter adapter, final Cast cast) {
        if (cast instanceof Transform) {
            final Transform transform = (Transform)cast;

            if (transform.upcast != null) {
                adapter.checkCast(transform.upcast.type);
            }

            if (java.lang.reflect.Modifier.isStatic(transform.method.reflect.getModifiers())) {
                adapter.invokeStatic(transform.method.owner.type, transform.method.method);
            } else if (java.lang.reflect.Modifier.isInterface(transform.method.owner.clazz.getModifiers())) {
                adapter.invokeInterface(transform.method.owner.type, transform.method.method);
            } else {
                adapter.invokeVirtual(transform.method.owner.type, transform.method.method);
            }

            if (transform.downcast != null) {
                adapter.checkCast(transform.downcast.type);
            }
        } else if (cast != null) {
            final Type from = cast.from;
            final Type to = cast.to;

            if (from.equals(to)) {
                return;
            }

            if (from.sort.numeric && from.sort.primitive && to.sort.numeric && to.sort.primitive) {
                adapter.cast(from.type, to.type);
            } else {
                try {
                    from.clazz.asSubclass(to.clazz);
                } catch (ClassCastException exception) {
                    adapter.checkCast(to.type);
                }
            }
        }
    }

    public static void writeNewStrings(final GeneratorAdapter adapter) {
        adapter.newInstance(STRINGBUILDER_TYPE);
        adapter.dup();
        adapter.invokeConstructor(STRINGBUILDER_TYPE, STRINGBUILDER_CONSTRUCTOR);
    }

    public static void writeAppendStrings(final GeneratorAdapter adapter, final Sort sort) {
        switch (sort) {
            case BOOL:   adapter.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_BOOLEAN); break;
            case CHAR:   adapter.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_CHAR);    break;
            case BYTE:
            case SHORT:
            case INT:    adapter.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_INT);     break;
            case LONG:   adapter.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_LONG);    break;
            case FLOAT:  adapter.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_FLOAT);   break;
            case DOUBLE: adapter.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_DOUBLE);  break;
            case STRING: adapter.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_STRING);  break;
            default:     adapter.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_OBJECT);
        }
    }

    public static void writeToStrings(final GeneratorAdapter adapter) {
        adapter.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_TOSTRING);
    }

    public static void writeDup(final GeneratorAdapter adapter, final int size, final boolean x1, final boolean x2) {
        if (size == 1) {
            if (x2) {
                adapter.dupX2();
            } else if (x1) {
                adapter.dupX1();
            } else {
                adapter.dup();
            }
        } else if (size == 2) {
            if (x2) {
                adapter.dup2X2();
            } else if (x1) {
                adapter.dup2X1();
            } else {
                adapter.dup2();
            }
        }
    }

    public static void writePop(final GeneratorAdapter adapter, final int size) {
        if (size == 1) {
            adapter.pop();
        } else if (size == 2) {
            adapter.pop2();
        }
    }

    /**
     * Called for any compound assignment (including increment/decrement instructions).
     * We have to be stricter than writeBinary and do overflow checks against the original type's size
     * instead of the promoted type's size, since the result will be implicitly cast back.
     *
     * @return This will be true if an instruction is written, false otherwise.
     */
    public static boolean writeExactInstruction(
        final Definition definition, final GeneratorAdapter adapter, final Sort fsort, final Sort tsort) {
        if (fsort == Sort.DOUBLE) {
            if (tsort == Sort.FLOAT) {
                adapter.invokeStatic(definition.utilityType.type, TOFLOATWOOVERFLOW_DOUBLE);
            } else if (tsort == Sort.FLOAT_OBJ) {
                adapter.invokeStatic(definition.utilityType.type, TOFLOATWOOVERFLOW_DOUBLE);
                adapter.checkCast(definition.floatobjType.type);
            } else if (tsort == Sort.LONG) {
                adapter.invokeStatic(definition.utilityType.type, TOLONGWOOVERFLOW_DOUBLE);
            } else if (tsort == Sort.LONG_OBJ) {
                adapter.invokeStatic(definition.utilityType.type, TOLONGWOOVERFLOW_DOUBLE);
                adapter.checkCast(definition.longobjType.type);
            } else if (tsort == Sort.INT) {
                adapter.invokeStatic(definition.utilityType.type, TOINTWOOVERFLOW_DOUBLE);
            } else if (tsort == Sort.INT_OBJ) {
                adapter.invokeStatic(definition.utilityType.type, TOINTWOOVERFLOW_DOUBLE);
                adapter.checkCast(definition.intobjType.type);
            } else if (tsort == Sort.CHAR) {
                adapter.invokeStatic(definition.utilityType.type, TOCHARWOOVERFLOW_DOUBLE);
            } else if (tsort == Sort.CHAR_OBJ) {
                adapter.invokeStatic(definition.utilityType.type, TOCHARWOOVERFLOW_DOUBLE);
                adapter.checkCast(definition.charobjType.type);
            } else if (tsort == Sort.SHORT) {
                adapter.invokeStatic(definition.utilityType.type, TOSHORTWOOVERFLOW_DOUBLE);
            } else if (tsort == Sort.SHORT_OBJ) {
                adapter.invokeStatic(definition.utilityType.type, TOSHORTWOOVERFLOW_DOUBLE);
                adapter.checkCast(definition.shortobjType.type);
            } else if (tsort == Sort.BYTE) {
                adapter.invokeStatic(definition.utilityType.type, TOBYTEWOOVERFLOW_DOUBLE);
            } else if (tsort == Sort.BYTE_OBJ) {
                adapter.invokeStatic(definition.utilityType.type, TOBYTEWOOVERFLOW_DOUBLE);
                adapter.checkCast(definition.byteobjType.type);
            } else {
                return false;
            }
        } else if (fsort == Sort.FLOAT) {
            if (tsort == Sort.LONG) {
                adapter.invokeStatic(definition.utilityType.type, TOLONGWOOVERFLOW_FLOAT);
            } else if (tsort == Sort.LONG_OBJ) {
                adapter.invokeStatic(definition.utilityType.type, TOLONGWOOVERFLOW_FLOAT);
                adapter.checkCast(definition.longobjType.type);
            } else if (tsort == Sort.INT) {
                adapter.invokeStatic(definition.utilityType.type, TOINTWOOVERFLOW_FLOAT);
            } else if (tsort == Sort.INT_OBJ) {
                adapter.invokeStatic(definition.utilityType.type, TOINTWOOVERFLOW_FLOAT);
                adapter.checkCast(definition.intobjType.type);
            } else if (tsort == Sort.CHAR) {
                adapter.invokeStatic(definition.utilityType.type, TOCHARWOOVERFLOW_FLOAT);
            } else if (tsort == Sort.CHAR_OBJ) {
                adapter.invokeStatic(definition.utilityType.type, TOCHARWOOVERFLOW_FLOAT);
                adapter.checkCast(definition.charobjType.type);
            } else if (tsort == Sort.SHORT) {
                adapter.invokeStatic(definition.utilityType.type, TOSHORTWOOVERFLOW_FLOAT);
            } else if (tsort == Sort.SHORT_OBJ) {
                adapter.invokeStatic(definition.utilityType.type, TOSHORTWOOVERFLOW_FLOAT);
                adapter.checkCast(definition.shortobjType.type);
            } else if (tsort == Sort.BYTE) {
                adapter.invokeStatic(definition.utilityType.type, TOBYTEWOOVERFLOW_FLOAT);
            } else if (tsort == Sort.BYTE_OBJ) {
                adapter.invokeStatic(definition.utilityType.type, TOBYTEWOOVERFLOW_FLOAT);
                adapter.checkCast(definition.byteobjType.type);
            } else {
                return false;
            }
        } else if (fsort == Sort.LONG) {
            if (tsort == Sort.INT) {
                adapter.invokeStatic(definition.mathType.type, TOINTEXACT_LONG);
            } else if (tsort == Sort.INT_OBJ) {
                adapter.invokeStatic(definition.mathType.type, TOINTEXACT_LONG);
                adapter.checkCast(definition.intobjType.type);
            } else if (tsort == Sort.CHAR) {
                adapter.invokeStatic(definition.utilityType.type, TOCHAREXACT_LONG);
            } else if (tsort == Sort.CHAR_OBJ) {
                adapter.invokeStatic(definition.utilityType.type, TOCHAREXACT_LONG);
                adapter.checkCast(definition.charobjType.type);
            } else if (tsort == Sort.SHORT) {
                adapter.invokeStatic(definition.utilityType.type, TOSHORTEXACT_LONG);
            } else if (tsort == Sort.SHORT_OBJ) {
                adapter.invokeStatic(definition.utilityType.type, TOSHORTEXACT_LONG);
                adapter.checkCast(definition.shortobjType.type);
            } else if (tsort == Sort.BYTE) {
                adapter.invokeStatic(definition.utilityType.type, TOBYTEEXACT_LONG);
            } else if (tsort == Sort.BYTE_OBJ) {
                adapter.invokeStatic(definition.utilityType.type, TOBYTEEXACT_LONG);
                adapter.checkCast(definition.byteobjType.type);
            } else {
                return false;
            }
        } else if (fsort == Sort.INT) {
            if (tsort == Sort.CHAR) {
                adapter.invokeStatic(definition.utilityType.type, TOCHAREXACT_INT);
            } else if (tsort == Sort.CHAR_OBJ) {
                adapter.invokeStatic(definition.utilityType.type, TOCHAREXACT_INT);
                adapter.checkCast(definition.charobjType.type);
            } else if (tsort == Sort.SHORT) {
                adapter.invokeStatic(definition.utilityType.type, TOSHORTEXACT_INT);
            } else if (tsort == Sort.SHORT_OBJ) {
                adapter.invokeStatic(definition.utilityType.type, TOSHORTEXACT_INT);
                adapter.checkCast(definition.shortobjType.type);
            } else if (tsort == Sort.BYTE) {
                adapter.invokeStatic(definition.utilityType.type, TOBYTEEXACT_INT);
            } else if (tsort == Sort.BYTE_OBJ) {
                adapter.invokeStatic(definition.utilityType.type, TOBYTEEXACT_INT);
                adapter.checkCast(definition.byteobjType.type);
            } else {
                return false;
            }
        } else {
            return false;
        }

        return true;
    }

    private Shared() {}
}
