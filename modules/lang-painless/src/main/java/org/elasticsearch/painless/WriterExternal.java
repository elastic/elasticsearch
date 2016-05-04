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

package org.elasticsearch.painless;

import org.antlr.v4.runtime.ParserRuleContext;
import org.elasticsearch.painless.Definition.Constructor;
import org.elasticsearch.painless.Definition.Field;
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Metadata.ExpressionMetadata;
import org.elasticsearch.painless.Metadata.ExtNodeMetadata;
import org.elasticsearch.painless.Metadata.ExternalMetadata;
import org.elasticsearch.painless.PainlessParser.ExpressionContext;
import org.elasticsearch.painless.PainlessParser.ExtbraceContext;
import org.elasticsearch.painless.PainlessParser.ExtcallContext;
import org.elasticsearch.painless.PainlessParser.ExtcastContext;
import org.elasticsearch.painless.PainlessParser.ExtdotContext;
import org.elasticsearch.painless.PainlessParser.ExtfieldContext;
import org.elasticsearch.painless.PainlessParser.ExtnewContext;
import org.elasticsearch.painless.PainlessParser.ExtprecContext;
import org.elasticsearch.painless.PainlessParser.ExtstartContext;
import org.elasticsearch.painless.PainlessParser.ExtstringContext;
import org.elasticsearch.painless.PainlessParser.ExtvarContext;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.List;

import static org.elasticsearch.painless.PainlessParser.ADD;
import static org.elasticsearch.painless.PainlessParser.DIV;
import static org.elasticsearch.painless.PainlessParser.MUL;
import static org.elasticsearch.painless.PainlessParser.REM;
import static org.elasticsearch.painless.PainlessParser.SUB;
import static org.elasticsearch.painless.WriterConstants.CLASS_TYPE;
import static org.elasticsearch.painless.WriterConstants.DEFINITION_TYPE;
import static org.elasticsearch.painless.WriterConstants.DEF_ARRAY_LOAD;
import static org.elasticsearch.painless.WriterConstants.DEF_ARRAY_STORE;
import static org.elasticsearch.painless.WriterConstants.DEF_FIELD_LOAD;
import static org.elasticsearch.painless.WriterConstants.DEF_FIELD_STORE;
import static org.elasticsearch.painless.WriterConstants.DEF_METHOD_CALL;
import static org.elasticsearch.painless.WriterConstants.TOBYTEEXACT_INT;
import static org.elasticsearch.painless.WriterConstants.TOBYTEEXACT_LONG;
import static org.elasticsearch.painless.WriterConstants.TOBYTEWOOVERFLOW_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.TOBYTEWOOVERFLOW_FLOAT;
import static org.elasticsearch.painless.WriterConstants.TOCHAREXACT_INT;
import static org.elasticsearch.painless.WriterConstants.TOCHAREXACT_LONG;
import static org.elasticsearch.painless.WriterConstants.TOCHARWOOVERFLOW_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.TOCHARWOOVERFLOW_FLOAT;
import static org.elasticsearch.painless.WriterConstants.TOFLOATWOOVERFLOW_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.TOINTEXACT_LONG;
import static org.elasticsearch.painless.WriterConstants.TOINTWOOVERFLOW_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.TOINTWOOVERFLOW_FLOAT;
import static org.elasticsearch.painless.WriterConstants.TOLONGWOOVERFLOW_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.TOLONGWOOVERFLOW_FLOAT;
import static org.elasticsearch.painless.WriterConstants.TOSHORTEXACT_INT;
import static org.elasticsearch.painless.WriterConstants.TOSHORTEXACT_LONG;
import static org.elasticsearch.painless.WriterConstants.TOSHORTWOOVERFLOW_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.TOSHORTWOOVERFLOW_FLOAT;

class WriterExternal {
    private final Metadata metadata;
    private final Definition definition;
    private final CompilerSettings settings;

    private final GeneratorAdapter execute;

    private final Writer writer;
    private final WriterUtility utility;
    private final WriterCaster caster;

    WriterExternal(final Metadata metadata, final GeneratorAdapter execute, final Writer writer,
                   final WriterUtility utility, final WriterCaster caster) {
        this.metadata = metadata;
        definition = metadata.definition;
        settings = metadata.settings;

        this.execute = execute;

        this.writer = writer;
        this.utility = utility;
        this.caster = caster;
    }

    void processExtstart(final ExtstartContext ctx) {
        final ExternalMetadata startemd = metadata.getExternalMetadata(ctx);

        if (startemd.token == ADD) {
            final ExpressionMetadata storeemd = metadata.getExpressionMetadata(startemd.storeExpr);

            if (startemd.current.sort == Sort.STRING || storeemd.from.sort == Sort.STRING) {
                utility.writeNewStrings();
                utility.addStrings(startemd.storeExpr);
            }
        }

        final ExtprecContext precctx = ctx.extprec();
        final ExtcastContext castctx = ctx.extcast();
        final ExtvarContext varctx = ctx.extvar();
        final ExtnewContext newctx = ctx.extnew();
        final ExtstringContext stringctx = ctx.extstring();

        if (precctx != null) {
            writer.visit(precctx);
        } else if (castctx != null) {
            writer.visit(castctx);
        } else if (varctx != null) {
            writer.visit(varctx);
        } else if (newctx != null) {
            writer.visit(newctx);
        } else if (stringctx != null) {
            writer.visit(stringctx);
        } else {
            throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
        }
    }

    void processExtprec(final ExtprecContext ctx) {
        final ExtprecContext precctx = ctx.extprec();
        final ExtcastContext castctx = ctx.extcast();
        final ExtvarContext varctx = ctx.extvar();
        final ExtnewContext newctx = ctx.extnew();
        final ExtstringContext stringctx = ctx.extstring();

        if (precctx != null) {
            writer.visit(precctx);
        } else if (castctx != null) {
            writer.visit(castctx);
        } else if (varctx != null) {
            writer.visit(varctx);
        } else if (newctx != null) {
            writer.visit(newctx);
        } else if (stringctx != null) {
            writer.visit(stringctx);
        } else {
            throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
        }

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        if (dotctx != null) {
            writer.visit(dotctx);
        } else if (bracectx != null) {
            writer.visit(bracectx);
        }
    }

    void processExtcast(final ExtcastContext ctx) {
        ExtNodeMetadata castenmd = metadata.getExtNodeMetadata(ctx);

        final ExtprecContext precctx = ctx.extprec();
        final ExtcastContext castctx = ctx.extcast();
        final ExtvarContext varctx = ctx.extvar();
        final ExtnewContext newctx = ctx.extnew();
        final ExtstringContext stringctx = ctx.extstring();

        if (precctx != null) {
            writer.visit(precctx);
        } else if (castctx != null) {
            writer.visit(castctx);
        } else if (varctx != null) {
            writer.visit(varctx);
        } else if (newctx != null) {
            writer.visit(newctx);
        } else if (stringctx != null) {
            writer.visit(stringctx);
        } else {
            throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
        }

        caster.checkWriteCast(ctx, castenmd.castTo);
    }

    void processExtbrace(final ExtbraceContext ctx) {
        final ExpressionContext exprctx = ctx.expression();

        writer.visit(exprctx);
        writeLoadStoreExternal(ctx);

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        if (dotctx != null) {
            writer.visit(dotctx);
        } else if (bracectx != null) {
            writer.visit(bracectx);
        }
    }

    void processExtdot(final ExtdotContext ctx) {
        final ExtcallContext callctx = ctx.extcall();
        final ExtfieldContext fieldctx = ctx.extfield();

        if (callctx != null) {
            writer.visit(callctx);
        } else if (fieldctx != null) {
            writer.visit(fieldctx);
        }
    }

    void processExtcall(final ExtcallContext ctx) {
        writeCallExternal(ctx);

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        if (dotctx != null) {
            writer.visit(dotctx);
        } else if (bracectx != null) {
            writer.visit(bracectx);
        }
    }

    void processExtvar(final ExtvarContext ctx) {
        writeLoadStoreExternal(ctx);

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        if (dotctx != null) {
            writer.visit(dotctx);
        } else if (bracectx != null) {
            writer.visit(bracectx);
        }
    }

    void processExtfield(final ExtfieldContext ctx) {
        writeLoadStoreExternal(ctx);

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        if (dotctx != null) {
            writer.visit(dotctx);
        } else if (bracectx != null) {
            writer.visit(bracectx);
        }
    }

    void processExtnew(final ExtnewContext ctx) {
        writeNewExternal(ctx);

        final ExtdotContext dotctx = ctx.extdot();

        if (dotctx != null) {
            writer.visit(dotctx);
        }
    }

    void processExtstring(final ExtstringContext ctx) {
        final ExtNodeMetadata stringenmd = metadata.getExtNodeMetadata(ctx);

        utility.writeConstant(ctx, stringenmd.target);

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        if (dotctx != null) {
            writer.visit(dotctx);
        } else if (bracectx != null) {
            writer.visit(bracectx);
        }
    }

    private void writeLoadStoreExternal(final ParserRuleContext source) {
        final ExtNodeMetadata sourceenmd = metadata.getExtNodeMetadata(source);
        final ExternalMetadata parentemd = metadata.getExternalMetadata(sourceenmd.parent);

        if (sourceenmd.target == null) {
            return;
        }

        final boolean length = "#length".equals(sourceenmd.target);
        final boolean array = "#brace".equals(sourceenmd.target);
        final boolean name = sourceenmd.target instanceof String && !length && !array;
        final boolean variable = sourceenmd.target instanceof Integer;
        final boolean field = sourceenmd.target instanceof Field;
        final boolean shortcut = sourceenmd.target instanceof Object[];

        if (!length && !variable && !field && !array && !name && !shortcut) {
            throw new IllegalStateException(WriterUtility.error(source) + "Target not found for load/store.");
        }

        final boolean maplist = shortcut && (boolean)((Object[])sourceenmd.target)[2];
        final Object constant = shortcut ? ((Object[])sourceenmd.target)[3] : null;

        final boolean x1 = field || name || (shortcut && !maplist);
        final boolean x2 = array || (shortcut && maplist);

        if (length) {
            execute.arrayLength();
        } else if (sourceenmd.last && parentemd.storeExpr != null) {
            final ExpressionMetadata expremd = metadata.getExpressionMetadata(parentemd.storeExpr);
            final boolean cat = utility.containsStrings(parentemd.storeExpr);

            if (cat) {
                if (field || name || shortcut) {
                    execute.dupX1();
                } else if (array) {
                    execute.dup2X1();
                }

                if (maplist) {
                    if (constant != null) {
                        utility.writeConstant(source, constant);
                    }

                    execute.dupX2();
                }

                writeLoadStoreInstruction(source, false, variable, field, name, array, shortcut);
                utility.writeAppendStrings(sourceenmd.type.sort);
                writer.visit(parentemd.storeExpr);

                if (utility.containsStrings(parentemd.storeExpr)) {
                    utility.writeAppendStrings(expremd.to.sort);
                    utility.removeStrings(parentemd.storeExpr);
                }

                utility.writeToStrings();
                caster.checkWriteCast(source, sourceenmd.castTo);

                if (parentemd.read) {
                    utility.writeDup(sourceenmd.type.sort.size, x1, x2);
                }

                writeLoadStoreInstruction(source, true, variable, field, name, array, shortcut);
            } else if (parentemd.token > 0) {
                final int token = parentemd.token;

                if (field || name || shortcut) {
                    execute.dup();
                } else if (array) {
                    execute.dup2();
                }

                if (maplist) {
                    if (constant != null) {
                        utility.writeConstant(source, constant);
                    }

                    execute.dupX1();
                }

                writeLoadStoreInstruction(source, false, variable, field, name, array, shortcut);

                if (parentemd.read && parentemd.post) {
                    utility.writeDup(sourceenmd.type.sort.size, x1, x2);
                }

                caster.checkWriteCast(source, sourceenmd.castFrom);
                writer.visit(parentemd.storeExpr);

                utility.writeBinaryInstruction(source, sourceenmd.promote, token);

                boolean exact = false;

                if (!settings.getNumericOverflow() && expremd.typesafe && sourceenmd.type.sort != Sort.DEF &&
                    (token == MUL || token == DIV || token == REM || token == ADD || token == SUB)) {
                    exact = writeExactInstruction(sourceenmd.type.sort, sourceenmd.promote.sort);
                }

                if (!exact) {
                    caster.checkWriteCast(source, sourceenmd.castTo);
                }

                if (parentemd.read && !parentemd.post) {
                    utility.writeDup(sourceenmd.type.sort.size, x1, x2);
                }

                writeLoadStoreInstruction(source, true, variable, field, name, array, shortcut);
            } else {
                if (constant != null) {
                    utility.writeConstant(source, constant);
                }

                writer.visit(parentemd.storeExpr);

                if (parentemd.read) {
                    utility.writeDup(sourceenmd.type.sort.size, x1, x2);
                }

                writeLoadStoreInstruction(source, true, variable, field, name, array, shortcut);
            }
        } else {
            if (constant != null) {
                utility.writeConstant(source, constant);
            }

            writeLoadStoreInstruction(source, false, variable, field, name, array, shortcut);
        }
    }

    private void writeLoadStoreInstruction(final ParserRuleContext source,
                                           final boolean store, final boolean variable,
                                           final boolean field, final boolean name,
                                           final boolean array, final boolean shortcut) {
        final ExtNodeMetadata sourceemd = metadata.getExtNodeMetadata(source);

        if (variable) {
            writeLoadStoreVariable(source, store, sourceemd.type, (int)sourceemd.target);
        } else if (field) {
            writeLoadStoreField(store, (Field)sourceemd.target);
        } else if (name) {
            writeLoadStoreField(source, store, (String)sourceemd.target);
        } else if (array) {
            writeLoadStoreArray(source, store, sourceemd.type);
        } else if (shortcut) {
            Object[] targets = (Object[])sourceemd.target;
            writeLoadStoreShortcut(store, (Method)targets[0], (Method)targets[1]);
        } else {
            throw new IllegalStateException(WriterUtility.error(source) + "Load/Store requires a variable, field, or array.");
        }
    }

    private void writeLoadStoreVariable(final ParserRuleContext source, final boolean store, final Type type, int slot) {
        if (type.sort == Sort.VOID) {
            throw new IllegalStateException(WriterUtility.error(source) + "Cannot load/store void type.");
        }

        if (!metadata.scoreValueUsed && slot > metadata.scoreValueSlot) {
            --slot;
        }

        if (store) {
            execute.visitVarInsn(type.type.getOpcode(Opcodes.ISTORE), slot);
        } else {
            execute.visitVarInsn(type.type.getOpcode(Opcodes.ILOAD), slot);
        }
    }

    private void writeLoadStoreField(final boolean store, final Field field) {
        if (java.lang.reflect.Modifier.isStatic(field.reflect.getModifiers())) {
            if (store) {
                execute.putStatic(field.owner.type, field.reflect.getName(), field.type.type);
            } else {
                execute.getStatic(field.owner.type, field.reflect.getName(), field.type.type);

                if (!field.generic.clazz.equals(field.type.clazz)) {
                    execute.checkCast(field.generic.type);
                }
            }
        } else {
            if (store) {
                execute.putField(field.owner.type, field.reflect.getName(), field.type.type);
            } else {
                execute.getField(field.owner.type, field.reflect.getName(), field.type.type);

                if (!field.generic.clazz.equals(field.type.clazz)) {
                    execute.checkCast(field.generic.type);
                }
            }
        }
    }

    private void writeLoadStoreField(final ParserRuleContext source, final boolean store, final String name) {
        if (store) {
            final ExtNodeMetadata sourceemd = metadata.getExtNodeMetadata(source);
            final ExternalMetadata parentemd = metadata.getExternalMetadata(sourceemd.parent);
            final ExpressionMetadata expremd = metadata.getExpressionMetadata(parentemd.storeExpr);

            execute.push(name);
            execute.loadThis();
            execute.getField(CLASS_TYPE, "definition", DEFINITION_TYPE);
            execute.push(parentemd.token == 0 && expremd.typesafe);
            execute.invokeStatic(definition.defobjType.type, DEF_FIELD_STORE);
        } else {
            execute.push(name);
            execute.loadThis();
            execute.getField(CLASS_TYPE, "definition", DEFINITION_TYPE);
            execute.invokeStatic(definition.defobjType.type, DEF_FIELD_LOAD);
        }
    }

    private void writeLoadStoreArray(final ParserRuleContext source, final boolean store, final Type type) {
        if (type.sort == Sort.VOID) {
            throw new IllegalStateException(WriterUtility.error(source) + "Cannot load/store void type.");
        }

        if (type.sort == Sort.DEF) {
            final ExtbraceContext bracectx = (ExtbraceContext)source;
            final ExpressionMetadata expremd0 = metadata.getExpressionMetadata(bracectx.expression());

            if (store) {
                final ExtNodeMetadata braceenmd = metadata.getExtNodeMetadata(bracectx);
                final ExternalMetadata parentemd = metadata.getExternalMetadata(braceenmd.parent);
                final ExpressionMetadata expremd1 = metadata.getExpressionMetadata(parentemd.storeExpr);

                execute.loadThis();
                execute.getField(CLASS_TYPE, "definition", DEFINITION_TYPE);
                execute.push(expremd0.typesafe);
                execute.push(parentemd.token == 0 && expremd1.typesafe);
                execute.invokeStatic(definition.defobjType.type, DEF_ARRAY_STORE);
            } else {
                execute.loadThis();
                execute.getField(CLASS_TYPE, "definition", DEFINITION_TYPE);
                execute.push(expremd0.typesafe);
                execute.invokeStatic(definition.defobjType.type, DEF_ARRAY_LOAD);
            }
        } else {
            if (store) {
                execute.arrayStore(type.type);
            } else {
                execute.arrayLoad(type.type);
            }
        }
    }

    private void writeLoadStoreShortcut(final boolean store, final Method getter, final Method setter) {
        final Method method = store ? setter : getter;

        if (java.lang.reflect.Modifier.isInterface(getter.owner.clazz.getModifiers())) {
            execute.invokeInterface(method.owner.type, method.method);
        } else {
            execute.invokeVirtual(method.owner.type, method.method);
        }

        if (store) {
            utility.writePop(method.rtn.type.getSize());
        } else if (!method.rtn.clazz.equals(method.handle.type().returnType())) {
            execute.checkCast(method.rtn.type);
        }
    }

    /**
     * Called for any compound assignment (including increment/decrement instructions).
     * We have to be stricter than writeBinary and do overflow checks against the original type's size
     * instead of the promoted type's size, since the result will be implicitly cast back.
     *
     * @return This will be true if an instruction is written, false otherwise.
     */
    private boolean writeExactInstruction(final Sort osort, final Sort psort) {
        if (psort == Sort.DOUBLE) {
            if (osort == Sort.FLOAT) {
                execute.invokeStatic(definition.utilityType.type, TOFLOATWOOVERFLOW_DOUBLE);
            } else if (osort == Sort.FLOAT_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOFLOATWOOVERFLOW_DOUBLE);
                execute.checkCast(definition.floatobjType.type);
            } else if (osort == Sort.LONG) {
                execute.invokeStatic(definition.utilityType.type, TOLONGWOOVERFLOW_DOUBLE);
            } else if (osort == Sort.LONG_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOLONGWOOVERFLOW_DOUBLE);
                execute.checkCast(definition.longobjType.type);
            } else if (osort == Sort.INT) {
                execute.invokeStatic(definition.utilityType.type, TOINTWOOVERFLOW_DOUBLE);
            } else if (osort == Sort.INT_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOINTWOOVERFLOW_DOUBLE);
                execute.checkCast(definition.intobjType.type);
            } else if (osort == Sort.CHAR) {
                execute.invokeStatic(definition.utilityType.type, TOCHARWOOVERFLOW_DOUBLE);
            } else if (osort == Sort.CHAR_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOCHARWOOVERFLOW_DOUBLE);
                execute.checkCast(definition.charobjType.type);
            } else if (osort == Sort.SHORT) {
                execute.invokeStatic(definition.utilityType.type, TOSHORTWOOVERFLOW_DOUBLE);
            } else if (osort == Sort.SHORT_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOSHORTWOOVERFLOW_DOUBLE);
                execute.checkCast(definition.shortobjType.type);
            } else if (osort == Sort.BYTE) {
                execute.invokeStatic(definition.utilityType.type, TOBYTEWOOVERFLOW_DOUBLE);
            } else if (osort == Sort.BYTE_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOBYTEWOOVERFLOW_DOUBLE);
                execute.checkCast(definition.byteobjType.type);
            } else {
                return false;
            }
        } else if (psort == Sort.FLOAT) {
            if (osort == Sort.LONG) {
                execute.invokeStatic(definition.utilityType.type, TOLONGWOOVERFLOW_FLOAT);
            } else if (osort == Sort.LONG_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOLONGWOOVERFLOW_FLOAT);
                execute.checkCast(definition.longobjType.type);
            } else if (osort == Sort.INT) {
                execute.invokeStatic(definition.utilityType.type, TOINTWOOVERFLOW_FLOAT);
            } else if (osort == Sort.INT_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOINTWOOVERFLOW_FLOAT);
                execute.checkCast(definition.intobjType.type);
            } else if (osort == Sort.CHAR) {
                execute.invokeStatic(definition.utilityType.type, TOCHARWOOVERFLOW_FLOAT);
            } else if (osort == Sort.CHAR_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOCHARWOOVERFLOW_FLOAT);
                execute.checkCast(definition.charobjType.type);
            } else if (osort == Sort.SHORT) {
                execute.invokeStatic(definition.utilityType.type, TOSHORTWOOVERFLOW_FLOAT);
            } else if (osort == Sort.SHORT_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOSHORTWOOVERFLOW_FLOAT);
                execute.checkCast(definition.shortobjType.type);
            } else if (osort == Sort.BYTE) {
                execute.invokeStatic(definition.utilityType.type, TOBYTEWOOVERFLOW_FLOAT);
            } else if (osort == Sort.BYTE_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOBYTEWOOVERFLOW_FLOAT);
                execute.checkCast(definition.byteobjType.type);
            } else {
                return false;
            }
        } else if (psort == Sort.LONG) {
            if (osort == Sort.INT) {
                execute.invokeStatic(definition.mathType.type, TOINTEXACT_LONG);
            } else if (osort == Sort.INT_OBJ) {
                execute.invokeStatic(definition.mathType.type, TOINTEXACT_LONG);
                execute.checkCast(definition.intobjType.type);
            } else if (osort == Sort.CHAR) {
                execute.invokeStatic(definition.utilityType.type, TOCHAREXACT_LONG);
            } else if (osort == Sort.CHAR_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOCHAREXACT_LONG);
                execute.checkCast(definition.charobjType.type);
            } else if (osort == Sort.SHORT) {
                execute.invokeStatic(definition.utilityType.type, TOSHORTEXACT_LONG);
            } else if (osort == Sort.SHORT_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOSHORTEXACT_LONG);
                execute.checkCast(definition.shortobjType.type);
            } else if (osort == Sort.BYTE) {
                execute.invokeStatic(definition.utilityType.type, TOBYTEEXACT_LONG);
            } else if (osort == Sort.BYTE_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOBYTEEXACT_LONG);
                execute.checkCast(definition.byteobjType.type);
            } else {
                return false;
            }
        } else if (psort == Sort.INT) {
            if (osort == Sort.CHAR) {
                execute.invokeStatic(definition.utilityType.type, TOCHAREXACT_INT);
            } else if (osort == Sort.CHAR_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOCHAREXACT_INT);
                execute.checkCast(definition.charobjType.type);
            } else if (osort == Sort.SHORT) {
                execute.invokeStatic(definition.utilityType.type, TOSHORTEXACT_INT);
            } else if (osort == Sort.SHORT_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOSHORTEXACT_INT);
                execute.checkCast(definition.shortobjType.type);
            } else if (osort == Sort.BYTE) {
                execute.invokeStatic(definition.utilityType.type, TOBYTEEXACT_INT);
            } else if (osort == Sort.BYTE_OBJ) {
                execute.invokeStatic(definition.utilityType.type, TOBYTEEXACT_INT);
                execute.checkCast(definition.byteobjType.type);
            } else {
                return false;
            }
        } else {
            return false;
        }

        return true;
    }

    private void writeNewExternal(final ExtnewContext source) {
        final ExtNodeMetadata sourceenmd = metadata.getExtNodeMetadata(source);
        final ExternalMetadata parentemd = metadata.getExternalMetadata(sourceenmd.parent);

        final boolean makearray = "#makearray".equals(sourceenmd.target);
        final boolean constructor = sourceenmd.target instanceof Constructor;

        if (!makearray && !constructor) {
            throw new IllegalStateException(WriterUtility.error(source) + "Target not found for new call.");
        }

        if (makearray) {
            for (final ExpressionContext exprctx : source.expression()) {
                writer.visit(exprctx);
            }

            if (sourceenmd.type.sort == Sort.ARRAY) {
                execute.visitMultiANewArrayInsn(sourceenmd.type.type.getDescriptor(), sourceenmd.type.type.getDimensions());
            } else {
                execute.newArray(sourceenmd.type.type);
            }
        } else {
            execute.newInstance(sourceenmd.type.type);

            if (parentemd.read) {
                execute.dup();
            }

            for (final ExpressionContext exprctx : source.arguments().expression()) {
                writer.visit(exprctx);
            }

            final Constructor target = (Constructor)sourceenmd.target;
            execute.invokeConstructor(target.owner.type, target.method);
        }
    }

    private void writeCallExternal(final ExtcallContext source) {
        final ExtNodeMetadata sourceenmd = metadata.getExtNodeMetadata(source);

        final boolean method = sourceenmd.target instanceof Method;
        final boolean def = sourceenmd.target instanceof String;

        if (!method && !def) {
            throw new IllegalStateException(WriterUtility.error(source) + "Target not found for call.");
        }

        final List<ExpressionContext> arguments = source.arguments().expression();

        if (method) {
            for (final ExpressionContext exprctx : arguments) {
                writer.visit(exprctx);
            }

            final Method target = (Method)sourceenmd.target;

            if (java.lang.reflect.Modifier.isStatic(target.reflect.getModifiers())) {
                execute.invokeStatic(target.owner.type, target.method);
            } else if (java.lang.reflect.Modifier.isInterface(target.owner.clazz.getModifiers())) {
                execute.invokeInterface(target.owner.type, target.method);
            } else {
                execute.invokeVirtual(target.owner.type, target.method);
            }

            if (!target.rtn.clazz.equals(target.handle.type().returnType())) {
                execute.checkCast(target.rtn.type);
            }
        } else {
            execute.push((String)sourceenmd.target);
            execute.loadThis();
            execute.getField(CLASS_TYPE, "definition", DEFINITION_TYPE);

            execute.push(arguments.size());
            execute.newArray(definition.defType.type);

            for (int argument = 0; argument < arguments.size(); ++argument) {
                execute.dup();
                execute.push(argument);
                writer.visit(arguments.get(argument));
                execute.arrayStore(definition.defType.type);
            }

            execute.push(arguments.size());
            execute.newArray(definition.booleanType.type);

            for (int argument = 0; argument < arguments.size(); ++argument) {
                execute.dup();
                execute.push(argument);
                execute.push(metadata.getExpressionMetadata(arguments.get(argument)).typesafe);
                execute.arrayStore(definition.booleanType.type);
            }

            execute.invokeStatic(definition.defobjType.type, DEF_METHOD_CALL);
        }
    }
}
