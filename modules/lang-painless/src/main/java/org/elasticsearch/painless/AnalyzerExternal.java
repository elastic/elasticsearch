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
import org.elasticsearch.painless.AnalyzerUtility.Variable;
import org.elasticsearch.painless.Definition.Constructor;
import org.elasticsearch.painless.Definition.Field;
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Struct;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Metadata.ExpressionMetadata;
import org.elasticsearch.painless.Metadata.ExtNodeMetadata;
import org.elasticsearch.painless.Metadata.ExternalMetadata;
import org.elasticsearch.painless.PainlessParser.DecltypeContext;
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
import org.elasticsearch.painless.PainlessParser.IdentifierContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.painless.PainlessParser.ADD;
import static org.elasticsearch.painless.PainlessParser.BWAND;
import static org.elasticsearch.painless.PainlessParser.BWOR;
import static org.elasticsearch.painless.PainlessParser.BWXOR;
import static org.elasticsearch.painless.PainlessParser.DIV;
import static org.elasticsearch.painless.PainlessParser.MUL;
import static org.elasticsearch.painless.PainlessParser.REM;
import static org.elasticsearch.painless.PainlessParser.SUB;

class AnalyzerExternal {
    private final Metadata metadata;
    private final Definition definition;

    private final Analyzer analyzer;
    private final AnalyzerUtility utility;
    private final AnalyzerCaster caster;
    private final AnalyzerPromoter promoter;

    AnalyzerExternal(final Metadata metadata, final Analyzer analyzer, final AnalyzerUtility utility,
                     final AnalyzerCaster caster, final AnalyzerPromoter promoter) {
        this.metadata = metadata;
        this.definition = metadata.definition;

        this.analyzer = analyzer;
        this.utility = utility;
        this.caster = caster;
        this.promoter = promoter;
    }

    void processExtstart(final ExtstartContext ctx) {
        final ExtprecContext precctx = ctx.extprec();
        final ExtcastContext castctx = ctx.extcast();
        final ExtvarContext varctx = ctx.extvar();
        final ExtnewContext newctx = ctx.extnew();
        final ExtstringContext stringctx = ctx.extstring();

        if (precctx != null) {
            metadata.createExtNodeMetadata(ctx, precctx);
            analyzer.visit(precctx);
        } else if (castctx != null) {
            metadata.createExtNodeMetadata(ctx, castctx);
            analyzer.visit(castctx);
        } else if (varctx != null) {
            metadata.createExtNodeMetadata(ctx, varctx);
            analyzer.visit(varctx);
        } else if (newctx != null) {
            metadata.createExtNodeMetadata(ctx, newctx);
            analyzer.visit(newctx);
        } else if (stringctx != null) {
            metadata.createExtNodeMetadata(ctx, stringctx);
            analyzer.visit(stringctx);
        } else {
            throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
        }
    }

    void processExtprec(final ExtprecContext ctx) {
        final ExtNodeMetadata precenmd = metadata.getExtNodeMetadata(ctx);
        final ParserRuleContext parent = precenmd.parent;
        final ExternalMetadata parentemd = metadata.getExternalMetadata(parent);

        final ExtprecContext precctx = ctx.extprec();
        final ExtcastContext castctx = ctx.extcast();
        final ExtvarContext varctx = ctx.extvar();
        final ExtnewContext newctx = ctx.extnew();
        final ExtstringContext stringctx = ctx.extstring();

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        if (dotctx != null || bracectx != null) {
            ++parentemd.scope;
        }

        if (precctx != null) {
            metadata.createExtNodeMetadata(parent, precctx);
            analyzer.visit(precctx);
        } else if (castctx != null) {
            metadata.createExtNodeMetadata(parent, castctx);
            analyzer.visit(castctx);
        } else if (varctx != null) {
            metadata.createExtNodeMetadata(parent, varctx);
            analyzer.visit(varctx);
        } else if (newctx != null) {
            metadata.createExtNodeMetadata(parent, newctx);
            analyzer.visit(newctx);
        } else if (stringctx != null) {
            metadata.createExtNodeMetadata(ctx, stringctx);
            analyzer.visit(stringctx);
        } else {
            throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
        }

        parentemd.statement = false;

        if (dotctx != null) {
            --parentemd.scope;

            metadata.createExtNodeMetadata(parent, dotctx);
            analyzer.visit(dotctx);
        } else if (bracectx != null) {
            --parentemd.scope;

            metadata.createExtNodeMetadata(parent, bracectx);
            analyzer.visit(bracectx);
        }
    }

    void processExtcast(final ExtcastContext ctx) {
        final ExtNodeMetadata castenmd = metadata.getExtNodeMetadata(ctx);
        final ParserRuleContext parent = castenmd.parent;
        final ExternalMetadata parentemd = metadata.getExternalMetadata(parent);

        final ExtprecContext precctx = ctx.extprec();
        final ExtcastContext castctx = ctx.extcast();
        final ExtvarContext varctx = ctx.extvar();
        final ExtnewContext newctx = ctx.extnew();
        final ExtstringContext stringctx = ctx.extstring();

        if (precctx != null) {
            metadata.createExtNodeMetadata(parent, precctx);
            analyzer.visit(precctx);
        } else if (castctx != null) {
            metadata.createExtNodeMetadata(parent, castctx);
            analyzer.visit(castctx);
        } else if (varctx != null) {
            metadata.createExtNodeMetadata(parent, varctx);
            analyzer.visit(varctx);
        } else if (newctx != null) {
            metadata.createExtNodeMetadata(parent, newctx);
            analyzer.visit(newctx);
        } else if (stringctx != null) {
            metadata.createExtNodeMetadata(ctx, stringctx);
            analyzer.visit(stringctx);
        } else {
            throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
        }

        final DecltypeContext declctx = ctx.decltype();
        final ExpressionMetadata declemd = metadata.createExpressionMetadata(declctx);
        analyzer.visit(declctx);

        castenmd.castTo = caster.getLegalCast(ctx, parentemd.current, declemd.from, true);
        castenmd.type = declemd.from;
        parentemd.current = declemd.from;
        parentemd.statement = false;
    }

    void processExtbrace(final ExtbraceContext ctx) {
        final ExtNodeMetadata braceenmd = metadata.getExtNodeMetadata(ctx);
        final ParserRuleContext parent = braceenmd.parent;
        final ExternalMetadata parentemd = metadata.getExternalMetadata(parent);

        final boolean array = parentemd.current.sort == Sort.ARRAY;
        final boolean def = parentemd.current.sort == Sort.DEF;
        boolean map = false;
        boolean list = false;

        try {
            parentemd.current.clazz.asSubclass(Map.class);
            map = true;
        } catch (final ClassCastException exception) {
            // Do nothing.
        }

        try {
            parentemd.current.clazz.asSubclass(List.class);
            list = true;
        } catch (final ClassCastException exception) {
            // Do nothing.
        }

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        braceenmd.last = parentemd.scope == 0 && dotctx == null && bracectx == null;

        final ExpressionContext exprctx = AnalyzerUtility.updateExpressionTree(ctx.expression());
        final ExpressionMetadata expremd = metadata.createExpressionMetadata(exprctx);

        if (array || def) {
            expremd.to = array ? definition.intType : definition.objectType;
            analyzer.visit(exprctx);
            caster.markCast(expremd);

            braceenmd.target = "#brace";
            braceenmd.type = def ? definition.defType :
                definition.getType(parentemd.current.struct, parentemd.current.type.getDimensions() - 1);
            analyzeLoadStoreExternal(ctx);
            parentemd.current = braceenmd.type;

            if (dotctx != null) {
                metadata.createExtNodeMetadata(parent, dotctx);
                analyzer.visit(dotctx);
            } else if (bracectx != null) {
                metadata.createExtNodeMetadata(parent, bracectx);
                analyzer.visit(bracectx);
            }
        } else {
            final boolean store = braceenmd.last && parentemd.storeExpr != null;
            final boolean get = parentemd.read || parentemd.token > 0 || !braceenmd.last;
            final boolean set = braceenmd.last && store;

            Method getter;
            Method setter;
            Type valuetype;
            Type settype;

            if (map) {
                getter = parentemd.current.struct.methods.get("get");
                setter = parentemd.current.struct.methods.get("put");

                if (getter != null && (getter.rtn.sort == Sort.VOID || getter.arguments.size() != 1)) {
                    throw new IllegalArgumentException(AnalyzerUtility.error(ctx) +
                        "Illegal map get shortcut for type [" + parentemd.current.name + "].");
                }

                if (setter != null && setter.arguments.size() != 2) {
                    throw new IllegalArgumentException(AnalyzerUtility.error(ctx) +
                        "Illegal map set shortcut for type [" + parentemd.current.name + "].");
                }

                if (getter != null && setter != null && (!getter.arguments.get(0).equals(setter.arguments.get(0))
                    || !getter.rtn.equals(setter.arguments.get(1)))) {
                    throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Shortcut argument types must match.");
                }

                valuetype = setter != null ? setter.arguments.get(0) : getter != null ? getter.arguments.get(0) : null;
                settype = setter == null ? null : setter.arguments.get(1);
            } else if (list) {
                getter = parentemd.current.struct.methods.get("get");
                setter = parentemd.current.struct.methods.get("set");

                if (getter != null && (getter.rtn.sort == Sort.VOID || getter.arguments.size() != 1 ||
                    getter.arguments.get(0).sort != Sort.INT)) {
                    throw new IllegalArgumentException(AnalyzerUtility.error(ctx) +
                        "Illegal list get shortcut for type [" + parentemd.current.name + "].");
                }

                if (setter != null && (setter.arguments.size() != 2 || setter.arguments.get(0).sort != Sort.INT)) {
                    throw new IllegalArgumentException(AnalyzerUtility.error(ctx) +
                        "Illegal list set shortcut for type [" + parentemd.current.name + "].");
                }

                if (getter != null && setter != null && (!getter.arguments.get(0).equals(setter.arguments.get(0))
                    || !getter.rtn.equals(setter.arguments.get(1)))) {
                    throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Shortcut argument types must match.");
                }

                valuetype = definition.intType;
                settype = setter == null ? null : setter.arguments.get(1);
            } else {
                throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
            }

            if ((get || set) && (!get || getter != null) && (!set || setter != null)) {
                expremd.to = valuetype;
                analyzer.visit(exprctx);
                caster.markCast(expremd);

                braceenmd.target = new Object[] {getter, setter, true, null};
                braceenmd.type = get ? getter.rtn : settype;
                analyzeLoadStoreExternal(ctx);
                parentemd.current = get ? getter.rtn : setter.rtn;
            }
        }

        if (braceenmd.target == null) {
            throw new IllegalArgumentException(AnalyzerUtility.error(ctx) +
                "Attempting to address a non-array type [" + parentemd.current.name + "] as an array.");
        }
    }

    void processExtdot(final ExtdotContext ctx) {
        final ExtNodeMetadata dotemnd = metadata.getExtNodeMetadata(ctx);
        final ParserRuleContext parent = dotemnd.parent;

        final ExtcallContext callctx = ctx.extcall();
        final ExtfieldContext fieldctx = ctx.extfield();

        if (callctx != null) {
            metadata.createExtNodeMetadata(parent, callctx);
            analyzer.visit(callctx);
        } else if (fieldctx != null) {
            metadata.createExtNodeMetadata(parent, fieldctx);
            analyzer.visit(fieldctx);
        }
    }

    void processExtcall(final ExtcallContext ctx) {
        final ExtNodeMetadata callenmd = metadata.getExtNodeMetadata(ctx);
        final ParserRuleContext parent = callenmd.parent;
        final ExternalMetadata parentemd = metadata.getExternalMetadata(parent);

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        callenmd.last = parentemd.scope == 0 && dotctx == null && bracectx == null;

        final String name = ctx.EXTID().getText();

        if (parentemd.current.sort == Sort.ARRAY) {
            throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Unexpected call [" + name + "] on an array.");
        } else if (callenmd.last && parentemd.storeExpr != null) {
            throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Cannot assign a value to a call [" + name + "].");
        }

        final Struct struct = parentemd.current.struct;
        final List<ExpressionContext> arguments = ctx.arguments().expression();
        final int size = arguments.size();
        Type[] types;

        final Method method = parentemd.statik ? struct.functions.get(name) : struct.methods.get(name);
        final boolean def = parentemd.current.sort == Sort.DEF;

        if (method == null && !def) {
            throw new IllegalArgumentException(
                AnalyzerUtility.error(ctx) + "Unknown call [" + name + "] on type [" + struct.name + "].");
        } else if (method != null) {
            types = new Type[method.arguments.size()];
            method.arguments.toArray(types);

            callenmd.target = method;
            callenmd.type = method.rtn;
            parentemd.statement = !parentemd.read && callenmd.last;
            parentemd.current = method.rtn;

            if (size != types.length) {
                throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "When calling [" + name + "] on type " +
                    "[" + struct.name + "] expected [" + types.length + "] arguments," +
                    " but found [" + arguments.size() + "].");
            }
        } else {
            types = new Type[arguments.size()];
            Arrays.fill(types, definition.defType);

            callenmd.target = name;
            callenmd.type = definition.defType;
            parentemd.statement = !parentemd.read && callenmd.last;
            parentemd.current = callenmd.type;
        }

        for (int argument = 0; argument < size; ++argument) {
            final ExpressionContext exprctx = AnalyzerUtility.updateExpressionTree(arguments.get(argument));
            final ExpressionMetadata expremd = metadata.createExpressionMetadata(exprctx);
            expremd.to = types[argument];
            analyzer.visit(exprctx);
            caster.markCast(expremd);
        }

        parentemd.statik = false;

        if (dotctx != null) {
            metadata.createExtNodeMetadata(parent, dotctx);
            analyzer.visit(dotctx);
        } else if (bracectx != null) {
            metadata.createExtNodeMetadata(parent, bracectx);
            analyzer.visit(bracectx);
        }
    }

    void processExtvar(final ExtvarContext ctx) {
        final ExtNodeMetadata varenmd = metadata.getExtNodeMetadata(ctx);
        final ParserRuleContext parent = varenmd.parent;
        final ExternalMetadata parentemd = metadata.getExternalMetadata(parent);

        final IdentifierContext idctx = ctx.identifier();
        final String id = idctx.getText();

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        final boolean type = utility.isValidType(idctx, false);

        if (type) {
            if (parentemd.current != null || dotctx == null || bracectx != null) {
                throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Unexpected static type [" + id + "].");
            }

            varenmd.type = definition.getType(id);
            parentemd.current = varenmd.type;
            parentemd.statik = true;

            metadata.createExtNodeMetadata(parent, dotctx);
            analyzer.visit(dotctx);
        } else {
            utility.isValidIdentifier(idctx, true);

            if (parentemd.current != null) {
                throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected variable [" + id + "] load.");
            }

            varenmd.last = parentemd.scope == 0 && dotctx == null && bracectx == null;

            final Variable variable = utility.getVariable(id);

            if (variable == null) {
                throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Unknown variable [" + id + "].");
            }

            varenmd.target = variable.slot;
            varenmd.type = variable.type;
            analyzeLoadStoreExternal(ctx);
            parentemd.current = varenmd.type;

            if (dotctx != null) {
                metadata.createExtNodeMetadata(parent, dotctx);
                analyzer.visit(dotctx);
            } else if (bracectx != null) {
                metadata.createExtNodeMetadata(parent, bracectx);
                analyzer.visit(bracectx);
            }
        }
    }

    void processExtfield(final ExtfieldContext ctx) {
        final ExtNodeMetadata memberenmd = metadata.getExtNodeMetadata(ctx);
        final ParserRuleContext parent = memberenmd.parent;
        final ExternalMetadata parentemd = metadata.getExternalMetadata(parent);

        if (ctx.EXTID() == null && ctx.EXTINTEGER() == null) {
            throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Unexpected state.");
        }

        final String value = ctx.EXTID() == null ? ctx.EXTINTEGER().getText() : ctx.EXTID().getText();

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        memberenmd.last = parentemd.scope == 0 && dotctx == null && bracectx == null;
        final boolean store = memberenmd.last && parentemd.storeExpr != null;

        if (parentemd.current == null) {
            throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected field [" + value + "] load.");
        }

        if (parentemd.current.sort == Sort.ARRAY) {
            if ("length".equals(value)) {
                if (!parentemd.read) {
                    throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Must read array field [length].");
                } else if (store) {
                    throw new IllegalArgumentException(
                        AnalyzerUtility.error(ctx) + "Cannot write to read-only array field [length].");
                }

                memberenmd.target = "#length";
                memberenmd.type = definition.intType;
                parentemd.current = definition.intType;
            } else {
                throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Unexpected array field [" + value + "].");
            }
        } else if (parentemd.current.sort == Sort.DEF) {
            memberenmd.target = value;
            memberenmd.type = definition.defType;
            analyzeLoadStoreExternal(ctx);
            parentemd.current = memberenmd.type;
        } else {
            final Struct struct = parentemd.current.struct;
            final Field field = parentemd.statik ? struct.statics.get(value) : struct.members.get(value);

            if (field != null) {
                if (store && java.lang.reflect.Modifier.isFinal(field.reflect.getModifiers())) {
                    throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Cannot write to read-only" +
                        " field [" + value + "] for type [" + struct.name + "].");
                }

                memberenmd.target = field;
                memberenmd.type = field.type;
                analyzeLoadStoreExternal(ctx);
                parentemd.current = memberenmd.type;
            } else {
                final boolean get = parentemd.read || parentemd.token > 0 || !memberenmd.last;
                final boolean set = memberenmd.last && store;

                Method getter = struct.methods.get("get" + Character.toUpperCase(value.charAt(0)) + value.substring(1));
                Method setter = struct.methods.get("set" + Character.toUpperCase(value.charAt(0)) + value.substring(1));
                Object constant = null;

                if (getter != null && (getter.rtn.sort == Sort.VOID || !getter.arguments.isEmpty())) {
                    throw new IllegalArgumentException(AnalyzerUtility.error(ctx) +
                        "Illegal get shortcut on field [" + value + "] for type [" + struct.name + "].");
                }

                if (setter != null && (setter.rtn.sort != Sort.VOID || setter.arguments.size() != 1)) {
                    throw new IllegalArgumentException(AnalyzerUtility.error(ctx) +
                        "Illegal set shortcut on field [" + value + "] for type [" + struct.name + "].");
                }

                Type settype = setter == null ? null : setter.arguments.get(0);

                if (getter == null && setter == null) {
                    if (ctx.EXTID() != null) {
                        try {
                            parentemd.current.clazz.asSubclass(Map.class);

                            getter = parentemd.current.struct.methods.get("get");
                            setter = parentemd.current.struct.methods.get("put");

                            if (getter != null && (getter.rtn.sort == Sort.VOID || getter.arguments.size() != 1 ||
                                getter.arguments.get(0).sort != Sort.STRING)) {
                                throw new IllegalArgumentException(AnalyzerUtility.error(ctx) +
                                    "Illegal map get shortcut [" + value + "] for type [" + struct.name + "].");
                            }

                            if (setter != null && (setter.arguments.size() != 2 ||
                                setter.arguments.get(0).sort != Sort.STRING)) {
                                throw new IllegalArgumentException(AnalyzerUtility.error(ctx) +
                                    "Illegal map set shortcut [" + value + "] for type [" + struct.name + "].");
                            }

                            if (getter != null && setter != null && !getter.rtn.equals(setter.arguments.get(1))) {
                                throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Shortcut argument types must match.");
                            }

                            settype = setter == null ? null : setter.arguments.get(1);
                            constant = value;
                        } catch (ClassCastException exception) {
                            //Do nothing.
                        }
                    } else if (ctx.EXTINTEGER() != null) {
                        try {
                            parentemd.current.clazz.asSubclass(List.class);

                            getter = parentemd.current.struct.methods.get("get");
                            setter = parentemd.current.struct.methods.get("set");

                            if (getter != null && (getter.rtn.sort == Sort.VOID || getter.arguments.size() != 1 ||
                                getter.arguments.get(0).sort != Sort.INT)) {
                                throw new IllegalArgumentException(AnalyzerUtility.error(ctx) +
                                    "Illegal list get shortcut [" + value + "] for type [" + struct.name + "].");
                            }

                            if (setter != null && (setter.rtn.sort != Sort.VOID || setter.arguments.size() != 2 ||
                                setter.arguments.get(0).sort != Sort.INT)) {
                                throw new IllegalArgumentException(AnalyzerUtility.error(ctx) +
                                    "Illegal list set shortcut [" + value + "] for type [" + struct.name + "].");
                            }

                            if (getter != null && setter != null && !getter.rtn.equals(setter.arguments.get(1))) {
                                throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Shortcut argument types must match.");
                            }

                            settype = setter == null ? null : setter.arguments.get(1);

                            try {
                                constant = Integer.parseInt(value);
                            } catch (NumberFormatException exception) {
                                throw new IllegalArgumentException(AnalyzerUtility.error(ctx) +
                                    "Illegal list shortcut value [" + value + "].");
                            }
                        } catch (ClassCastException exception) {
                            //Do nothing.
                        }
                    } else {
                        throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
                    }
                }

                if ((get || set) && (!get || getter != null) && (!set || setter != null)) {
                    memberenmd.target = new Object[] {getter, setter, constant != null, constant};
                    memberenmd.type = get ? getter.rtn : settype;
                    analyzeLoadStoreExternal(ctx);
                    parentemd.current = get ? getter.rtn : setter.rtn;
                }
            }

            if (memberenmd.target == null) {
                throw new IllegalArgumentException(
                    AnalyzerUtility.error(ctx) + "Unknown field [" + value + "] for type [" + struct.name + "].");
            }
        }

        parentemd.statik = false;

        if (dotctx != null) {
            metadata.createExtNodeMetadata(parent, dotctx);
            analyzer.visit(dotctx);
        } else if (bracectx != null) {
            metadata.createExtNodeMetadata(parent, bracectx);
            analyzer.visit(bracectx);
        }
    }

    void processExtnew(final ExtnewContext ctx) {
        final ExtNodeMetadata newenmd = metadata.getExtNodeMetadata(ctx);
        final ParserRuleContext parent = newenmd.parent;
        final ExternalMetadata parentemd = metadata.getExternalMetadata(parent);

        final ExtdotContext dotctx = ctx.extdot();
        newenmd.last = parentemd.scope == 0 && dotctx == null;

        final IdentifierContext idctx = ctx.identifier();
        final String type = idctx.getText();
        utility.isValidType(idctx, true);

        if (parentemd.current != null) {
            throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Unexpected new call.");
        } else if (newenmd.last && parentemd.storeExpr != null) {
            throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Cannot assign a value to a new call.");
        }

        final Struct struct = definition.structs.get(type);

        final boolean newclass = ctx.arguments() != null;
        final boolean newarray = !ctx.expression().isEmpty();

        final List<ExpressionContext> arguments = newclass ? ctx.arguments().expression() : ctx.expression();
        final int size = arguments.size();

        Type[] types;

        if (newarray) {
            if (!parentemd.read) {
                throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "A newly created array must be assigned.");
            }

            types = new Type[size];
            Arrays.fill(types, definition.intType);

            newenmd.target = "#makearray";

            if (size > 1) {
                newenmd.type = definition.getType(struct, size);
                parentemd.current = newenmd.type;
            } else if (size == 1) {
                newenmd.type = definition.getType(struct, 0);
                parentemd.current = definition.getType(struct, 1);
            } else {
                throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "A newly created array cannot have zero dimensions.");
            }
        } else if (newclass) {
            final Constructor constructor = struct.constructors.get("new");

            if (constructor != null) {
                types = new Type[constructor.arguments.size()];
                constructor.arguments.toArray(types);

                newenmd.target = constructor;
                newenmd.type = definition.getType(struct, 0);
                parentemd.statement = !parentemd.read && newenmd.last;
                parentemd.current = newenmd.type;
            } else {
                throw new IllegalArgumentException(
                    AnalyzerUtility.error(ctx) + "Unknown new call on type [" + struct.name + "].");
            }
        } else {
            throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Unknown state.");
        }

        if (size != types.length) {
            throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "When calling constructor on type " +
                "[" + struct.name + "] expected [" + types.length + "] arguments," +
                " but found [" + arguments.size() + "].");
        }

        for (int argument = 0; argument < size; ++argument) {
            final ExpressionContext exprctx = AnalyzerUtility.updateExpressionTree(arguments.get(argument));
            final ExpressionMetadata expremd = metadata.createExpressionMetadata(exprctx);
            expremd.to = types[argument];
            analyzer.visit(exprctx);
            caster.markCast(expremd);
        }

        if (dotctx != null) {
            metadata.createExtNodeMetadata(parent, dotctx);
            analyzer.visit(dotctx);
        }
    }

    void processExtstring(final ExtstringContext ctx) {
        final ExtNodeMetadata memberenmd = metadata.getExtNodeMetadata(ctx);
        final ParserRuleContext parent = memberenmd.parent;
        final ExternalMetadata parentemd = metadata.getExternalMetadata(parent);

        final String string = ctx.STRING().getText().substring(1, ctx.STRING().getText().length() - 1);

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        memberenmd.last = parentemd.scope == 0 && dotctx == null && bracectx == null;
        final boolean store = memberenmd.last && parentemd.storeExpr != null;

        if (parentemd.current != null) {
            throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected String constant [" + string + "].");
        }

        if (!parentemd.read) {
            throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Must read String constant [" + string + "].");
        } else if (store) {
            throw new IllegalArgumentException(
                AnalyzerUtility.error(ctx) + "Cannot write to read-only String constant [" + string + "].");
        }

        memberenmd.target = string;
        memberenmd.type = definition.stringType;
        parentemd.current = definition.stringType;

        if (memberenmd.last) {
            parentemd.constant = string;
        }

        if (dotctx != null) {
            metadata.createExtNodeMetadata(parent, dotctx);
            analyzer.visit(dotctx);
        } else if (bracectx != null) {
            metadata.createExtNodeMetadata(parent, bracectx);
            analyzer.visit(bracectx);
        }
    }

    private void analyzeLoadStoreExternal(final ParserRuleContext source) {
        final ExtNodeMetadata extenmd = metadata.getExtNodeMetadata(source);
        final ParserRuleContext parent = extenmd.parent;
        final ExternalMetadata parentemd = metadata.getExternalMetadata(parent);

        if (extenmd.last && parentemd.storeExpr != null) {
            final ParserRuleContext store = parentemd.storeExpr;
            final ExpressionMetadata storeemd = metadata.createExpressionMetadata(parentemd.storeExpr);
            final int token = parentemd.token;

            if (token > 0) {
                analyzer.visit(store);

                final boolean add = token == ADD;
                final boolean xor = token == BWAND || token == BWXOR || token == BWOR;
                final boolean decimal = token == MUL || token == DIV || token == REM || token == SUB;

                extenmd.promote = add ? promoter.promoteAdd(extenmd.type, storeemd.from) :
                    xor ? promoter.promoteXor(extenmd.type, storeemd.from) :
                        promoter.promoteNumeric(extenmd.type, storeemd.from, decimal, true);

                if (extenmd.promote == null) {
                    throw new IllegalArgumentException("Cannot apply compound assignment to " +
                        "types [" + extenmd.type.name + "] and [" + storeemd.from.name + "].");
                }

                extenmd.castFrom = caster.getLegalCast(source, extenmd.type, extenmd.promote, false);
                extenmd.castTo = caster.getLegalCast(source, extenmd.promote, extenmd.type, true);

                storeemd.to = add && extenmd.promote.sort == Sort.STRING ? storeemd.from : extenmd.promote;
                caster.markCast(storeemd);
            } else {
                storeemd.to = extenmd.type;
                analyzer.visit(store);
                caster.markCast(storeemd);
            }
        }
    }
}
