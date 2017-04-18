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

import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Type;

import java.util.Objects;

import static org.elasticsearch.painless.Definition.BOOLEAN_TYPE;
import static org.elasticsearch.painless.Definition.BYTE_TYPE;
import static org.elasticsearch.painless.Definition.CHAR_TYPE;
import static org.elasticsearch.painless.Definition.DEF_TYPE;
import static org.elasticsearch.painless.Definition.DOUBLE_TYPE;
import static org.elasticsearch.painless.Definition.FLOAT_TYPE;
import static org.elasticsearch.painless.Definition.INT_TYPE;
import static org.elasticsearch.painless.Definition.LONG_TYPE;
import static org.elasticsearch.painless.Definition.OBJECT_TYPE;
import static org.elasticsearch.painless.Definition.SHORT_TYPE;
import static org.elasticsearch.painless.Definition.STRING_TYPE;
import static org.elasticsearch.painless.WriterConstants.CHAR_TO_STRING;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_BOOLEAN;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_BYTE_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_BYTE_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_CHAR_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_CHAR_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_DOUBLE_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_DOUBLE_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_FLOAT_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_FLOAT_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_INT_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_INT_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_LONG_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_LONG_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_SHORT_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_SHORT_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_UTIL_TYPE;
import static org.elasticsearch.painless.WriterConstants.STRING_TO_CHAR;
import static org.elasticsearch.painless.WriterConstants.UTILITY_TYPE;

/**
 * Used during the analysis phase to collect legal type casts and promotions
 * for type-checking and later to write necessary casts in the bytecode.
 */
public final class AnalyzerCaster {

    @SuppressWarnings("incomplete-switch") // Missing arms mean the cast is not supported
    public static Cast getLegalCast(Location location, Type actual, Type expected, boolean explicit, boolean internal) {
        Objects.requireNonNull(actual);
        Objects.requireNonNull(expected);

        if (actual.equals(expected)) {
            return Cast.NOOP;
        }

        switch (actual.sort) {
            case BOOL:
                switch (expected.sort) {
                    case DEF:
                        return new Cast.Box(BOOLEAN_TYPE);
                    case OBJECT:
                        if (OBJECT_TYPE.equals(expected) && internal) return new Cast.Box(BOOLEAN_TYPE);
                        break;
                    case BOOL_OBJ:
                        if (internal) return new Cast.Box(BOOLEAN_TYPE);
                        break;
                }
                break;
            case BYTE:
                switch (expected.sort) {
                    case SHORT:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        return new Cast.Numeric(BYTE_TYPE, expected);
                    case CHAR:
                        if (explicit) return new Cast.Numeric(BYTE_TYPE, expected);
                        break;
                    case DEF:
                        return new Cast.Box(BYTE_TYPE);
                    case OBJECT:
                        if (OBJECT_TYPE.equals(expected) && internal) return new Cast.Box(BYTE_TYPE);
                        break;
                    case NUMBER:
                        if (internal) return new Cast.Box(BYTE_TYPE);
                        break;
                    case BYTE_OBJ:
                        if (internal) return new Cast.Box(BYTE_TYPE);
                        break;
                    case SHORT_OBJ:
                        if (internal) return new Cast.Numeric(BYTE_TYPE, SHORT_TYPE, new Cast.Box(SHORT_TYPE));
                        break;
                    case INT_OBJ:
                        if (internal) return new Cast.Numeric(BYTE_TYPE, INT_TYPE, new Cast.Box(INT_TYPE));
                        break;
                    case LONG_OBJ:
                        if (internal) return new Cast.Numeric(BYTE_TYPE, LONG_TYPE, new Cast.Box(LONG_TYPE));
                        break;
                    case FLOAT_OBJ:
                        if (internal) return new Cast.Numeric(BYTE_TYPE, FLOAT_TYPE, new Cast.Box(FLOAT_TYPE));
                        break;
                    case DOUBLE_OBJ:
                        if (internal) return new Cast.Numeric(BYTE_TYPE, DOUBLE_TYPE, new Cast.Box(DOUBLE_TYPE));
                        break;
                    case CHAR_OBJ:
                        if (explicit && internal) return new Cast.Numeric(BYTE_TYPE, CHAR_TYPE, new Cast.Box(CHAR_TYPE));
                        break;
                }
                break;
            case SHORT:
                switch (expected.sort) {
                    case BYTE:
                        if (explicit) return new Cast.Numeric(SHORT_TYPE, expected);
                        break;
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        return new Cast.Numeric(SHORT_TYPE, expected);
                    case CHAR:
                        if (explicit) return new Cast.Numeric(SHORT_TYPE, expected);
                        break;
                    case DEF:
                        return new Cast.Box(SHORT_TYPE);
                    case OBJECT:
                        if (OBJECT_TYPE.equals(expected) && internal) return new Cast.Box(SHORT_TYPE);
                        break;
                    case NUMBER:
                        if (internal) return new Cast.Box(SHORT_TYPE);
                        break;
                    case BYTE_OBJ:
                        if (explicit && internal) return new Cast.Numeric(SHORT_TYPE, BYTE_TYPE, new Cast.Box(BYTE_TYPE));
                        break;
                    case CHAR_OBJ:
                        if (explicit && internal) return new Cast.Numeric(SHORT_TYPE, CHAR_TYPE, new Cast.Box(CHAR_TYPE));
                        break;
                    case SHORT_OBJ:
                        if (internal) return new Cast.Box(SHORT_TYPE);
                        break;
                    case INT_OBJ:
                        if (internal) return new Cast.Numeric(SHORT_TYPE, INT_TYPE, new Cast.Box(INT_TYPE));
                        break;
                    case LONG_OBJ:
                        if (internal) return new Cast.Numeric(SHORT_TYPE, LONG_TYPE, new Cast.Box(LONG_TYPE));
                        break;
                    case FLOAT_OBJ:
                        if (internal) return new Cast.Numeric(SHORT_TYPE, FLOAT_TYPE, new Cast.Box(FLOAT_TYPE));
                        break;
                    case DOUBLE_OBJ:
                        if (internal) return new Cast.Numeric(SHORT_TYPE, DOUBLE_TYPE, new Cast.Box(DOUBLE_TYPE));
                        break;
                }
                break;
            case CHAR:
                switch (expected.sort) {
                    case BYTE:
                    case SHORT:
                        if (explicit) return new Cast.Numeric(CHAR_TYPE, expected);
                        break;
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        return new Cast.Numeric(CHAR_TYPE, expected);
                    case DEF:
                        return new Cast.Box(CHAR_TYPE);
                    case OBJECT:
                        if (OBJECT_TYPE.equals(expected) && internal) return new Cast.Box(CHAR_TYPE);
                        break;
                    case CHAR_OBJ:
                        if (internal) return new Cast.Box(CHAR_TYPE);
                        break;
                    case STRING:
                        return new Cast.InvokeStatic(UTILITY_TYPE, CHAR_TO_STRING, c -> Utility.charToString((Character) c));
                    case BYTE_OBJ:
                        if (explicit && internal) return new Cast.Numeric(CHAR_TYPE, BYTE_TYPE, new Cast.Box(BYTE_TYPE));
                        break;
                    case SHORT_OBJ:
                        if (explicit && internal) return new Cast.Numeric(CHAR_TYPE, SHORT_TYPE, new Cast.Box(SHORT_TYPE));
                        break;
                    case INT_OBJ:
                        if (internal) return new Cast.Numeric(CHAR_TYPE, INT_TYPE, new Cast.Box(INT_TYPE));
                        break;
                    case LONG_OBJ:
                        if (internal) return new Cast.Numeric(CHAR_TYPE, LONG_TYPE, new Cast.Box(LONG_TYPE));
                        break;
                    case FLOAT_OBJ:
                        if (internal) return new Cast.Numeric(CHAR_TYPE, FLOAT_TYPE, new Cast.Box(FLOAT_TYPE));
                        break;
                    case DOUBLE_OBJ:
                        if (internal) return new Cast.Numeric(CHAR_TYPE, DOUBLE_TYPE, new Cast.Box(DOUBLE_TYPE));
                        break;
                }
                break;
            case INT:
                switch (expected.sort) {
                    case BYTE:
                    case SHORT:
                    case CHAR:
                        if (explicit) return new Cast.Numeric(INT_TYPE, expected);
                        break;
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        return new Cast.Numeric(INT_TYPE, expected);
                    case DEF:
                        return new Cast.Box(INT_TYPE);
                    case OBJECT:
                        if (OBJECT_TYPE.equals(expected) && internal) return new Cast.Box(INT_TYPE);
                        break;
                    case NUMBER:
                        if (internal) return new Cast.Box(INT_TYPE);
                        break;
                    case BYTE_OBJ:
                        if (explicit && internal) return new Cast.Numeric(INT_TYPE, BYTE_TYPE, new Cast.Box(BYTE_TYPE));
                        break;
                    case SHORT_OBJ:
                        if (explicit && internal) return new Cast.Numeric(INT_TYPE, SHORT_TYPE, new Cast.Box(SHORT_TYPE));
                        break;
                    case CHAR_OBJ:
                        if (explicit && internal) return new Cast.Numeric(INT_TYPE, CHAR_TYPE, new Cast.Box(CHAR_TYPE));
                        break;
                    case INT_OBJ:
                        if (internal) return new Cast.Box(INT_TYPE);
                        break;
                    case LONG_OBJ:
                        if (internal) return new Cast.Numeric(INT_TYPE, LONG_TYPE, new Cast.Box(LONG_TYPE));
                        break;
                    case FLOAT_OBJ:
                        if (internal) return new Cast.Numeric(INT_TYPE, FLOAT_TYPE, new Cast.Box(FLOAT_TYPE));
                        break;
                    case DOUBLE_OBJ:
                        if (internal) return new Cast.Numeric(INT_TYPE, DOUBLE_TYPE, new Cast.Box(DOUBLE_TYPE));
                        break;
                }
                break;
            case LONG:
                    switch (expected.sort) {
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                        if (explicit) return new Cast.Numeric(LONG_TYPE, expected);
                        break;
                    case FLOAT:
                    case DOUBLE:
                        return new Cast.Numeric(LONG_TYPE, expected);
                    case DEF:
                        return new Cast.Box(LONG_TYPE);
                    case OBJECT:
                        if (OBJECT_TYPE.equals(expected) && internal) return new Cast.Box(LONG_TYPE);
                        break;
                    case NUMBER:
                        if (internal) return new Cast.Box(LONG_TYPE);
                        break;
                    case BYTE_OBJ:
                        if (explicit && internal) return new Cast.Numeric(LONG_TYPE, BYTE_TYPE, new Cast.Box(BYTE_TYPE));
                        break;
                    case SHORT_OBJ:
                        if (explicit && internal) return new Cast.Numeric(LONG_TYPE, SHORT_TYPE, new Cast.Box(SHORT_TYPE));
                        break;
                    case CHAR_OBJ:
                        if (explicit && internal) return new Cast.Numeric(LONG_TYPE, CHAR_TYPE, new Cast.Box(CHAR_TYPE));
                        break;
                    case INT_OBJ:
                        if (explicit && internal) return new Cast.Numeric(LONG_TYPE, INT_TYPE, new Cast.Box(INT_TYPE));
                        break;
                    case LONG_OBJ:
                        if (internal) return new Cast.Box(LONG_TYPE);
                        break;
                    case FLOAT_OBJ:
                        if (internal) return new Cast.Numeric(LONG_TYPE, FLOAT_TYPE, new Cast.Box(FLOAT_TYPE));
                        break;
                    case DOUBLE_OBJ:
                        if (internal) return new Cast.Numeric(LONG_TYPE, DOUBLE_TYPE, new Cast.Box(DOUBLE_TYPE));
                        break;
                }
                break;
            case FLOAT:
                switch (expected.sort) {
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case LONG:
                        if (explicit) return new Cast.Numeric(FLOAT_TYPE, expected);
                        break;
                    case DOUBLE:
                        return new Cast.Numeric(FLOAT_TYPE, expected);
                    case DEF:
                        return new Cast.Box(FLOAT_TYPE);
                    case OBJECT:
                        if (OBJECT_TYPE.equals(expected) && internal) return new Cast.Box(FLOAT_TYPE);
                        break;
                    case NUMBER:
                        if (internal) return new Cast.Box(FLOAT_TYPE);
                        break;
                    case BYTE_OBJ:
                        if (explicit && internal) return new Cast.Numeric(FLOAT_TYPE, BYTE_TYPE, new Cast.Box(BYTE_TYPE));
                        break;
                    case SHORT_OBJ:
                        if (explicit && internal) return new Cast.Numeric(FLOAT_TYPE, SHORT_TYPE, new Cast.Box(SHORT_TYPE));
                        break;
                    case CHAR_OBJ:
                        if (explicit && internal) return new Cast.Numeric(FLOAT_TYPE, CHAR_TYPE, new Cast.Box(CHAR_TYPE));
                        break;
                    case INT_OBJ:
                        if (explicit && internal) return new Cast.Numeric(FLOAT_TYPE, INT_TYPE, new Cast.Box(INT_TYPE));
                        break;
                    case LONG_OBJ:
                        if (explicit && internal) return new Cast.Numeric(FLOAT_TYPE, LONG_TYPE, new Cast.Box(LONG_TYPE));
                        break;
                    case FLOAT_OBJ:
                        if (internal) return new Cast.Box(FLOAT_TYPE);
                        break;
                    case DOUBLE_OBJ:
                        if (internal) return new Cast.Numeric(FLOAT_TYPE, DOUBLE_TYPE, new Cast.Box(DOUBLE_TYPE));
                        break;
                }

                break;
            case DOUBLE:
                switch (expected.sort) {
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case LONG:
                    case FLOAT:
                        if (explicit) return new Cast.Numeric(DOUBLE_TYPE, expected);
                        break;
                    case DEF:
                        return new Cast.Box(DOUBLE_TYPE);
                    case OBJECT:
                        if (OBJECT_TYPE.equals(expected) && internal) return new Cast.Box(DOUBLE_TYPE);
                        break;
                    case NUMBER:
                        if (internal) return new Cast.Box(DOUBLE_TYPE);
                        break;
                    case BYTE_OBJ:
                        if (explicit && internal) return new Cast.Numeric(DOUBLE_TYPE, BYTE_TYPE, new Cast.Box(BYTE_TYPE));
                        break;
                    case SHORT_OBJ:
                        if (explicit && internal) return new Cast.Numeric(DOUBLE_TYPE, SHORT_TYPE, new Cast.Box(SHORT_TYPE));
                        break;
                    case CHAR_OBJ:
                        if (explicit && internal) return new Cast.Numeric(DOUBLE_TYPE, CHAR_TYPE, new Cast.Box(CHAR_TYPE));
                        break;
                    case INT_OBJ:
                        if (explicit && internal) return new Cast.Numeric(DOUBLE_TYPE, INT_TYPE, new Cast.Box(INT_TYPE));
                        break;
                    case LONG_OBJ:
                        if (explicit && internal) return new Cast.Numeric(DOUBLE_TYPE, LONG_TYPE, new Cast.Box(LONG_TYPE));
                        break;
                    case FLOAT_OBJ:
                        if (explicit && internal) return new Cast.Numeric(DOUBLE_TYPE, FLOAT_TYPE, new Cast.Box(FLOAT_TYPE));
                        break;
                    case DOUBLE_OBJ:
                        if (internal) return new Cast.Box(DOUBLE_TYPE);
                        break;
                }
                break;
            case OBJECT:
                if (OBJECT_TYPE.equals(actual))
                    switch (expected.sort) {
                        case BYTE:
                            if (internal && explicit) return new Cast.Unbox(BYTE_TYPE);
                            break;
                        case SHORT:
                            if (internal && explicit) return new Cast.Unbox(SHORT_TYPE);
                            break;
                        case CHAR:
                            if (internal && explicit) return new Cast.Unbox(CHAR_TYPE);
                            break;
                        case INT:
                            if (internal && explicit) return new Cast.Unbox(INT_TYPE);
                            break;
                        case LONG:
                            if (internal && explicit) return new Cast.Unbox(LONG_TYPE);
                            break;
                        case FLOAT:
                            if (internal && explicit) return new Cast.Unbox(FLOAT_TYPE);
                            break;
                        case DOUBLE:
                            if (internal && explicit) return new Cast.Unbox(DOUBLE_TYPE);
                            break;
                }
                break;
            case NUMBER:
                switch (expected.sort) {
                    case BYTE:
                        if (internal && explicit) return new Cast.Unbox(BYTE_TYPE);
                        break;
                    case SHORT:
                        if (internal && explicit) return new Cast.Unbox(SHORT_TYPE);
                        break;
                    case CHAR:
                        if (internal && explicit) return new Cast.Unbox(CHAR_TYPE);
                        break;
                    case INT:
                        if (internal && explicit) return new Cast.Unbox(INT_TYPE);
                        break;
                    case LONG:
                        if (internal && explicit) return new Cast.Unbox(LONG_TYPE);
                        break;
                    case FLOAT:
                        if (internal && explicit) return new Cast.Unbox(FLOAT_TYPE);
                        break;
                    case DOUBLE:
                        if (internal && explicit) return new Cast.Unbox(DOUBLE_TYPE);
                        break;
                }
                break;
            case BOOL_OBJ:
                switch (expected.sort) {
                    case BOOL:
                        if (internal) return new Cast.Unbox(BOOLEAN_TYPE);
                        break;
                }
                break;
            case BYTE_OBJ:
                switch (expected.sort) {
                    case BYTE:
                    case SHORT:
                        if (internal) return new Cast.Unbox(expected);
                        break;
                    case CHAR:
                        if (internal && explicit) return new Cast.Unbox(BYTE_TYPE, new Cast.Numeric(BYTE_TYPE, CHAR_TYPE));
                        break;
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        if (internal) return new Cast.Unbox(expected);
                        break;
                }
                break;
            case SHORT_OBJ:
                switch (expected.sort) {
                    case BYTE:
                        if (internal && explicit) return new Cast.Unbox(expected);
                        break;
                    case SHORT:
                        if (internal) return new Cast.Unbox(expected);
                        break;
                    case CHAR:
                        if (internal && explicit) return new Cast.Unbox(SHORT_TYPE, new Cast.Numeric(SHORT_TYPE, CHAR_TYPE));
                        break;
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        if (internal) return new Cast.Unbox(expected);
                        break;
                }
                break;
            case CHAR_OBJ:
                switch (expected.sort) {
                    case BYTE:
                    case SHORT:
                        if (internal && explicit) return new Cast.Unbox(CHAR_TYPE, new Cast.Numeric(CHAR_TYPE, expected));
                        break;
                    case CHAR:
                        if (internal) return new Cast.Unbox(CHAR_TYPE);
                        break;
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        if (internal) return new Cast.Unbox(CHAR_TYPE, new Cast.Numeric(CHAR_TYPE, expected));
                        break;
                }
                break;
            case INT_OBJ:
                switch (expected.sort) {
                    case BYTE:
                    case SHORT:
                        if (internal && explicit) return new Cast.Unbox(expected);
                        break;
                    case CHAR:
                        if (internal && explicit) return new Cast.Unbox(INT_TYPE, new Cast.Numeric(INT_TYPE, CHAR_TYPE));
                        break;
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        if (internal) return new Cast.Unbox(expected);
                        break;
                }
                break;
            case LONG_OBJ:
                switch (expected.sort) {
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                        if (internal && explicit) return new Cast.Unbox(LONG_TYPE, new Cast.Numeric(LONG_TYPE, expected));
                        break;
                    case LONG:
                        if (internal) return new Cast.Unbox(LONG_TYPE);
                        break;
                    case FLOAT:
                    case DOUBLE:
                        if (internal) return new Cast.Unbox(LONG_TYPE, new Cast.Numeric(LONG_TYPE, expected));
                        break;
                }
                break;
            case FLOAT_OBJ:
                switch (expected.sort) {
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case LONG:
                        if (internal && explicit) return new Cast.Unbox(FLOAT_TYPE, new Cast.Numeric(FLOAT_TYPE, expected));
                        break;
                    case FLOAT:
                        if (internal) return new Cast.Unbox(FLOAT_TYPE);
                        break;
                    case DOUBLE:
                        if (internal) return new Cast.Unbox(FLOAT_TYPE, new Cast.Numeric(FLOAT_TYPE, DOUBLE_TYPE));
                        break;
                }
                break;
            case DOUBLE_OBJ:
                switch (expected.sort) {
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case LONG:
                    case FLOAT:
                        if (internal && explicit) return new Cast.Unbox(DOUBLE_TYPE, new Cast.Numeric(DOUBLE_TYPE, expected));
                        break;
                    case DOUBLE:
                        if (internal) return new Cast.Unbox(DOUBLE_TYPE);
                        break;
                }
                break;
            case DEF:
                switch (expected.sort) {
                    case BOOL:
                        return new Cast.InvokeStatic(DEF_UTIL_TYPE, DEF_TO_BOOLEAN, Def::DefToboolean);
                    case BYTE:
                        if (explicit) return new Cast.InvokeStatic(DEF_UTIL_TYPE, DEF_TO_BYTE_EXPLICIT, Def::DefTobyteExplicit);
                        return new Cast.InvokeStatic(DEF_UTIL_TYPE, DEF_TO_BYTE_IMPLICIT, Def::DefTobyteImplicit);
                    case SHORT:
                        if (explicit) return new Cast.InvokeStatic(DEF_UTIL_TYPE, DEF_TO_SHORT_EXPLICIT, Def::DefToshortExplicit);
                        return new Cast.InvokeStatic(DEF_UTIL_TYPE, DEF_TO_SHORT_IMPLICIT, Def::DefToshortImplicit);
                    case CHAR:
                        if (explicit) return new Cast.InvokeStatic(DEF_UTIL_TYPE, DEF_TO_CHAR_EXPLICIT, Def::DefTocharExplicit);
                        return new Cast.InvokeStatic(DEF_UTIL_TYPE, DEF_TO_CHAR_IMPLICIT, Def::DefTocharImplicit);
                    case INT:
                        if (explicit) return new Cast.InvokeStatic(DEF_UTIL_TYPE, DEF_TO_INT_EXPLICIT, Def::DefTointExplicit);
                        return new Cast.InvokeStatic(DEF_UTIL_TYPE, DEF_TO_INT_IMPLICIT, Def::DefTointImplicit);
                    case LONG:
                        if (explicit) return new Cast.InvokeStatic(DEF_UTIL_TYPE, DEF_TO_LONG_EXPLICIT, Def::DefTolongExplicit);
                        return new Cast.InvokeStatic(DEF_UTIL_TYPE, DEF_TO_LONG_IMPLICIT, Def::DefTolongImplicit);
                    case FLOAT:
                        if (explicit) return new Cast.InvokeStatic(DEF_UTIL_TYPE, DEF_TO_FLOAT_EXPLICIT, Def::DefTofloatExplicit);
                        return new Cast.InvokeStatic(DEF_UTIL_TYPE, DEF_TO_FLOAT_IMPLICIT, Def::DefTofloatImplicit);
                    case DOUBLE:
                        if (explicit) return new Cast.InvokeStatic(DEF_UTIL_TYPE, DEF_TO_DOUBLE_EXPLICIT, Def::DefTodoubleExplicit);
                        return new Cast.InvokeStatic(DEF_UTIL_TYPE, DEF_TO_DOUBLE_IMPLICIT, Def::DefTodoubleImplicit);
                }
                break;
            case STRING:
                switch (expected.sort) {
                    case CHAR:
                        if (explicit) return new Cast.InvokeStatic(UTILITY_TYPE, STRING_TO_CHAR, c -> Utility.StringTochar((String) c));
                        break;
                }
                break;
        }

        if (expected.clazz.isAssignableFrom(actual.clazz)) {
            return Cast.NOOP;
        }
        if (explicit && actual.clazz.isAssignableFrom(expected.clazz)) {
            return new Cast.CheckedCast(expected);
        }
        if (actual.sort == Sort.DEF) {
            return new Cast.CheckedCast(expected);
        }
        throw location.createError(new ClassCastException("Cannot cast from [" + actual.name + "] to [" + expected.name + "]."));
    }

    public static Type promoteNumeric(Type from, boolean decimal) {
        final Sort sort = from.sort;

        if (sort == Sort.DEF) {
            return DEF_TYPE;
        } else if ((sort == Sort.DOUBLE) && decimal) {
            return DOUBLE_TYPE;
        } else if ((sort == Sort.FLOAT) && decimal) {
            return  FLOAT_TYPE;
        } else if (sort == Sort.LONG) {
            return LONG_TYPE;
        } else if (sort == Sort.INT || sort == Sort.CHAR || sort == Sort.SHORT || sort == Sort.BYTE) {
            return INT_TYPE;
        }

        return null;
    }

    public static Type promoteNumeric(Type from0, Type from1, boolean decimal) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.DEF || sort1 == Sort.DEF) {
            return DEF_TYPE;
        }

        if (decimal) {
            if (sort0 == Sort.DOUBLE || sort1 == Sort.DOUBLE) {
                return DOUBLE_TYPE;
            } else if (sort0 == Sort.FLOAT || sort1 == Sort.FLOAT) {
                return FLOAT_TYPE;
            }
        }

        if (sort0 == Sort.LONG || sort1 == Sort.LONG) {
            return LONG_TYPE;
        } else if (sort0 == Sort.INT   || sort1 == Sort.INT   ||
                   sort0 == Sort.CHAR  || sort1 == Sort.CHAR  ||
                   sort0 == Sort.SHORT || sort1 == Sort.SHORT ||
                   sort0 == Sort.BYTE  || sort1 == Sort.BYTE) {
            return INT_TYPE;
        }

        return null;
    }

    public static Type promoteAdd(final Type from0, final Type from1) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.STRING || sort1 == Sort.STRING) {
            return STRING_TYPE;
        }

        return promoteNumeric(from0, from1, true);
    }

    public static Type promoteXor(final Type from0, final Type from1) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.DEF || sort1 == Sort.DEF) {
            return DEF_TYPE;
        }

        if (sort0.bool || sort1.bool) {
            return BOOLEAN_TYPE;
        }

        return promoteNumeric(from0, from1, false);
    }

    public static Type promoteEquality(final Type from0, final Type from1) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.DEF || sort1 == Sort.DEF) {
            return DEF_TYPE;
        }

        if (sort0.primitive && sort1.primitive) {
            if (sort0.bool && sort1.bool) {
                return BOOLEAN_TYPE;
            }

            if (sort0.numeric && sort1.numeric) {
                return promoteNumeric(from0, from1, true);
            }
        }

        return OBJECT_TYPE;
    }

    public static Type promoteConditional(final Type from0, final Type from1, final Object const0, final Object const1) {
        if (from0.equals(from1)) {
            return from0;
        }

        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.DEF || sort1 == Sort.DEF) {
            return DEF_TYPE;
        }

        if (sort0.primitive && sort1.primitive) {
            if (sort0.bool && sort1.bool) {
                return BOOLEAN_TYPE;
            }

            if (sort0 == Sort.DOUBLE || sort1 == Sort.DOUBLE) {
                return DOUBLE_TYPE;
            } else if (sort0 == Sort.FLOAT || sort1 == Sort.FLOAT) {
                return FLOAT_TYPE;
            } else if (sort0 == Sort.LONG || sort1 == Sort.LONG) {
                return LONG_TYPE;
            } else {
                if (sort0 == Sort.BYTE) {
                    if (sort1 == Sort.BYTE) {
                        return BYTE_TYPE;
                    } else if (sort1 == Sort.SHORT) {
                        if (const1 != null) {
                            final short constant = (short)const1;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return BYTE_TYPE;
                            }
                        }

                        return SHORT_TYPE;
                    } else if (sort1 == Sort.CHAR) {
                        return INT_TYPE;
                    } else if (sort1 == Sort.INT) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return BYTE_TYPE;
                            }
                        }

                        return INT_TYPE;
                    }
                } else if (sort0 == Sort.SHORT) {
                    if (sort1 == Sort.BYTE) {
                        if (const0 != null) {
                            final short constant = (short)const0;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return BYTE_TYPE;
                            }
                        }

                        return SHORT_TYPE;
                    } else if (sort1 == Sort.SHORT) {
                        return SHORT_TYPE;
                    } else if (sort1 == Sort.CHAR) {
                        return INT_TYPE;
                    } else if (sort1 == Sort.INT) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Short.MAX_VALUE && constant >= Short.MIN_VALUE) {
                                return SHORT_TYPE;
                            }
                        }

                        return INT_TYPE;
                    }
                } else if (sort0 == Sort.CHAR) {
                    if (sort1 == Sort.BYTE) {
                        return INT_TYPE;
                    } else if (sort1 == Sort.SHORT) {
                        return INT_TYPE;
                    } else if (sort1 == Sort.CHAR) {
                        return CHAR_TYPE;
                    } else if (sort1 == Sort.INT) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Character.MAX_VALUE && constant >= Character.MIN_VALUE) {
                                return BYTE_TYPE;
                            }
                        }

                        return INT_TYPE;
                    }
                } else if (sort0 == Sort.INT) {
                    if (sort1 == Sort.BYTE) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return BYTE_TYPE;
                            }
                        }

                        return INT_TYPE;
                    } else if (sort1 == Sort.SHORT) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Short.MAX_VALUE && constant >= Short.MIN_VALUE) {
                                return BYTE_TYPE;
                            }
                        }

                        return INT_TYPE;
                    } else if (sort1 == Sort.CHAR) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Character.MAX_VALUE && constant >= Character.MIN_VALUE) {
                                return BYTE_TYPE;
                            }
                        }

                        return INT_TYPE;
                    } else if (sort1 == Sort.INT) {
                        return INT_TYPE;
                    }
                }
            }
        }

        // TODO: In the rare case we still haven't reached a correct promotion we need
        //       to calculate the highest upper bound for the two types and return that.
        //       However, for now we just return objectType that may require an extra cast.

        return OBJECT_TYPE;
    }

    private AnalyzerCaster() {}
}
