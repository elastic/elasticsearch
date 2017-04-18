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

import org.elasticsearch.painless.Definition.Cast;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Type;

import java.util.Objects;

import static org.elasticsearch.painless.Definition.BOOLEAN_OBJ_TYPE;
import static org.elasticsearch.painless.Definition.BOOLEAN_TYPE;
import static org.elasticsearch.painless.Definition.BYTE_OBJ_TYPE;
import static org.elasticsearch.painless.Definition.BYTE_TYPE;
import static org.elasticsearch.painless.Definition.CHAR_OBJ_TYPE;
import static org.elasticsearch.painless.Definition.CHAR_TYPE;
import static org.elasticsearch.painless.Definition.DEF_TYPE;
import static org.elasticsearch.painless.Definition.DOUBLE_OBJ_TYPE;
import static org.elasticsearch.painless.Definition.DOUBLE_TYPE;
import static org.elasticsearch.painless.Definition.FLOAT_OBJ_TYPE;
import static org.elasticsearch.painless.Definition.FLOAT_TYPE;
import static org.elasticsearch.painless.Definition.INT_OBJ_TYPE;
import static org.elasticsearch.painless.Definition.INT_TYPE;
import static org.elasticsearch.painless.Definition.LONG_OBJ_TYPE;
import static org.elasticsearch.painless.Definition.LONG_TYPE;
import static org.elasticsearch.painless.Definition.NUMBER_TYPE;
import static org.elasticsearch.painless.Definition.OBJECT_TYPE;
import static org.elasticsearch.painless.Definition.SHORT_OBJ_TYPE;
import static org.elasticsearch.painless.Definition.SHORT_TYPE;
import static org.elasticsearch.painless.Definition.STRING_TYPE;

/**
 * Used during the analysis phase to collect legal type casts and promotions
 * for type-checking and later to write necessary casts in the bytecode.
 */
public final class AnalyzerCaster {

    public static Cast getLegalCast(Location location, Type actual, Type expected, boolean explicit, boolean internal) {
        Objects.requireNonNull(actual);
        Objects.requireNonNull(expected);

        if (actual.equals(expected)) {
            return null;
        }

        switch (actual.sort) {
            case BOOL:
                switch (expected.sort) {
                    case DEF:
                        return new Cast(BOOLEAN_OBJ_TYPE, DEF_TYPE, explicit, null, null, BOOLEAN_TYPE, null);
                    case OBJECT:
                        if (OBJECT_TYPE.equals(expected) && internal)
                            return new Cast(BOOLEAN_OBJ_TYPE, OBJECT_TYPE, explicit, null, null, BOOLEAN_TYPE, null);

                        break;
                    case BOOL_OBJ:
                        if (internal)
                            return new Cast(BOOLEAN_TYPE, BOOLEAN_TYPE, explicit, null, null, null, BOOLEAN_TYPE);
                }

                break;
            case BYTE:
                switch (expected.sort) {
                    case SHORT:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        return new Cast(BYTE_TYPE, expected, explicit);
                    case CHAR:
                        if (explicit)
                            return new Cast(BYTE_TYPE, CHAR_TYPE, true);

                        break;
                    case DEF:
                        return new Cast(BYTE_OBJ_TYPE, DEF_TYPE, explicit, null, null, BYTE_TYPE, null);
                    case OBJECT:
                        if (OBJECT_TYPE.equals(expected) && internal)
                            return new Cast(BYTE_OBJ_TYPE, OBJECT_TYPE, explicit, null, null, BYTE_TYPE, null);

                        break;
                    case NUMBER:
                        if (internal)
                            return new Cast(BYTE_OBJ_TYPE, NUMBER_TYPE, explicit, null, null, BYTE_TYPE, null);

                        break;
                    case BYTE_OBJ:
                        if (internal)
                            return new Cast(BYTE_TYPE, BYTE_TYPE, explicit, null, null, null, BYTE_TYPE);

                        break;
                    case SHORT_OBJ:
                        if (internal)
                            return new Cast(BYTE_TYPE, SHORT_TYPE, explicit, null, null, null, SHORT_TYPE);

                        break;
                    case INT_OBJ:
                        if (internal)
                            return new Cast(BYTE_TYPE, INT_TYPE, explicit, null, null, null, INT_TYPE);

                        break;
                    case LONG_OBJ:
                        if (internal)
                            return new Cast(BYTE_TYPE, LONG_TYPE, explicit, null, null, null, LONG_TYPE);

                        break;
                    case FLOAT_OBJ:
                        if (internal)
                            return new Cast(BYTE_TYPE, FLOAT_TYPE, explicit, null, null, null, FLOAT_TYPE);

                        break;
                    case DOUBLE_OBJ:
                        if (internal)
                            return new Cast(BYTE_TYPE, DOUBLE_TYPE, explicit, null, null, null, DOUBLE_TYPE);

                        break;
                    case CHAR_OBJ:
                        if (explicit && internal)
                            return new Cast(BYTE_TYPE, CHAR_TYPE, true, null, null, null, CHAR_TYPE);

                        break;
                }

                break;
            case SHORT:
                switch (expected.sort) {
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        return new Cast(SHORT_TYPE, expected, explicit);
                    case BYTE:
                    case CHAR:
                        if (explicit)
                            return new Cast(SHORT_TYPE, expected, true);

                        break;
                    case DEF:
                        return new Cast(SHORT_OBJ_TYPE, DEF_TYPE, explicit, null, null, SHORT_TYPE, null);
                    case OBJECT:
                        if (OBJECT_TYPE.equals(expected) && internal)
                            return new Cast(SHORT_OBJ_TYPE, OBJECT_TYPE, explicit, null, null, SHORT_TYPE, null);

                        break;
                    case NUMBER:
                        if (internal)
                            return new Cast(SHORT_OBJ_TYPE, NUMBER_TYPE, explicit, null, null, SHORT_TYPE, null);

                        break;
                    case SHORT_OBJ:
                        if (internal)
                            return new Cast(SHORT_TYPE, SHORT_TYPE, explicit, null, null, null, SHORT_TYPE);

                        break;
                    case INT_OBJ:
                        if (internal)
                            return new Cast(SHORT_TYPE, INT_TYPE, explicit, null, null, null, INT_TYPE);

                        break;
                    case LONG_OBJ:
                        if (internal)
                            return new Cast(SHORT_TYPE, LONG_TYPE, explicit, null, null, null, LONG_TYPE);

                        break;
                    case FLOAT_OBJ:
                        if (internal)
                            return new Cast(SHORT_TYPE, FLOAT_TYPE, explicit, null, null, null, FLOAT_TYPE);

                        break;
                    case DOUBLE_OBJ:
                        if (internal)
                            return new Cast(SHORT_TYPE, DOUBLE_TYPE, explicit, null, null, null, DOUBLE_TYPE);

                        break;
                    case BYTE_OBJ:
                        if (explicit && internal)
                            return new Cast(SHORT_TYPE, BYTE_TYPE, true, null, null, null, BYTE_TYPE);

                        break;
                    case CHAR_OBJ:
                        if (explicit && internal)
                            return new Cast(SHORT_TYPE, CHAR_TYPE, true, null, null, null, CHAR_TYPE);

                        break;
                }

                break;
            case CHAR:
                switch (expected.sort) {
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        return new Cast(CHAR_TYPE, expected, explicit);
                    case BYTE:
                    case SHORT:
                        if (explicit)
                            return new Cast(actual, expected, true);

                        break;
                    case DEF:
                        return new Cast(CHAR_OBJ_TYPE, DEF_TYPE, explicit, null, null, CHAR_TYPE, null);
                    case OBJECT:
                        if (OBJECT_TYPE.equals(expected) && internal)
                            return new Cast(CHAR_OBJ_TYPE, OBJECT_TYPE, explicit, null, null, CHAR_TYPE, null);

                        break;
                    case NUMBER:
                        if (internal)
                            return new Cast(CHAR_OBJ_TYPE, NUMBER_TYPE, explicit, null, null, CHAR_TYPE, null);

                        break;
                    case CHAR_OBJ:
                        if (internal)
                            return new Cast(CHAR_TYPE, CHAR_TYPE, explicit, null, null, null, CHAR_TYPE);

                        break;
                    case STRING:
                        return new Cast(CHAR_TYPE, STRING_TYPE, explicit);
                    case INT_OBJ:
                        if (internal)
                            return new Cast(CHAR_TYPE, INT_TYPE, explicit, null, null, null, INT_TYPE);

                        break;
                    case LONG_OBJ:
                        if (internal)
                            return new Cast(CHAR_TYPE, LONG_TYPE, explicit, null, null, null, LONG_TYPE);

                        break;
                    case FLOAT_OBJ:
                        if (internal)
                            return new Cast(CHAR_TYPE, FLOAT_TYPE, explicit, null, null, null, FLOAT_TYPE);

                        break;
                    case DOUBLE_OBJ:
                        if (internal)
                            return new Cast(CHAR_TYPE, DOUBLE_TYPE, explicit, null, null, null, DOUBLE_TYPE);

                        break;
                    case BYTE_OBJ:
                        if (explicit && internal)
                            return new Cast(CHAR_TYPE, BYTE_TYPE, true, null, null, null, BYTE_TYPE);

                        break;
                    case SHORT_OBJ:
                        if (explicit && internal)
                            return new Cast(CHAR_TYPE, SHORT_TYPE, true, null, null, null, SHORT_TYPE);

                        break;
                }

                break;
            case INT:
                switch (expected.sort) {
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        return new Cast(INT_TYPE, expected, explicit);
                    case BYTE:
                    case SHORT:
                    case CHAR:
                        if (explicit)
                            return new Cast(INT_TYPE, expected, true);

                        break;
                    case DEF:
                        return new Cast(INT_OBJ_TYPE, DEF_TYPE, explicit, null, null, INT_TYPE, null);
                    case OBJECT:
                        if (OBJECT_TYPE.equals(expected) && internal)
                            return new Cast(INT_OBJ_TYPE, OBJECT_TYPE, explicit, null, null, INT_TYPE, null);

                        break;
                    case NUMBER:
                        if (internal)
                            return new Cast(INT_OBJ_TYPE, NUMBER_TYPE, explicit, null, null, INT_TYPE, null);

                        break;
                    case INT_OBJ:
                        if (internal)
                            return new Cast(INT_TYPE, INT_TYPE, explicit, null, null, null, INT_TYPE);

                        break;
                    case LONG_OBJ:
                        if (internal)
                            return new Cast(INT_TYPE, LONG_TYPE, explicit, null, null, null, LONG_TYPE);

                        break;
                    case FLOAT_OBJ:
                        if (internal)
                            return new Cast(INT_TYPE, FLOAT_TYPE, explicit, null, null, null, FLOAT_TYPE);

                        break;
                    case DOUBLE_OBJ:
                        if (internal)
                            return new Cast(INT_TYPE, DOUBLE_TYPE, explicit, null, null, null, DOUBLE_TYPE);

                        break;
                    case BYTE_OBJ:
                        if (explicit && internal)
                            return new Cast(INT_TYPE, BYTE_TYPE, true, null, null, null, BYTE_TYPE);

                        break;
                    case SHORT_OBJ:
                        if (explicit && internal)
                            return new Cast(INT_TYPE, SHORT_TYPE, true, null, null, null, SHORT_TYPE);

                        break;
                    case CHAR_OBJ:
                        if (explicit && internal)
                            return new Cast(INT_TYPE, CHAR_TYPE, true, null, null, null, CHAR_TYPE);

                        break;
                }

                break;
            case LONG:
                switch (expected.sort) {
                    case FLOAT:
                    case DOUBLE:
                        return new Cast(LONG_TYPE, expected, explicit);
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                        if (explicit)
                            return new Cast(actual, expected, true);

                        break;
                    case DEF:
                        return new Cast(LONG_TYPE, DEF_TYPE, explicit, null, null, LONG_TYPE, null);
                    case OBJECT:
                        if (OBJECT_TYPE.equals(expected) && internal)
                            return new Cast(LONG_TYPE, actual, explicit, null, null, LONG_TYPE, null);

                        break;
                    case NUMBER:
                        if (internal)
                            return new Cast(LONG_OBJ_TYPE, NUMBER_TYPE, explicit, null, null, LONG_TYPE, null);

                        break;
                    case LONG_OBJ:
                        if (internal)
                            return new Cast(LONG_TYPE, LONG_TYPE, explicit, null, null, null, LONG_TYPE);

                        break;
                    case FLOAT_OBJ:
                        if (internal)
                            return new Cast(LONG_TYPE, FLOAT_TYPE, explicit, null, null, null, FLOAT_TYPE);

                        break;
                    case DOUBLE_OBJ:
                        if (internal)
                            return new Cast(LONG_TYPE, DOUBLE_TYPE, explicit, null, null, null, DOUBLE_TYPE);

                        break;
                    case BYTE_OBJ:
                        if (explicit && internal)
                            return new Cast(LONG_TYPE, BYTE_TYPE, true, null, null, null, BYTE_TYPE);

                        break;
                    case SHORT_OBJ:
                        if (explicit && internal)
                            return new Cast(LONG_TYPE, SHORT_TYPE, true, null, null, null, SHORT_TYPE);

                        break;
                    case CHAR_OBJ:
                        if (explicit && internal)
                            return new Cast(LONG_TYPE, CHAR_TYPE, true, null, null, null, CHAR_TYPE);

                        break;
                    case INT_OBJ:
                        if (explicit && internal)
                            return new Cast(LONG_TYPE, INT_TYPE, true, null, null, null, INT_TYPE);

                        break;
                }

                break;
            case FLOAT:
                switch (expected.sort) {
                    case DOUBLE:
                        return new Cast(actual, expected, explicit);
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case FLOAT:
                        if (explicit)
                            return new Cast(actual, expected, true);

                        break;
                    case DEF:
                        return new Cast(FLOAT_OBJ_TYPE, DEF_TYPE, explicit, null, null, FLOAT_TYPE, null);
                    case OBJECT:
                        if (OBJECT_TYPE.equals(expected) && internal)
                            return new Cast(FLOAT_OBJ_TYPE, OBJECT_TYPE, explicit, null, null, FLOAT_TYPE, null);

                        break;
                    case NUMBER:
                        if (internal)
                            return new Cast(FLOAT_OBJ_TYPE, NUMBER_TYPE, explicit, null, null, FLOAT_TYPE, null);

                        break;
                    case FLOAT_OBJ:
                        if (internal)
                            return new Cast(FLOAT_TYPE, FLOAT_TYPE, explicit, null, null, null, FLOAT_TYPE);

                        break;
                    case DOUBLE_OBJ:
                        if (internal)
                            return new Cast(FLOAT_TYPE, DOUBLE_TYPE, explicit, null, null, null, DOUBLE_TYPE);

                        break;
                    case BYTE_OBJ:
                        if (explicit && internal)
                            return new Cast(FLOAT_TYPE, BYTE_TYPE, true, null, null, null, BYTE_TYPE);

                        break;
                    case SHORT_OBJ:
                        if (explicit && internal)
                            return new Cast(FLOAT_TYPE, SHORT_TYPE, true, null, null, null, SHORT_TYPE);

                        break;
                    case CHAR_OBJ:
                        if (explicit && internal)
                            return new Cast(FLOAT_TYPE, CHAR_TYPE, true, null, null, null, CHAR_TYPE);

                        break;
                    case INT_OBJ:
                        if (explicit && internal)
                            return new Cast(FLOAT_TYPE, INT_TYPE, true, null, null, null, INT_TYPE);

                        break;
                    case LONG_OBJ:
                        if (explicit && internal)
                            return new Cast(FLOAT_TYPE, LONG_TYPE, true, null, null, null, LONG_TYPE);

                        break;
                }

                break;
            case DOUBLE:
                switch (expected.sort) {
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case FLOAT:
                        if (explicit)
                            return new Cast(DOUBLE_TYPE, expected, true);

                        break;
                    case DEF:
                        return new Cast(DOUBLE_OBJ_TYPE, DEF_TYPE, explicit, null, null, DOUBLE_TYPE, null);
                    case OBJECT:
                        if (OBJECT_TYPE.equals(expected) && internal)
                            return new Cast(DOUBLE_OBJ_TYPE, OBJECT_TYPE, explicit, null, null, DOUBLE_TYPE, null);

                        break;
                    case NUMBER:
                        if (internal)
                            return new Cast(DOUBLE_OBJ_TYPE, NUMBER_TYPE, explicit, null, null, DOUBLE_TYPE, null);

                        break;
                    case DOUBLE_OBJ:
                        if (internal)
                            return new Cast(DOUBLE_TYPE, DOUBLE_TYPE, explicit, null, null, null, DOUBLE_TYPE);

                        break;
                    case BYTE_OBJ:
                        if (explicit && internal)
                            return new Cast(DOUBLE_TYPE, BYTE_TYPE, true, null, null, null, BYTE_TYPE);

                        break;
                    case SHORT_OBJ:
                        if (explicit && internal)
                            return new Cast(DOUBLE_TYPE, SHORT_TYPE, true, null, null, null, SHORT_TYPE);

                        break;
                    case CHAR_OBJ:
                        if (explicit && internal)
                            return new Cast(DOUBLE_TYPE, CHAR_TYPE, true, null, null, null, CHAR_TYPE);

                        break;
                    case INT_OBJ:
                        if (explicit && internal)
                            return new Cast(DOUBLE_TYPE, INT_TYPE, true, null, null, null, INT_TYPE);

                        break;
                    case LONG_OBJ:
                        if (explicit && internal)
                            return new Cast(DOUBLE_TYPE, LONG_TYPE, true, null, null, null, LONG_TYPE);

                        break;
                    case FLOAT_OBJ:
                        if (explicit && internal)
                            return new Cast(DOUBLE_TYPE, FLOAT_TYPE, true, null, null, null, FLOAT_TYPE);

                        break;
                }

                break;
            case OBJECT:
                if (OBJECT_TYPE.equals(actual))
                    switch (expected.sort) {
                        case BYTE:
                            if (internal && explicit)
                                return new Cast(OBJECT_TYPE, BYTE_OBJ_TYPE, true, null, BYTE_TYPE, null, null);

                            break;
                        case SHORT:
                            if (internal && explicit)
                                return new Cast(OBJECT_TYPE, SHORT_OBJ_TYPE, true, null, SHORT_TYPE, null, null);

                            break;
                        case CHAR:
                            if (internal && explicit)
                                return new Cast(OBJECT_TYPE, CHAR_OBJ_TYPE, true, null, CHAR_TYPE, null, null);

                            break;
                        case INT:
                            if (internal && explicit)
                                return new Cast(OBJECT_TYPE, INT_OBJ_TYPE, true, null, INT_TYPE, null, null);

                            break;
                        case LONG:
                            if (internal && explicit)
                                return new Cast(OBJECT_TYPE, LONG_OBJ_TYPE, true, null, LONG_TYPE, null, null);

                            break;
                        case FLOAT:
                            if (internal && explicit)
                                return new Cast(OBJECT_TYPE, FLOAT_OBJ_TYPE, true, null, FLOAT_TYPE, null, null);

                            break;
                        case DOUBLE:
                            if (internal && explicit)
                                return new Cast(OBJECT_TYPE, DOUBLE_OBJ_TYPE, true, null, DOUBLE_TYPE, null, null);

                            break;
                    }
                break;
            case NUMBER:
                switch (expected.sort) {
                    case BYTE:
                        if (internal && explicit)
                            return new Cast(NUMBER_TYPE, BYTE_OBJ_TYPE, true, null, BYTE_TYPE, null, null);

                        break;
                    case SHORT:
                        if (internal && explicit)
                            return new Cast(NUMBER_TYPE, SHORT_OBJ_TYPE, true, null, SHORT_TYPE, null, null);

                        break;
                    case CHAR:
                        if (internal && explicit)
                            return new Cast(NUMBER_TYPE, CHAR_OBJ_TYPE, true, null, CHAR_TYPE, null, null);

                        break;
                    case INT:
                        if (internal && explicit)
                            return new Cast(NUMBER_TYPE, INT_OBJ_TYPE, true, null, INT_TYPE, null, null);

                        break;
                    case LONG:
                        if (internal && explicit)
                            return new Cast(NUMBER_TYPE, LONG_OBJ_TYPE, true, null, LONG_TYPE, null, null);

                        break;
                    case FLOAT:
                        if (internal && explicit)
                            return new Cast(NUMBER_TYPE, FLOAT_OBJ_TYPE, true, null, FLOAT_TYPE, null, null);

                        break;
                    case DOUBLE:
                        if (internal && explicit)
                            return new Cast(NUMBER_TYPE, DOUBLE_OBJ_TYPE, true, null, DOUBLE_TYPE, null, null);

                        break;
                }

                break;
            case BOOL_OBJ:
                switch (expected.sort) {
                    case BOOL:
                        if (internal)
                            return new Cast(BOOLEAN_TYPE, BOOLEAN_TYPE, explicit, BOOLEAN_TYPE, null, null, null);

                        break;
                }

                break;
            case BYTE_OBJ:
                switch (expected.sort) {
                    case BYTE:
                    case SHORT:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        if (internal)
                            return new Cast(BYTE_TYPE, expected, explicit, BYTE_TYPE, null, null, null);

                        break;
                    case CHAR:
                        if (internal && explicit)
                            return new Cast(BYTE_TYPE, expected, true, BYTE_TYPE, null, null, null);

                        break;
                }

                break;
            case SHORT_OBJ:
                switch (expected.sort) {
                    case SHORT:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        if (internal)
                            return new Cast(SHORT_TYPE, expected, explicit, SHORT_TYPE, null, null, null);

                        break;
                    case BYTE:
                    case CHAR:
                        if (internal && explicit)
                            return new Cast(SHORT_TYPE, expected, true, SHORT_TYPE, null, null, null);

                        break;
                }

                break;
            case CHAR_OBJ:
                switch (expected.sort) {
                    case CHAR:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        if (internal)
                            return new Cast(CHAR_TYPE, expected, explicit, CHAR_TYPE, null, null, null);

                        break;
                    case BYTE:
                    case SHORT:
                        if (internal && explicit)
                            return new Cast(CHAR_TYPE, expected, true, CHAR_TYPE, null, null, null);

                        break;
                }

                break;
            case INT_OBJ:
                switch (expected.sort) {
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        if (internal)
                            return new Cast(INT_TYPE, expected, explicit, INT_TYPE, null, null, null);

                        break;
                    case BYTE:
                    case SHORT:
                    case CHAR:
                        if (internal && explicit)
                            return new Cast(INT_TYPE, expected, true, INT_TYPE, null, null, null);

                        break;
                }

                break;
            case LONG_OBJ:
                switch (expected.sort) {
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        if (internal)
                            return new Cast(LONG_TYPE, expected, explicit, LONG_TYPE, null, null, null);

                        break;
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                        if (internal && explicit)
                            return new Cast(LONG_TYPE, expected, true, LONG_TYPE, null, null, null);

                        break;
                }

                break;
            case FLOAT_OBJ:
                switch (expected.sort) {
                    case FLOAT:
                    case DOUBLE:
                        if (internal)
                            return new Cast(FLOAT_TYPE, expected, explicit, FLOAT_TYPE, null, null, null);

                        break;
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case LONG:
                        if (internal && explicit)
                            return new Cast(FLOAT_TYPE, expected, true, FLOAT_TYPE, null, null, null);

                        break;
                }

                break;
            case DOUBLE_OBJ:
                switch (expected.sort) {
                    case DOUBLE:
                        if (internal)
                            return new Cast(DOUBLE_TYPE, expected, explicit, DOUBLE_TYPE, null, null, null);

                        break;
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case LONG:
                    case FLOAT:
                        if (internal && explicit)
                            return new Cast(DOUBLE_TYPE, expected, true, DOUBLE_TYPE, null, null, null);

                        break;
                }

                break;
            case DEF:
                switch (expected.sort) {
                    case BOOL:
                        return new Cast(DEF_TYPE, BOOLEAN_OBJ_TYPE, explicit, null, BOOLEAN_TYPE, null, null);
                    case BYTE:
                        return new Cast(DEF_TYPE, BYTE_OBJ_TYPE, explicit, null, BYTE_TYPE, null, null);
                    case SHORT:
                        return new Cast(DEF_TYPE, SHORT_OBJ_TYPE, explicit, null, SHORT_TYPE, null, null);
                    case CHAR:
                        return new Cast(DEF_TYPE, CHAR_OBJ_TYPE, explicit, null, CHAR_TYPE, null, null);
                    case INT:
                        return new Cast(DEF_TYPE, INT_OBJ_TYPE, explicit, null, INT_TYPE, null, null);
                    case LONG:
                        return new Cast(DEF_TYPE, LONG_OBJ_TYPE, explicit, null, LONG_TYPE, null, null);
                    case FLOAT:
                        return new Cast(DEF_TYPE, FLOAT_OBJ_TYPE, explicit, null, FLOAT_TYPE, null, null);
                    case DOUBLE:
                            return new Cast(DEF_TYPE, DOUBLE_OBJ_TYPE, explicit, null, DOUBLE_TYPE, null, null);
                }

                break;
            case STRING:
                switch (expected.sort) {
                    case CHAR:
                        if (explicit)
                            return new Cast(STRING_TYPE, CHAR_TYPE, true);

                        break;
                }

                break;
        }

        if (       actual.sort == Sort.DEF
                || (actual.sort != Sort.VOID && expected.sort == Sort.DEF)
                || expected.clazz.isAssignableFrom(actual.clazz)
                || (explicit && actual.clazz.isAssignableFrom(expected.clazz))) {
            return new Cast(actual, expected, explicit);
        } else {
            throw location.createError(new ClassCastException("Cannot cast from [" + actual.name + "] to [" + expected.name + "]."));
        }
    }

    public static Object constCast(Location location, final Object constant, final Cast cast) {
        final Sort fsort = cast.from.sort;
        final Sort tsort = cast.to.sort;

        if (fsort == tsort) {
            return constant;
        } else if (fsort == Sort.STRING && tsort == Sort.CHAR) {
            return Utility.StringTochar((String)constant);
        } else if (fsort == Sort.CHAR && tsort == Sort.STRING) {
            return Utility.charToString((char)constant);
        } else if (fsort.numeric && tsort.numeric) {
            final Number number;

            if (fsort == Sort.CHAR) {
                number = (int)(char)constant;
            } else {
                number = (Number)constant;
            }

            switch (tsort) {
                case BYTE:   return number.byteValue();
                case SHORT:  return number.shortValue();
                case CHAR:   return (char)number.intValue();
                case INT:    return number.intValue();
                case LONG:   return number.longValue();
                case FLOAT:  return number.floatValue();
                case DOUBLE: return number.doubleValue();
                default:
                    throw location.createError(new IllegalStateException("Cannot cast from " +
                        "[" + cast.from.clazz.getCanonicalName() + "] to [" + cast.to.clazz.getCanonicalName() + "]."));
            }
        } else {
            throw location.createError(new IllegalStateException("Cannot cast from " +
                "[" + cast.from.clazz.getCanonicalName() + "] to [" + cast.to.clazz.getCanonicalName() + "]."));
        }
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
