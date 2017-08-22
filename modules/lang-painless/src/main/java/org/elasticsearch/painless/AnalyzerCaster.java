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

        if (actual.dynamic) {
            if (expected.clazz == boolean.class) {
                return new Cast(DEF_TYPE, BOOLEAN_OBJ_TYPE, explicit, null, BOOLEAN_TYPE, null, null);
            } else if (expected.clazz == byte.class) {
                return new Cast(DEF_TYPE, BYTE_OBJ_TYPE, explicit, null, BYTE_TYPE, null, null);
            } else if (expected.clazz == short.class) {
                return new Cast(DEF_TYPE, SHORT_OBJ_TYPE, explicit, null, SHORT_TYPE, null, null);
            } else if (expected.clazz == char.class) {
                return new Cast(DEF_TYPE, CHAR_OBJ_TYPE, explicit, null, CHAR_TYPE, null, null);
            } else if (expected.clazz == int.class) {
                return new Cast(DEF_TYPE, INT_OBJ_TYPE, explicit, null, INT_TYPE, null, null);
            } else if (expected.clazz == long.class) {
                return new Cast(DEF_TYPE, LONG_OBJ_TYPE, explicit, null, LONG_TYPE, null, null);
            } else if (expected.clazz == float.class) {
                return new Cast(DEF_TYPE, FLOAT_OBJ_TYPE, explicit, null, FLOAT_TYPE, null, null);
            } else if (expected.clazz == double.class) {
                return new Cast(DEF_TYPE, DOUBLE_OBJ_TYPE, explicit, null, DOUBLE_TYPE, null, null);
            }
        } else if (actual.clazz == Object.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return new Cast(OBJECT_TYPE, BYTE_OBJ_TYPE, true, null, BYTE_TYPE, null, null);
            } else if (expected.clazz == short.class && explicit && internal) {
                return new Cast(OBJECT_TYPE, SHORT_OBJ_TYPE, true, null, SHORT_TYPE, null, null);
            } else if (expected.clazz == char.class && explicit && internal) {
                return new Cast(OBJECT_TYPE, CHAR_OBJ_TYPE, true, null, CHAR_TYPE, null, null);
            } else if (expected.clazz == int.class && explicit && internal) {
                return new Cast(OBJECT_TYPE, INT_OBJ_TYPE, true, null, INT_TYPE, null, null);
            } else if (expected.clazz == long.class && explicit && internal) {
                return new Cast(OBJECT_TYPE, LONG_OBJ_TYPE, true, null, LONG_TYPE, null, null);
            } else if (expected.clazz == float.class && explicit && internal) {
                return new Cast(OBJECT_TYPE, FLOAT_OBJ_TYPE, true, null, FLOAT_TYPE, null, null);
            } else if (expected.clazz == double.class && explicit && internal) {
                return new Cast(OBJECT_TYPE, DOUBLE_OBJ_TYPE, true, null, DOUBLE_TYPE, null, null);
            }
        } else if (actual.clazz == Number.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return new Cast(NUMBER_TYPE, BYTE_OBJ_TYPE, true, null, BYTE_TYPE, null, null);
            } else if (expected.clazz == short.class && explicit && internal) {
                return new Cast(NUMBER_TYPE, SHORT_OBJ_TYPE, true, null, SHORT_TYPE, null, null);
            } else if (expected.clazz == char.class && explicit && internal) {
                return new Cast(NUMBER_TYPE, CHAR_OBJ_TYPE, true, null, CHAR_TYPE, null, null);
            } else if (expected.clazz == int.class && explicit && internal) {
                return new Cast(NUMBER_TYPE, INT_OBJ_TYPE, true, null, INT_TYPE, null, null);
            } else if (expected.clazz == long.class && explicit && internal) {
                return new Cast(NUMBER_TYPE, LONG_OBJ_TYPE, true, null, LONG_TYPE, null, null);
            } else if (expected.clazz == float.class && explicit && internal) {
                return new Cast(NUMBER_TYPE, FLOAT_OBJ_TYPE, true, null, FLOAT_TYPE, null, null);
            } else if (expected.clazz == double.class && explicit && internal) {
                return new Cast(NUMBER_TYPE, DOUBLE_OBJ_TYPE, true, null, DOUBLE_TYPE, null, null);
            }
        } else if (actual.clazz == String.class) {
            if (expected.clazz == char.class && explicit) {
                return new Cast(STRING_TYPE, CHAR_TYPE, true);
            }
        } else if (actual.clazz == boolean.class) {
            if (expected.dynamic) {
                return new Cast(BOOLEAN_OBJ_TYPE, DEF_TYPE, explicit, null, null, BOOLEAN_TYPE, null);
            } else if (expected.clazz == Object.class && internal) {
                return new Cast(BOOLEAN_OBJ_TYPE, OBJECT_TYPE, explicit, null, null, BOOLEAN_TYPE, null);
            } else if (expected.clazz == Boolean.class && internal) {
                return new Cast(BOOLEAN_TYPE, BOOLEAN_TYPE, explicit, null, null, null, BOOLEAN_TYPE);
            }
        } else if (actual.clazz == byte.class) {
            if (expected.dynamic) {
                return new Cast(BYTE_OBJ_TYPE, DEF_TYPE, explicit, null, null, BYTE_TYPE, null);
            } else if (expected.clazz == Object.class && internal) {
                return new Cast(BYTE_OBJ_TYPE, OBJECT_TYPE, explicit, null, null, BYTE_TYPE, null);
            } else if (expected.clazz == Number.class && internal) {
                return new Cast(BYTE_OBJ_TYPE, NUMBER_TYPE, explicit, null, null, BYTE_TYPE, null);
            } else if (expected.clazz == short.class) {
                return new Cast(BYTE_TYPE, SHORT_TYPE, explicit);
            } else if (expected.clazz == char.class && explicit) {
                return new Cast(BYTE_TYPE, CHAR_TYPE, true);
            } else if (expected.clazz == int.class) {
                return new Cast(BYTE_TYPE, INT_TYPE, explicit);
            } else if (expected.clazz == long.class) {
                return new Cast(BYTE_TYPE, LONG_TYPE, explicit);
            } else if (expected.clazz == float.class) {
                return new Cast(BYTE_TYPE, FLOAT_TYPE, explicit);
            } else if (expected.clazz == double.class) {
                return new Cast(BYTE_TYPE, DOUBLE_TYPE, explicit);
            } else if (expected.clazz == Byte.class && internal) {
                return new Cast(BYTE_TYPE, BYTE_TYPE, explicit, null, null, null, BYTE_TYPE);
            } else if (expected.clazz == Short.class && internal) {
                return new Cast(BYTE_TYPE, SHORT_TYPE, explicit, null, null, null, SHORT_TYPE);
            } else if (expected.clazz == Character.class && explicit && internal) {
                return new Cast(BYTE_TYPE, CHAR_TYPE, true, null, null, null, CHAR_TYPE);
            } else if (expected.clazz == Integer.class && internal) {
                return new Cast(BYTE_TYPE, INT_TYPE, explicit, null, null, null, INT_TYPE);
            } else if (expected.clazz == Long.class && internal) {
                return new Cast(BYTE_TYPE, LONG_TYPE, explicit, null, null, null, LONG_TYPE);
            } else if (expected.clazz == Float.class && internal) {
                return new Cast(BYTE_TYPE, FLOAT_TYPE, explicit, null, null, null, FLOAT_TYPE);
            } else if (expected.clazz == Double.class && internal) {
                return new Cast(BYTE_TYPE, DOUBLE_TYPE, explicit, null, null, null, DOUBLE_TYPE);
            }
        } else if (actual.clazz == short.class) {
            if (expected.dynamic) {
                return new Cast(SHORT_OBJ_TYPE, DEF_TYPE, explicit, null, null, SHORT_TYPE, null);
            } else if (expected.clazz == Object.class && internal) {
                return new Cast(SHORT_OBJ_TYPE, OBJECT_TYPE, explicit, null, null, SHORT_TYPE, null);
            } else if (expected.clazz == Number.class && internal) {
                return new Cast(SHORT_OBJ_TYPE, NUMBER_TYPE, explicit, null, null, SHORT_TYPE, null);
            } else if (expected.clazz == byte.class && explicit) {
                return new Cast(SHORT_TYPE, BYTE_TYPE, true);
            } else if (expected.clazz == char.class && explicit) {
                return new Cast(SHORT_TYPE, CHAR_TYPE, true);
            } else if (expected.clazz == int.class) {
                return new Cast(SHORT_TYPE, INT_TYPE, explicit);
            } else if (expected.clazz == long.class) {
                return new Cast(SHORT_TYPE, LONG_TYPE, explicit);
            } else if (expected.clazz == float.class) {
                return new Cast(SHORT_TYPE, FLOAT_TYPE, explicit);
            } else if (expected.clazz == double.class) {
                return new Cast(SHORT_TYPE, DOUBLE_TYPE, explicit);
            } else if (expected.clazz == Byte.class && explicit && internal) {
                return new Cast(SHORT_TYPE, BYTE_TYPE, true, null, null, null, BYTE_TYPE);
            } else if (expected.clazz == Short.class && internal) {
                return new Cast(SHORT_TYPE, SHORT_TYPE, explicit, null, null, null, SHORT_TYPE);
            } else if (expected.clazz == Character.class && explicit && internal) {
                return new Cast(SHORT_TYPE, CHAR_TYPE, true, null, null, null, CHAR_TYPE);
            } else if (expected.clazz == Integer.class && internal) {
                return new Cast(SHORT_TYPE, INT_TYPE, explicit, null, null, null, INT_TYPE);
            } else if (expected.clazz == Long.class && internal) {
                return new Cast(SHORT_TYPE, LONG_TYPE, explicit, null, null, null, LONG_TYPE);
            } else if (expected.clazz == Float.class && internal) {
                return new Cast(SHORT_TYPE, FLOAT_TYPE, explicit, null, null, null, FLOAT_TYPE);
            } else if (expected.clazz == Double.class && internal) {
                return new Cast(SHORT_TYPE, DOUBLE_TYPE, explicit, null, null, null, DOUBLE_TYPE);
            }
        } else if (actual.clazz == char.class) {
            if (expected.dynamic) {
                return new Cast(CHAR_OBJ_TYPE, DEF_TYPE, explicit, null, null, CHAR_TYPE, null);
            } else if (expected.clazz == Object.class && internal) {
                return new Cast(CHAR_OBJ_TYPE, OBJECT_TYPE, explicit, null, null, CHAR_TYPE, null);
            } else if (expected.clazz == Number.class && internal) {
                return new Cast(CHAR_OBJ_TYPE, NUMBER_TYPE, explicit, null, null, CHAR_TYPE, null);
            } else if (expected.clazz == String.class) {
                return new Cast(CHAR_TYPE, STRING_TYPE, explicit);
            } else if (expected.clazz == byte.class && explicit) {
                return new Cast(CHAR_TYPE, BYTE_TYPE, true);
            } else if (expected.clazz == short.class && explicit) {
                return new Cast(CHAR_TYPE, SHORT_TYPE, true);
            } else if (expected.clazz == int.class) {
                return new Cast(CHAR_TYPE, INT_TYPE, explicit);
            } else if (expected.clazz == long.class) {
                return new Cast(CHAR_TYPE, LONG_TYPE, explicit);
            } else if (expected.clazz == float.class) {
                return new Cast(CHAR_TYPE, FLOAT_TYPE, explicit);
            } else if (expected.clazz == double.class) {
                return new Cast(CHAR_TYPE, DOUBLE_TYPE, explicit);
            } else if (expected.clazz == Byte.class && explicit && internal) {
                return new Cast(CHAR_TYPE, BYTE_TYPE, true, null, null, null, BYTE_TYPE);
            } else if (expected.clazz == Short.class && internal) {
                return new Cast(CHAR_TYPE, SHORT_TYPE, explicit, null, null, null, SHORT_TYPE);
            } else if (expected.clazz == Character.class && internal) {
                return new Cast(CHAR_TYPE, CHAR_TYPE, true, null, null, null, CHAR_TYPE);
            } else if (expected.clazz == Integer.class && internal) {
                return new Cast(CHAR_TYPE, INT_TYPE, explicit, null, null, null, INT_TYPE);
            } else if (expected.clazz == Long.class && internal) {
                return new Cast(CHAR_TYPE, LONG_TYPE, explicit, null, null, null, LONG_TYPE);
            } else if (expected.clazz == Float.class && internal) {
                return new Cast(CHAR_TYPE, FLOAT_TYPE, explicit, null, null, null, FLOAT_TYPE);
            } else if (expected.clazz == Double.class && internal) {
                return new Cast(CHAR_TYPE, DOUBLE_TYPE, explicit, null, null, null, DOUBLE_TYPE);
            }
        } else if (actual.clazz == int.class) {
            if (expected.dynamic) {
                return new Cast(INT_OBJ_TYPE, DEF_TYPE, explicit, null, null, INT_TYPE, null);
            } else if (expected.clazz == Object.class && internal) {
                return new Cast(INT_OBJ_TYPE, OBJECT_TYPE, explicit, null, null, INT_TYPE, null);
            } else if (expected.clazz == Number.class && internal) {
                return new Cast(INT_OBJ_TYPE, NUMBER_TYPE, explicit, null, null, INT_TYPE, null);
            } else if (expected.clazz == byte.class && explicit) {
                return new Cast(INT_TYPE, BYTE_TYPE, true);
            } else if (expected.clazz == char.class && explicit) {
                return new Cast(INT_TYPE, CHAR_TYPE, true);
            } else if (expected.clazz == short.class && explicit) {
                return new Cast(INT_TYPE, SHORT_TYPE, true);
            } else if (expected.clazz == long.class) {
                return new Cast(INT_TYPE, LONG_TYPE, explicit);
            } else if (expected.clazz == float.class) {
                return new Cast(INT_TYPE, FLOAT_TYPE, explicit);
            } else if (expected.clazz == double.class) {
                return new Cast(INT_TYPE, DOUBLE_TYPE, explicit);
            } else if (expected.clazz == Byte.class && explicit && internal) {
                return new Cast(INT_TYPE, BYTE_TYPE, true, null, null, null, BYTE_TYPE);
            } else if (expected.clazz == Short.class && explicit && internal) {
                return new Cast(INT_TYPE, SHORT_TYPE, true, null, null, null, SHORT_TYPE);
            } else if (expected.clazz == Character.class && explicit && internal) {
                return new Cast(INT_TYPE, CHAR_TYPE, true, null, null, null, CHAR_TYPE);
            } else if (expected.clazz == Integer.class && internal) {
                return new Cast(INT_TYPE, INT_TYPE, explicit, null, null, null, INT_TYPE);
            } else if (expected.clazz == Long.class && internal) {
                return new Cast(INT_TYPE, LONG_TYPE, explicit, null, null, null, LONG_TYPE);
            } else if (expected.clazz == Float.class && internal) {
                return new Cast(INT_TYPE, FLOAT_TYPE, explicit, null, null, null, FLOAT_TYPE);
            } else if (expected.clazz == Double.class && internal) {
                return new Cast(INT_TYPE, DOUBLE_TYPE, explicit, null, null, null, DOUBLE_TYPE);
            }
        } else if (actual.clazz == long.class) {
            if (expected.dynamic) {
                return new Cast(LONG_OBJ_TYPE, DEF_TYPE, explicit, null, null, LONG_TYPE, null);
            } else if (expected.clazz == Object.class && internal) {
                return new Cast(LONG_OBJ_TYPE, OBJECT_TYPE, explicit, null, null, LONG_TYPE, null);
            } else if (expected.clazz == Number.class && internal) {
                return new Cast(LONG_OBJ_TYPE, NUMBER_TYPE, explicit, null, null, LONG_TYPE, null);
            } else if (expected.clazz == byte.class && explicit) {
                return new Cast(LONG_TYPE, BYTE_TYPE, true);
            } else if (expected.clazz == char.class && explicit) {
                return new Cast(LONG_TYPE, CHAR_TYPE, true);
            } else if (expected.clazz == short.class && explicit) {
                return new Cast(LONG_TYPE, SHORT_TYPE, true);
            } else if (expected.clazz == int.class && explicit) {
                return new Cast(LONG_TYPE, INT_TYPE, true);
            } else if (expected.clazz == float.class) {
                return new Cast(LONG_TYPE, FLOAT_TYPE, explicit);
            } else if (expected.clazz == double.class) {
                return new Cast(LONG_TYPE, DOUBLE_TYPE, explicit);
            } else if (expected.clazz == Byte.class && explicit && internal) {
                return new Cast(LONG_TYPE, BYTE_TYPE, true, null, null, null, BYTE_TYPE);
            } else if (expected.clazz == Short.class && explicit && internal) {
                return new Cast(LONG_TYPE, SHORT_TYPE, true, null, null, null, SHORT_TYPE);
            } else if (expected.clazz == Character.class && explicit && internal) {
                return new Cast(LONG_TYPE, CHAR_TYPE, true, null, null, null, CHAR_TYPE);
            } else if (expected.clazz == Integer.class && explicit && internal) {
                return new Cast(LONG_TYPE, INT_TYPE, true, null, null, null, INT_TYPE);
            } else if (expected.clazz == Long.class && internal) {
                return new Cast(LONG_TYPE, LONG_TYPE, explicit, null, null, null, LONG_TYPE);
            } else if (expected.clazz == Float.class && internal) {
                return new Cast(LONG_TYPE, FLOAT_TYPE, explicit, null, null, null, FLOAT_TYPE);
            } else if (expected.clazz == Double.class && internal) {
                return new Cast(LONG_TYPE, DOUBLE_TYPE, explicit, null, null, null, DOUBLE_TYPE);
            }
        } else if (actual.clazz == float.class) {
            if (expected.dynamic) {
                return new Cast(FLOAT_OBJ_TYPE, DEF_TYPE, explicit, null, null, FLOAT_TYPE, null);
            } else if (expected.clazz == Object.class && internal) {
                return new Cast(FLOAT_OBJ_TYPE, OBJECT_TYPE, explicit, null, null, FLOAT_TYPE, null);
            } else if (expected.clazz == Number.class && internal) {
                return new Cast(FLOAT_OBJ_TYPE, NUMBER_TYPE, explicit, null, null, FLOAT_TYPE, null);
            } else if (expected.clazz == byte.class && explicit) {
                return new Cast(FLOAT_TYPE, BYTE_TYPE, true);
            } else if (expected.clazz == char.class && explicit) {
                return new Cast(FLOAT_TYPE, CHAR_TYPE, true);
            } else if (expected.clazz == short.class && explicit) {
                return new Cast(FLOAT_TYPE, SHORT_TYPE, true);
            } else if (expected.clazz == int.class && explicit) {
                return new Cast(FLOAT_TYPE, INT_TYPE, true);
            } else if (expected.clazz == long.class && explicit) {
                return new Cast(FLOAT_TYPE, LONG_TYPE, true);
            } else if (expected.clazz == double.class) {
                return new Cast(FLOAT_TYPE, DOUBLE_TYPE, explicit);
            } else if (expected.clazz == Byte.class && explicit && internal) {
                return new Cast(FLOAT_TYPE, BYTE_TYPE, true, null, null, null, BYTE_TYPE);
            } else if (expected.clazz == Short.class && explicit && internal) {
                return new Cast(FLOAT_TYPE, SHORT_TYPE, true, null, null, null, SHORT_TYPE);
            } else if (expected.clazz == Character.class && explicit && internal) {
                return new Cast(FLOAT_TYPE, CHAR_TYPE, true, null, null, null, CHAR_TYPE);
            } else if (expected.clazz == Integer.class && explicit && internal) {
                return new Cast(FLOAT_TYPE, INT_TYPE, true, null, null, null, INT_TYPE);
            } else if (expected.clazz == Long.class && explicit && internal) {
                return new Cast(FLOAT_TYPE, LONG_TYPE, true, null, null, null, LONG_TYPE);
            } else if (expected.clazz == Float.class && internal) {
                return new Cast(FLOAT_TYPE, FLOAT_TYPE, explicit, null, null, null, FLOAT_TYPE);
            } else if (expected.clazz == Double.class && internal) {
                return new Cast(FLOAT_TYPE, DOUBLE_TYPE, explicit, null, null, null, DOUBLE_TYPE);
            }
        } else if (actual.clazz == double.class) {
            if (expected.dynamic) {
                return new Cast(DOUBLE_OBJ_TYPE, DEF_TYPE, explicit, null, null, DOUBLE_TYPE, null);
            } else if (expected.clazz == Object.class && internal) {
                return new Cast(DOUBLE_OBJ_TYPE, OBJECT_TYPE, explicit, null, null, DOUBLE_TYPE, null);
            } else if (expected.clazz == Number.class && internal) {
                return new Cast(DOUBLE_OBJ_TYPE, NUMBER_TYPE, explicit, null, null, DOUBLE_TYPE, null);
            } else if (expected.clazz == byte.class && explicit) {
                return new Cast(DOUBLE_TYPE, BYTE_TYPE, true);
            } else if (expected.clazz == char.class && explicit) {
                return new Cast(DOUBLE_TYPE, CHAR_TYPE, true);
            } else if (expected.clazz == short.class && explicit) {
                return new Cast(DOUBLE_TYPE, SHORT_TYPE, true);
            } else if (expected.clazz == int.class && explicit) {
                return new Cast(DOUBLE_TYPE, INT_TYPE, true);
            } else if (expected.clazz == long.class && explicit) {
                return new Cast(DOUBLE_TYPE, LONG_TYPE, true);
            } else if (expected.clazz == float.class && explicit) {
                return new Cast(DOUBLE_TYPE, FLOAT_TYPE, true);
            } else if (expected.clazz == Byte.class && explicit && internal) {
                return new Cast(DOUBLE_TYPE, BYTE_TYPE, true, null, null, null, BYTE_TYPE);
            } else if (expected.clazz == Short.class && explicit && internal) {
                return new Cast(DOUBLE_TYPE, SHORT_TYPE, true, null, null, null, SHORT_TYPE);
            } else if (expected.clazz == Character.class && explicit && internal) {
                return new Cast(DOUBLE_TYPE, CHAR_TYPE, true, null, null, null, CHAR_TYPE);
            } else if (expected.clazz == Integer.class && explicit && internal) {
                return new Cast(DOUBLE_TYPE, INT_TYPE, true, null, null, null, INT_TYPE);
            } else if (expected.clazz == Long.class && explicit && internal) {
                return new Cast(DOUBLE_TYPE, LONG_TYPE, true, null, null, null, LONG_TYPE);
            } else if (expected.clazz == Float.class && explicit && internal) {
                return new Cast(DOUBLE_TYPE, FLOAT_TYPE, true, null, null, null, FLOAT_TYPE);
            } else if (expected.clazz == Double.class && internal) {
                return new Cast(DOUBLE_TYPE, DOUBLE_TYPE, explicit, null, null, null, DOUBLE_TYPE);
            }
        } else if (actual.clazz == Boolean.class) {
            if (expected.clazz == boolean.class && internal) {
                return new Cast(BOOLEAN_TYPE, BOOLEAN_TYPE, explicit, BOOLEAN_TYPE, null, null, null);
            }
        } else if (actual.clazz == Byte.class) {
            if (expected.clazz == byte.class && internal) {
                return new Cast(BYTE_TYPE, BYTE_TYPE, explicit, BYTE_TYPE, null, null, null);
            } else if (expected.clazz == short.class && internal) {
                return new Cast(BYTE_TYPE, SHORT_TYPE, explicit, BYTE_TYPE, null, null, null);
            } else if (expected.clazz == char.class && explicit && internal) {
                return new Cast(BYTE_TYPE, CHAR_TYPE, true, BYTE_TYPE, null, null, null);
            } else if (expected.clazz == int.class && internal) {
                return new Cast(BYTE_TYPE, INT_TYPE, explicit, BYTE_TYPE, null, null, null);
            } else if (expected.clazz == long.class && internal) {
                return new Cast(BYTE_TYPE, LONG_TYPE, explicit, BYTE_TYPE, null, null, null);
            } else if (expected.clazz == float.class && internal) {
                return new Cast(BYTE_TYPE, FLOAT_TYPE, explicit, BYTE_TYPE, null, null, null);
            } else if (expected.clazz == double.class && internal) {
                return new Cast(BYTE_TYPE, DOUBLE_TYPE, explicit, BYTE_TYPE, null, null, null);
            }
        } else if (actual.clazz == Short.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return new Cast(SHORT_TYPE, BYTE_TYPE, true, SHORT_TYPE, null, null, null);
            } else if (expected.clazz == short.class && internal) {
                return new Cast(SHORT_TYPE, SHORT_TYPE, explicit, SHORT_TYPE, null, null, null);
            } else if (expected.clazz == char.class && explicit && internal) {
                return new Cast(SHORT_TYPE, CHAR_TYPE, true, SHORT_TYPE, null, null, null);
            } else if (expected.clazz == int.class && internal) {
                return new Cast(SHORT_TYPE, INT_TYPE, explicit, SHORT_TYPE, null, null, null);
            } else if (expected.clazz == long.class && internal) {
                return new Cast(SHORT_TYPE, LONG_TYPE, explicit, SHORT_TYPE, null, null, null);
            } else if (expected.clazz == float.class && internal) {
                return new Cast(SHORT_TYPE, FLOAT_TYPE, explicit, SHORT_TYPE, null, null, null);
            } else if (expected.clazz == double.class && internal) {
                return new Cast(SHORT_TYPE, DOUBLE_TYPE, explicit, SHORT_TYPE, null, null, null);
            }
        } else if (actual.clazz == Character.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return new Cast(CHAR_TYPE, BYTE_TYPE, true, CHAR_TYPE, null, null, null);
            } else if (expected.clazz == short.class && explicit && internal) {
                return new Cast(CHAR_TYPE, SHORT_TYPE, true, CHAR_TYPE, null, null, null);
            } else if (expected.clazz == char.class && internal) {
                return new Cast(CHAR_TYPE, CHAR_TYPE, explicit, CHAR_TYPE, null, null, null);
            } else if (expected.clazz == int.class && internal) {
                return new Cast(CHAR_TYPE, INT_TYPE, explicit, CHAR_TYPE, null, null, null);
            } else if (expected.clazz == long.class && internal) {
                return new Cast(CHAR_TYPE, LONG_TYPE, explicit, CHAR_TYPE, null, null, null);
            } else if (expected.clazz == float.class && internal) {
                return new Cast(CHAR_TYPE, FLOAT_TYPE, explicit, CHAR_TYPE, null, null, null);
            } else if (expected.clazz == double.class && internal) {
                return new Cast(CHAR_TYPE, DOUBLE_TYPE, explicit, CHAR_TYPE, null, null, null);
            }
        } else if (actual.clazz == Integer.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return new Cast(INT_TYPE, BYTE_TYPE, true, INT_TYPE, null, null, null);
            } else if (expected.clazz == short.class && explicit && internal) {
                return new Cast(INT_TYPE, SHORT_TYPE, true, INT_TYPE, null, null, null);
            } else if (expected.clazz == char.class && explicit && internal) {
                return new Cast(INT_TYPE, CHAR_TYPE, true, INT_TYPE, null, null, null);
            } else if (expected.clazz == int.class && internal) {
                return new Cast(INT_TYPE, INT_TYPE, explicit, INT_TYPE, null, null, null);
            } else if (expected.clazz == long.class && internal) {
                return new Cast(INT_TYPE, LONG_TYPE, explicit, INT_TYPE, null, null, null);
            } else if (expected.clazz == float.class && internal) {
                return new Cast(INT_TYPE, FLOAT_TYPE, explicit, INT_TYPE, null, null, null);
            } else if (expected.clazz == double.class && internal) {
                return new Cast(INT_TYPE, DOUBLE_TYPE, explicit, INT_TYPE, null, null, null);
            }
        } else if (actual.clazz == Long.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return new Cast(LONG_TYPE, BYTE_TYPE, true, LONG_TYPE, null, null, null);
            } else if (expected.clazz == short.class && explicit && internal) {
                return new Cast(LONG_TYPE, SHORT_TYPE, true, LONG_TYPE, null, null, null);
            } else if (expected.clazz == char.class && explicit && internal) {
                return new Cast(LONG_TYPE, CHAR_TYPE, true, LONG_TYPE, null, null, null);
            } else if (expected.clazz == int.class && explicit && internal) {
                return new Cast(LONG_TYPE, INT_TYPE, true, LONG_TYPE, null, null, null);
            } else if (expected.clazz == long.class && internal) {
                return new Cast(LONG_TYPE, LONG_TYPE, explicit, LONG_TYPE, null, null, null);
            } else if (expected.clazz == float.class && internal) {
                return new Cast(LONG_TYPE, FLOAT_TYPE, explicit, LONG_TYPE, null, null, null);
            } else if (expected.clazz == double.class && internal) {
                return new Cast(LONG_TYPE, DOUBLE_TYPE, explicit, LONG_TYPE, null, null, null);
            }
        } else if (actual.clazz == Float.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return new Cast(FLOAT_TYPE, BYTE_TYPE, true, FLOAT_TYPE, null, null, null);
            } else if (expected.clazz == short.class && explicit && internal) {
                return new Cast(FLOAT_TYPE, SHORT_TYPE, true, FLOAT_TYPE, null, null, null);
            } else if (expected.clazz == char.class && explicit && internal) {
                return new Cast(FLOAT_TYPE, CHAR_TYPE, true, FLOAT_TYPE, null, null, null);
            } else if (expected.clazz == int.class && explicit && internal) {
                return new Cast(FLOAT_TYPE, INT_TYPE, true, FLOAT_TYPE, null, null, null);
            } else if (expected.clazz == long.class && explicit && internal) {
                return new Cast(FLOAT_TYPE, LONG_TYPE, true, FLOAT_TYPE, null, null, null);
            } else if (expected.clazz == float.class && internal) {
                return new Cast(FLOAT_TYPE, FLOAT_TYPE, explicit, FLOAT_TYPE, null, null, null);
            } else if (expected.clazz == double.class && internal) {
                return new Cast(FLOAT_TYPE, DOUBLE_TYPE, explicit, FLOAT_TYPE, null, null, null);
            }
        } else if (actual.clazz == Double.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return new Cast(DOUBLE_TYPE, BYTE_TYPE, true, DOUBLE_TYPE, null, null, null);
            } else if (expected.clazz == short.class && explicit && internal) {
                return new Cast(DOUBLE_TYPE, SHORT_TYPE, true, DOUBLE_TYPE, null, null, null);
            } else if (expected.clazz == char.class && explicit && internal) {
                return new Cast(DOUBLE_TYPE, CHAR_TYPE, true, DOUBLE_TYPE, null, null, null);
            } else if (expected.clazz == int.class && explicit && internal) {
                return new Cast(DOUBLE_TYPE, INT_TYPE, true, DOUBLE_TYPE, null, null, null);
            } else if (expected.clazz == long.class && explicit && internal) {
                return new Cast(DOUBLE_TYPE, LONG_TYPE, true, DOUBLE_TYPE, null, null, null);
            } else if (expected.clazz == float.class && explicit && internal) {
                return new Cast(DOUBLE_TYPE, FLOAT_TYPE, true, DOUBLE_TYPE, null, null, null);
            } else if (expected.clazz == double.class && internal) {
                return new Cast(DOUBLE_TYPE, DOUBLE_TYPE, explicit, DOUBLE_TYPE, null, null, null);
            }
        }

        if (    actual.dynamic                                   ||
                (actual.clazz != void.class && expected.dynamic) ||
                expected.clazz.isAssignableFrom(actual.clazz)    ||
                (actual.clazz.isAssignableFrom(expected.clazz) && explicit)) {
            return new Cast(actual, expected, explicit);
        } else {
            throw location.createError(new ClassCastException("Cannot cast from [" + actual.name + "] to [" + expected.name + "]."));
        }
    }

    public static Object constCast(Location location, final Object constant, final Cast cast) {
        Class<?> fsort = cast.from.clazz;
        Class<?> tsort = cast.to.clazz;

        if (fsort == tsort) {
            return constant;
        } else if (fsort == String.class && tsort == char.class) {
            return Utility.StringTochar((String)constant);
        } else if (fsort == char.class && tsort == String.class) {
            return Utility.charToString((char)constant);
        } else if (fsort.isPrimitive() && fsort != boolean.class && tsort.isPrimitive() && tsort != boolean.class) {
            final Number number;

            if (fsort == char.class) {
                number = (int)(char)constant;
            } else {
                number = (Number)constant;
            }

            if      (tsort == byte.class) return number.byteValue();
            else if (tsort == short.class) return number.shortValue();
            else if (tsort == char.class) return (char)number.intValue();
            else if (tsort == int.class) return number.intValue();
            else if (tsort == long.class) return number.longValue();
            else if (tsort == float.class) return number.floatValue();
            else if (tsort == double.class) return number.doubleValue();
            else {
                throw location.createError(new IllegalStateException("Cannot cast from " +
                    "[" + cast.from.clazz.getCanonicalName() + "] to [" + cast.to.clazz.getCanonicalName() + "]."));
            }
        } else {
            throw location.createError(new IllegalStateException("Cannot cast from " +
                "[" + cast.from.clazz.getCanonicalName() + "] to [" + cast.to.clazz.getCanonicalName() + "]."));
        }
    }

    public static Type promoteNumeric(Type from, boolean decimal) {
        Class<?> sort = from.clazz;

        if (from.dynamic) {
            return DEF_TYPE;
        } else if ((sort == double.class) && decimal) {
            return DOUBLE_TYPE;
        } else if ((sort == float.class) && decimal) {
            return  FLOAT_TYPE;
        } else if (sort == long.class) {
            return LONG_TYPE;
        } else if (sort == int.class || sort == char.class || sort == short.class || sort == byte.class) {
            return INT_TYPE;
        }

        return null;
    }

    public static Type promoteNumeric(Type from0, Type from1, boolean decimal) {
        Class<?> sort0 = from0.clazz;
        Class<?> sort1 = from1.clazz;

        if (from0.dynamic || from1.dynamic) {
            return DEF_TYPE;
        }

        if (decimal) {
            if (sort0 == double.class || sort1 == double.class) {
                return DOUBLE_TYPE;
            } else if (sort0 == float.class || sort1 == float.class) {
                return FLOAT_TYPE;
            }
        }

        if (sort0 == long.class || sort1 == long.class) {
            return LONG_TYPE;
        } else if (sort0 == int.class   || sort1 == int.class   ||
                   sort0 == char.class  || sort1 == char.class  ||
                   sort0 == short.class || sort1 == short.class ||
                   sort0 == byte.class  || sort1 == byte.class) {
            return INT_TYPE;
        }

        return null;
    }

    public static Type promoteAdd(Type from0, Type from1) {
        Class<?> sort0 = from0.clazz;
        Class<?> sort1 = from1.clazz;

        if (sort0 == String.class || sort1 == String.class) {
            return STRING_TYPE;
        }

        return promoteNumeric(from0, from1, true);
    }

    public static Type promoteXor(Type from0, Type from1) {
        Class<?> sort0 = from0.clazz;
        Class<?> sort1 = from1.clazz;

        if (from0.dynamic || from1.dynamic) {
            return DEF_TYPE;
        }

        if (sort0 == boolean.class || sort1 == boolean.class) {
            return BOOLEAN_TYPE;
        }

        return promoteNumeric(from0, from1, false);
    }

    public static Type promoteEquality(Type from0, Type from1) {
        Class<?> sort0 = from0.clazz;
        Class<?> sort1 = from1.clazz;

        if (from0.dynamic || from1.dynamic) {
            return DEF_TYPE;
        }

        if (sort0.isPrimitive() && sort1.isPrimitive()) {
            if (sort0 == boolean.class && sort1 == boolean.class) {
                return BOOLEAN_TYPE;
            }

            return promoteNumeric(from0, from1, true);
        }

        return OBJECT_TYPE;
    }

    public static Type promoteConditional(Type from0, Type from1, Object const0, Object const1) {
        if (from0.equals(from1)) {
            return from0;
        }

        Class<?> sort0 = from0.clazz;
        Class<?> sort1 = from1.clazz;

        if (from0.dynamic || from1.dynamic) {
            return DEF_TYPE;
        }

        if (sort0.isPrimitive() && sort1.isPrimitive()) {
            if (sort0 == boolean.class && sort1 == boolean.class) {
                return BOOLEAN_TYPE;
            }

            if (sort0 == double.class || sort1 == double.class) {
                return DOUBLE_TYPE;
            } else if (sort0 == float.class || sort1 == float.class) {
                return FLOAT_TYPE;
            } else if (sort0 == long.class || sort1 == long.class) {
                return LONG_TYPE;
            } else {
                if (sort0 == byte.class) {
                    if (sort1 == byte.class) {
                        return BYTE_TYPE;
                    } else if (sort1 == short.class) {
                        if (const1 != null) {
                            final short constant = (short)const1;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return BYTE_TYPE;
                            }
                        }

                        return SHORT_TYPE;
                    } else if (sort1 == char.class) {
                        return INT_TYPE;
                    } else if (sort1 == int.class) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return BYTE_TYPE;
                            }
                        }

                        return INT_TYPE;
                    }
                } else if (sort0 == short.class) {
                    if (sort1 == byte.class) {
                        if (const0 != null) {
                            final short constant = (short)const0;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return BYTE_TYPE;
                            }
                        }

                        return SHORT_TYPE;
                    } else if (sort1 == short.class) {
                        return SHORT_TYPE;
                    } else if (sort1 == char.class) {
                        return INT_TYPE;
                    } else if (sort1 == int.class) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Short.MAX_VALUE && constant >= Short.MIN_VALUE) {
                                return SHORT_TYPE;
                            }
                        }

                        return INT_TYPE;
                    }
                } else if (sort0 == char.class) {
                    if (sort1 == byte.class) {
                        return INT_TYPE;
                    } else if (sort1 == short.class) {
                        return INT_TYPE;
                    } else if (sort1 == char.class) {
                        return CHAR_TYPE;
                    } else if (sort1 == int.class) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Character.MAX_VALUE && constant >= Character.MIN_VALUE) {
                                return BYTE_TYPE;
                            }
                        }

                        return INT_TYPE;
                    }
                } else if (sort0 == int.class) {
                    if (sort1 == byte.class) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return BYTE_TYPE;
                            }
                        }

                        return INT_TYPE;
                    } else if (sort1 == short.class) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Short.MAX_VALUE && constant >= Short.MIN_VALUE) {
                                return BYTE_TYPE;
                            }
                        }

                        return INT_TYPE;
                    } else if (sort1 == char.class) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Character.MAX_VALUE && constant >= Character.MIN_VALUE) {
                                return BYTE_TYPE;
                            }
                        }

                        return INT_TYPE;
                    } else if (sort1 == int.class) {
                        return INT_TYPE;
                    }
                }
            }
        }

        // TODO: In the rare case we still haven't reached a correct promotion we need
        // TODO: to calculate the highest upper bound for the two types and return that.
        // TODO: However, for now we just return objectType that may require an extra cast.

        return OBJECT_TYPE;
    }

    private AnalyzerCaster() {}
}
