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

/**
 * Used during the analysis phase to collect legal type casts and promotions
 * for type-checking and later to write necessary casts in the bytecode.
 */
public final class AnalyzerCaster {

    private Definition definition;

    public AnalyzerCaster(Definition definition) {
        this.definition = definition;
    }

    public Cast getLegalCast(Location location, Type actual, Type expected, boolean explicit, boolean internal) {
        Objects.requireNonNull(actual);
        Objects.requireNonNull(expected);

        if (actual.equals(expected)) {
            return null;
        }

        if (actual.dynamic) {
            if (expected.clazz == boolean.class) {
                return new Cast(definition.DefType, definition.BooleanType, explicit, null, definition.booleanType, null, null);
            } else if (expected.clazz == byte.class) {
                return new Cast(definition.DefType, definition.ByteType, explicit, null, definition.byteType, null, null);
            } else if (expected.clazz == short.class) {
                return new Cast(definition.DefType, definition.ShortType, explicit, null, definition.shortType, null, null);
            } else if (expected.clazz == char.class) {
                return new Cast(definition.DefType, definition.CharacterType, explicit, null, definition.charType, null, null);
            } else if (expected.clazz == int.class) {
                return new Cast(definition.DefType, definition.IntegerType, explicit, null, definition.intType, null, null);
            } else if (expected.clazz == long.class) {
                return new Cast(definition.DefType, definition.LongType, explicit, null, definition.longType, null, null);
            } else if (expected.clazz == float.class) {
                return new Cast(definition.DefType, definition.FloatType, explicit, null, definition.floatType, null, null);
            } else if (expected.clazz == double.class) {
                return new Cast(definition.DefType, definition.DoubleType, explicit, null, definition.doubleType, null, null);
            }
        } else if (actual.clazz == Object.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return new Cast(definition.ObjectType, definition.ByteType, true, null, definition.byteType, null, null);
            } else if (expected.clazz == short.class && explicit && internal) {
                return new Cast(definition.ObjectType, definition.ShortType, true, null, definition.shortType, null, null);
            } else if (expected.clazz == char.class && explicit && internal) {
                return new Cast(definition.ObjectType, definition.CharacterType, true, null, definition.charType, null, null);
            } else if (expected.clazz == int.class && explicit && internal) {
                return new Cast(definition.ObjectType, definition.IntegerType, true, null, definition.intType, null, null);
            } else if (expected.clazz == long.class && explicit && internal) {
                return new Cast(definition.ObjectType, definition.LongType, true, null, definition.longType, null, null);
            } else if (expected.clazz == float.class && explicit && internal) {
                return new Cast(definition.ObjectType, definition.FloatType, true, null, definition.floatType, null, null);
            } else if (expected.clazz == double.class && explicit && internal) {
                return new Cast(definition.ObjectType, definition.DoubleType, true, null, definition.doubleType, null, null);
            }
        } else if (actual.clazz == Number.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return new Cast(definition.NumberType, definition.ByteType, true, null, definition.byteType, null, null);
            } else if (expected.clazz == short.class && explicit && internal) {
                return new Cast(definition.NumberType, definition.ShortType, true, null, definition.shortType, null, null);
            } else if (expected.clazz == char.class && explicit && internal) {
                return new Cast(definition.NumberType, definition.CharacterType, true, null, definition.charType, null, null);
            } else if (expected.clazz == int.class && explicit && internal) {
                return new Cast(definition.NumberType, definition.IntegerType, true, null, definition.intType, null, null);
            } else if (expected.clazz == long.class && explicit && internal) {
                return new Cast(definition.NumberType, definition.LongType, true, null, definition.longType, null, null);
            } else if (expected.clazz == float.class && explicit && internal) {
                return new Cast(definition.NumberType, definition.FloatType, true, null, definition.floatType, null, null);
            } else if (expected.clazz == double.class && explicit && internal) {
                return new Cast(definition.NumberType, definition.DoubleType, true, null, definition.doubleType, null, null);
            }
        } else if (actual.clazz == String.class) {
            if (expected.clazz == char.class && explicit) {
                return new Cast(definition.StringType, definition.charType, true);
            }
        } else if (actual.clazz == boolean.class) {
            if (expected.dynamic) {
                return new Cast(definition.BooleanType, definition.DefType, explicit, null, null, definition.booleanType, null);
            } else if (expected.clazz == Object.class && internal) {
                return new Cast(definition.BooleanType, definition.ObjectType, explicit, null, null, definition.booleanType, null);
            } else if (expected.clazz == Boolean.class && internal) {
                return new Cast(definition.booleanType, definition.booleanType, explicit, null, null, null, definition.booleanType);
            }
        } else if (actual.clazz == byte.class) {
            if (expected.dynamic) {
                return new Cast(definition.ByteType, definition.DefType, explicit, null, null, definition.byteType, null);
            } else if (expected.clazz == Object.class && internal) {
                return new Cast(definition.ByteType, definition.ObjectType, explicit, null, null, definition.byteType, null);
            } else if (expected.clazz == Number.class && internal) {
                return new Cast(definition.ByteType, definition.NumberType, explicit, null, null, definition.byteType, null);
            } else if (expected.clazz == short.class) {
                return new Cast(definition.byteType, definition.shortType, explicit);
            } else if (expected.clazz == char.class && explicit) {
                return new Cast(definition.byteType, definition.charType, true);
            } else if (expected.clazz == int.class) {
                return new Cast(definition.byteType, definition.intType, explicit);
            } else if (expected.clazz == long.class) {
                return new Cast(definition.byteType, definition.longType, explicit);
            } else if (expected.clazz == float.class) {
                return new Cast(definition.byteType, definition.floatType, explicit);
            } else if (expected.clazz == double.class) {
                return new Cast(definition.byteType, definition.doubleType, explicit);
            } else if (expected.clazz == Byte.class && internal) {
                return new Cast(definition.byteType, definition.byteType, explicit, null, null, null, definition.byteType);
            } else if (expected.clazz == Short.class && internal) {
                return new Cast(definition.byteType, definition.shortType, explicit, null, null, null, definition.shortType);
            } else if (expected.clazz == Character.class && explicit && internal) {
                return new Cast(definition.byteType, definition.charType, true, null, null, null, definition.charType);
            } else if (expected.clazz == Integer.class && internal) {
                return new Cast(definition.byteType, definition.intType, explicit, null, null, null, definition.intType);
            } else if (expected.clazz == Long.class && internal) {
                return new Cast(definition.byteType, definition.longType, explicit, null, null, null, definition.longType);
            } else if (expected.clazz == Float.class && internal) {
                return new Cast(definition.byteType, definition.floatType, explicit, null, null, null, definition.floatType);
            } else if (expected.clazz == Double.class && internal) {
                return new Cast(definition.byteType, definition.doubleType, explicit, null, null, null, definition.doubleType);
            }
        } else if (actual.clazz == short.class) {
            if (expected.dynamic) {
                return new Cast(definition.ShortType, definition.DefType, explicit, null, null, definition.shortType, null);
            } else if (expected.clazz == Object.class && internal) {
                return new Cast(definition.ShortType, definition.ObjectType, explicit, null, null, definition.shortType, null);
            } else if (expected.clazz == Number.class && internal) {
                return new Cast(definition.ShortType, definition.NumberType, explicit, null, null, definition.shortType, null);
            } else if (expected.clazz == byte.class && explicit) {
                return new Cast(definition.shortType, definition.byteType, true);
            } else if (expected.clazz == char.class && explicit) {
                return new Cast(definition.shortType, definition.charType, true);
            } else if (expected.clazz == int.class) {
                return new Cast(definition.shortType, definition.intType, explicit);
            } else if (expected.clazz == long.class) {
                return new Cast(definition.shortType, definition.longType, explicit);
            } else if (expected.clazz == float.class) {
                return new Cast(definition.shortType, definition.floatType, explicit);
            } else if (expected.clazz == double.class) {
                return new Cast(definition.shortType, definition.doubleType, explicit);
            } else if (expected.clazz == Byte.class && explicit && internal) {
                return new Cast(definition.shortType, definition.byteType, true, null, null, null, definition.byteType);
            } else if (expected.clazz == Short.class && internal) {
                return new Cast(definition.shortType, definition.shortType, explicit, null, null, null, definition.shortType);
            } else if (expected.clazz == Character.class && explicit && internal) {
                return new Cast(definition.shortType, definition.charType, true, null, null, null, definition.charType);
            } else if (expected.clazz == Integer.class && internal) {
                return new Cast(definition.shortType, definition.intType, explicit, null, null, null, definition.intType);
            } else if (expected.clazz == Long.class && internal) {
                return new Cast(definition.shortType, definition.longType, explicit, null, null, null, definition.longType);
            } else if (expected.clazz == Float.class && internal) {
                return new Cast(definition.shortType, definition.floatType, explicit, null, null, null, definition.floatType);
            } else if (expected.clazz == Double.class && internal) {
                return new Cast(definition.shortType, definition.doubleType, explicit, null, null, null, definition.doubleType);
            }
        } else if (actual.clazz == char.class) {
            if (expected.dynamic) {
                return new Cast(definition.CharacterType, definition.DefType, explicit, null, null, definition.charType, null);
            } else if (expected.clazz == Object.class && internal) {
                return new Cast(definition.CharacterType, definition.ObjectType, explicit, null, null, definition.charType, null);
            } else if (expected.clazz == Number.class && internal) {
                return new Cast(definition.CharacterType, definition.NumberType, explicit, null, null, definition.charType, null);
            } else if (expected.clazz == String.class) {
                return new Cast(definition.charType, definition.StringType, explicit);
            } else if (expected.clazz == byte.class && explicit) {
                return new Cast(definition.charType, definition.byteType, true);
            } else if (expected.clazz == short.class && explicit) {
                return new Cast(definition.charType, definition.shortType, true);
            } else if (expected.clazz == int.class) {
                return new Cast(definition.charType, definition.intType, explicit);
            } else if (expected.clazz == long.class) {
                return new Cast(definition.charType, definition.longType, explicit);
            } else if (expected.clazz == float.class) {
                return new Cast(definition.charType, definition.floatType, explicit);
            } else if (expected.clazz == double.class) {
                return new Cast(definition.charType, definition.doubleType, explicit);
            } else if (expected.clazz == Byte.class && explicit && internal) {
                return new Cast(definition.charType, definition.byteType, true, null, null, null, definition.byteType);
            } else if (expected.clazz == Short.class && internal) {
                return new Cast(definition.charType, definition.shortType, explicit, null, null, null, definition.shortType);
            } else if (expected.clazz == Character.class && internal) {
                return new Cast(definition.charType, definition.charType, true, null, null, null, definition.charType);
            } else if (expected.clazz == Integer.class && internal) {
                return new Cast(definition.charType, definition.intType, explicit, null, null, null, definition.intType);
            } else if (expected.clazz == Long.class && internal) {
                return new Cast(definition.charType, definition.longType, explicit, null, null, null, definition.longType);
            } else if (expected.clazz == Float.class && internal) {
                return new Cast(definition.charType, definition.floatType, explicit, null, null, null, definition.floatType);
            } else if (expected.clazz == Double.class && internal) {
                return new Cast(definition.charType, definition.doubleType, explicit, null, null, null, definition.doubleType);
            }
        } else if (actual.clazz == int.class) {
            if (expected.dynamic) {
                return new Cast(definition.IntegerType, definition.DefType, explicit, null, null, definition.intType, null);
            } else if (expected.clazz == Object.class && internal) {
                return new Cast(definition.IntegerType, definition.ObjectType, explicit, null, null, definition.intType, null);
            } else if (expected.clazz == Number.class && internal) {
                return new Cast(definition.IntegerType, definition.NumberType, explicit, null, null, definition.intType, null);
            } else if (expected.clazz == byte.class && explicit) {
                return new Cast(definition.intType, definition.byteType, true);
            } else if (expected.clazz == char.class && explicit) {
                return new Cast(definition.intType, definition.charType, true);
            } else if (expected.clazz == short.class && explicit) {
                return new Cast(definition.intType, definition.shortType, true);
            } else if (expected.clazz == long.class) {
                return new Cast(definition.intType, definition.longType, explicit);
            } else if (expected.clazz == float.class) {
                return new Cast(definition.intType, definition.floatType, explicit);
            } else if (expected.clazz == double.class) {
                return new Cast(definition.intType, definition.doubleType, explicit);
            } else if (expected.clazz == Byte.class && explicit && internal) {
                return new Cast(definition.intType, definition.byteType, true, null, null, null, definition.byteType);
            } else if (expected.clazz == Short.class && explicit && internal) {
                return new Cast(definition.intType, definition.shortType, true, null, null, null, definition.shortType);
            } else if (expected.clazz == Character.class && explicit && internal) {
                return new Cast(definition.intType, definition.charType, true, null, null, null, definition.charType);
            } else if (expected.clazz == Integer.class && internal) {
                return new Cast(definition.intType, definition.intType, explicit, null, null, null, definition.intType);
            } else if (expected.clazz == Long.class && internal) {
                return new Cast(definition.intType, definition.longType, explicit, null, null, null, definition.longType);
            } else if (expected.clazz == Float.class && internal) {
                return new Cast(definition.intType, definition.floatType, explicit, null, null, null, definition.floatType);
            } else if (expected.clazz == Double.class && internal) {
                return new Cast(definition.intType, definition.doubleType, explicit, null, null, null, definition.doubleType);
            }
        } else if (actual.clazz == long.class) {
            if (expected.dynamic) {
                return new Cast(definition.LongType, definition.DefType, explicit, null, null, definition.longType, null);
            } else if (expected.clazz == Object.class && internal) {
                return new Cast(definition.LongType, definition.ObjectType, explicit, null, null, definition.longType, null);
            } else if (expected.clazz == Number.class && internal) {
                return new Cast(definition.LongType, definition.NumberType, explicit, null, null, definition.longType, null);
            } else if (expected.clazz == byte.class && explicit) {
                return new Cast(definition.longType, definition.byteType, true);
            } else if (expected.clazz == char.class && explicit) {
                return new Cast(definition.longType, definition.charType, true);
            } else if (expected.clazz == short.class && explicit) {
                return new Cast(definition.longType, definition.shortType, true);
            } else if (expected.clazz == int.class && explicit) {
                return new Cast(definition.longType, definition.intType, true);
            } else if (expected.clazz == float.class) {
                return new Cast(definition.longType, definition.floatType, explicit);
            } else if (expected.clazz == double.class) {
                return new Cast(definition.longType, definition.doubleType, explicit);
            } else if (expected.clazz == Byte.class && explicit && internal) {
                return new Cast(definition.longType, definition.byteType, true, null, null, null, definition.byteType);
            } else if (expected.clazz == Short.class && explicit && internal) {
                return new Cast(definition.longType, definition.shortType, true, null, null, null, definition.shortType);
            } else if (expected.clazz == Character.class && explicit && internal) {
                return new Cast(definition.longType, definition.charType, true, null, null, null, definition.charType);
            } else if (expected.clazz == Integer.class && explicit && internal) {
                return new Cast(definition.longType, definition.intType, true, null, null, null, definition.intType);
            } else if (expected.clazz == Long.class && internal) {
                return new Cast(definition.longType, definition.longType, explicit, null, null, null, definition.longType);
            } else if (expected.clazz == Float.class && internal) {
                return new Cast(definition.longType, definition.floatType, explicit, null, null, null, definition.floatType);
            } else if (expected.clazz == Double.class && internal) {
                return new Cast(definition.longType, definition.doubleType, explicit, null, null, null, definition.doubleType);
            }
        } else if (actual.clazz == float.class) {
            if (expected.dynamic) {
                return new Cast(definition.FloatType, definition.DefType, explicit, null, null, definition.floatType, null);
            } else if (expected.clazz == Object.class && internal) {
                return new Cast(definition.FloatType, definition.ObjectType, explicit, null, null, definition.floatType, null);
            } else if (expected.clazz == Number.class && internal) {
                return new Cast(definition.FloatType, definition.NumberType, explicit, null, null, definition.floatType, null);
            } else if (expected.clazz == byte.class && explicit) {
                return new Cast(definition.floatType, definition.byteType, true);
            } else if (expected.clazz == char.class && explicit) {
                return new Cast(definition.floatType, definition.charType, true);
            } else if (expected.clazz == short.class && explicit) {
                return new Cast(definition.floatType, definition.shortType, true);
            } else if (expected.clazz == int.class && explicit) {
                return new Cast(definition.floatType, definition.intType, true);
            } else if (expected.clazz == long.class && explicit) {
                return new Cast(definition.floatType, definition.longType, true);
            } else if (expected.clazz == double.class) {
                return new Cast(definition.floatType, definition.doubleType, explicit);
            } else if (expected.clazz == Byte.class && explicit && internal) {
                return new Cast(definition.floatType, definition.byteType, true, null, null, null, definition.byteType);
            } else if (expected.clazz == Short.class && explicit && internal) {
                return new Cast(definition.floatType, definition.shortType, true, null, null, null, definition.shortType);
            } else if (expected.clazz == Character.class && explicit && internal) {
                return new Cast(definition.floatType, definition.charType, true, null, null, null, definition.charType);
            } else if (expected.clazz == Integer.class && explicit && internal) {
                return new Cast(definition.floatType, definition.intType, true, null, null, null, definition.intType);
            } else if (expected.clazz == Long.class && explicit && internal) {
                return new Cast(definition.floatType, definition.longType, true, null, null, null, definition.longType);
            } else if (expected.clazz == Float.class && internal) {
                return new Cast(definition.floatType, definition.floatType, explicit, null, null, null, definition.floatType);
            } else if (expected.clazz == Double.class && internal) {
                return new Cast(definition.floatType, definition.doubleType, explicit, null, null, null, definition.doubleType);
            }
        } else if (actual.clazz == double.class) {
            if (expected.dynamic) {
                return new Cast(definition.DoubleType, definition.DefType, explicit, null, null, definition.doubleType, null);
            } else if (expected.clazz == Object.class && internal) {
                return new Cast(definition.DoubleType, definition.ObjectType, explicit, null, null, definition.doubleType, null);
            } else if (expected.clazz == Number.class && internal) {
                return new Cast(definition.DoubleType, definition.NumberType, explicit, null, null, definition.doubleType, null);
            } else if (expected.clazz == byte.class && explicit) {
                return new Cast(definition.doubleType, definition.byteType, true);
            } else if (expected.clazz == char.class && explicit) {
                return new Cast(definition.doubleType, definition.charType, true);
            } else if (expected.clazz == short.class && explicit) {
                return new Cast(definition.doubleType, definition.shortType, true);
            } else if (expected.clazz == int.class && explicit) {
                return new Cast(definition.doubleType, definition.intType, true);
            } else if (expected.clazz == long.class && explicit) {
                return new Cast(definition.doubleType, definition.longType, true);
            } else if (expected.clazz == float.class && explicit) {
                return new Cast(definition.doubleType, definition.floatType, true);
            } else if (expected.clazz == Byte.class && explicit && internal) {
                return new Cast(definition.doubleType, definition.byteType, true, null, null, null, definition.byteType);
            } else if (expected.clazz == Short.class && explicit && internal) {
                return new Cast(definition.doubleType, definition.shortType, true, null, null, null, definition.shortType);
            } else if (expected.clazz == Character.class && explicit && internal) {
                return new Cast(definition.doubleType, definition.charType, true, null, null, null, definition.charType);
            } else if (expected.clazz == Integer.class && explicit && internal) {
                return new Cast(definition.doubleType, definition.intType, true, null, null, null, definition.intType);
            } else if (expected.clazz == Long.class && explicit && internal) {
                return new Cast(definition.doubleType, definition.longType, true, null, null, null, definition.longType);
            } else if (expected.clazz == Float.class && explicit && internal) {
                return new Cast(definition.doubleType, definition.floatType, true, null, null, null, definition.floatType);
            } else if (expected.clazz == Double.class && internal) {
                return new Cast(definition.doubleType, definition.doubleType, explicit, null, null, null, definition.doubleType);
            }
        } else if (actual.clazz == Boolean.class) {
            if (expected.clazz == boolean.class && internal) {
                return new Cast(definition.booleanType, definition.booleanType, explicit, definition.booleanType, null, null, null);
            }
        } else if (actual.clazz == Byte.class) {
            if (expected.clazz == byte.class && internal) {
                return new Cast(definition.byteType, definition.byteType, explicit, definition.byteType, null, null, null);
            } else if (expected.clazz == short.class && internal) {
                return new Cast(definition.byteType, definition.shortType, explicit, definition.byteType, null, null, null);
            } else if (expected.clazz == char.class && explicit && internal) {
                return new Cast(definition.byteType, definition.charType, true, definition.byteType, null, null, null);
            } else if (expected.clazz == int.class && internal) {
                return new Cast(definition.byteType, definition.intType, explicit, definition.byteType, null, null, null);
            } else if (expected.clazz == long.class && internal) {
                return new Cast(definition.byteType, definition.longType, explicit, definition.byteType, null, null, null);
            } else if (expected.clazz == float.class && internal) {
                return new Cast(definition.byteType, definition.floatType, explicit, definition.byteType, null, null, null);
            } else if (expected.clazz == double.class && internal) {
                return new Cast(definition.byteType, definition.doubleType, explicit, definition.byteType, null, null, null);
            }
        } else if (actual.clazz == Short.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return new Cast(definition.shortType, definition.byteType, true, definition.shortType, null, null, null);
            } else if (expected.clazz == short.class && internal) {
                return new Cast(definition.shortType, definition.shortType, explicit, definition.shortType, null, null, null);
            } else if (expected.clazz == char.class && explicit && internal) {
                return new Cast(definition.shortType, definition.charType, true, definition.shortType, null, null, null);
            } else if (expected.clazz == int.class && internal) {
                return new Cast(definition.shortType, definition.intType, explicit, definition.shortType, null, null, null);
            } else if (expected.clazz == long.class && internal) {
                return new Cast(definition.shortType, definition.longType, explicit, definition.shortType, null, null, null);
            } else if (expected.clazz == float.class && internal) {
                return new Cast(definition.shortType, definition.floatType, explicit, definition.shortType, null, null, null);
            } else if (expected.clazz == double.class && internal) {
                return new Cast(definition.shortType, definition.doubleType, explicit, definition.shortType, null, null, null);
            }
        } else if (actual.clazz == Character.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return new Cast(definition.charType, definition.byteType, true, definition.charType, null, null, null);
            } else if (expected.clazz == short.class && explicit && internal) {
                return new Cast(definition.charType, definition.shortType, true, definition.charType, null, null, null);
            } else if (expected.clazz == char.class && internal) {
                return new Cast(definition.charType, definition.charType, explicit, definition.charType, null, null, null);
            } else if (expected.clazz == int.class && internal) {
                return new Cast(definition.charType, definition.intType, explicit, definition.charType, null, null, null);
            } else if (expected.clazz == long.class && internal) {
                return new Cast(definition.charType, definition.longType, explicit, definition.charType, null, null, null);
            } else if (expected.clazz == float.class && internal) {
                return new Cast(definition.charType, definition.floatType, explicit, definition.charType, null, null, null);
            } else if (expected.clazz == double.class && internal) {
                return new Cast(definition.charType, definition.doubleType, explicit, definition.charType, null, null, null);
            }
        } else if (actual.clazz == Integer.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return new Cast(definition.intType, definition.byteType, true, definition.intType, null, null, null);
            } else if (expected.clazz == short.class && explicit && internal) {
                return new Cast(definition.intType, definition.shortType, true, definition.intType, null, null, null);
            } else if (expected.clazz == char.class && explicit && internal) {
                return new Cast(definition.intType, definition.charType, true, definition.intType, null, null, null);
            } else if (expected.clazz == int.class && internal) {
                return new Cast(definition.intType, definition.intType, explicit, definition.intType, null, null, null);
            } else if (expected.clazz == long.class && internal) {
                return new Cast(definition.intType, definition.longType, explicit, definition.intType, null, null, null);
            } else if (expected.clazz == float.class && internal) {
                return new Cast(definition.intType, definition.floatType, explicit, definition.intType, null, null, null);
            } else if (expected.clazz == double.class && internal) {
                return new Cast(definition.intType, definition.doubleType, explicit, definition.intType, null, null, null);
            }
        } else if (actual.clazz == Long.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return new Cast(definition.longType, definition.byteType, true, definition.longType, null, null, null);
            } else if (expected.clazz == short.class && explicit && internal) {
                return new Cast(definition.longType, definition.shortType, true, definition.longType, null, null, null);
            } else if (expected.clazz == char.class && explicit && internal) {
                return new Cast(definition.longType, definition.charType, true, definition.longType, null, null, null);
            } else if (expected.clazz == int.class && explicit && internal) {
                return new Cast(definition.longType, definition.intType, true, definition.longType, null, null, null);
            } else if (expected.clazz == long.class && internal) {
                return new Cast(definition.longType, definition.longType, explicit, definition.longType, null, null, null);
            } else if (expected.clazz == float.class && internal) {
                return new Cast(definition.longType, definition.floatType, explicit, definition.longType, null, null, null);
            } else if (expected.clazz == double.class && internal) {
                return new Cast(definition.longType, definition.doubleType, explicit, definition.longType, null, null, null);
            }
        } else if (actual.clazz == Float.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return new Cast(definition.floatType, definition.byteType, true, definition.floatType, null, null, null);
            } else if (expected.clazz == short.class && explicit && internal) {
                return new Cast(definition.floatType, definition.shortType, true, definition.floatType, null, null, null);
            } else if (expected.clazz == char.class && explicit && internal) {
                return new Cast(definition.floatType, definition.charType, true, definition.floatType, null, null, null);
            } else if (expected.clazz == int.class && explicit && internal) {
                return new Cast(definition.floatType, definition.intType, true, definition.floatType, null, null, null);
            } else if (expected.clazz == long.class && explicit && internal) {
                return new Cast(definition.floatType, definition.longType, true, definition.floatType, null, null, null);
            } else if (expected.clazz == float.class && internal) {
                return new Cast(definition.floatType, definition.floatType, explicit, definition.floatType, null, null, null);
            } else if (expected.clazz == double.class && internal) {
                return new Cast(definition.floatType, definition.doubleType, explicit, definition.floatType, null, null, null);
            }
        } else if (actual.clazz == Double.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return new Cast(definition.doubleType, definition.byteType, true, definition.doubleType, null, null, null);
            } else if (expected.clazz == short.class && explicit && internal) {
                return new Cast(definition.doubleType, definition.shortType, true, definition.doubleType, null, null, null);
            } else if (expected.clazz == char.class && explicit && internal) {
                return new Cast(definition.doubleType, definition.charType, true, definition.doubleType, null, null, null);
            } else if (expected.clazz == int.class && explicit && internal) {
                return new Cast(definition.doubleType, definition.intType, true, definition.doubleType, null, null, null);
            } else if (expected.clazz == long.class && explicit && internal) {
                return new Cast(definition.doubleType, definition.longType, true, definition.doubleType, null, null, null);
            } else if (expected.clazz == float.class && explicit && internal) {
                return new Cast(definition.doubleType, definition.floatType, true, definition.doubleType, null, null, null);
            } else if (expected.clazz == double.class && internal) {
                return new Cast(definition.doubleType, definition.doubleType, explicit, definition.doubleType, null, null, null);
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

    public Object constCast(Location location, final Object constant, final Cast cast) {
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

    public Type promoteNumeric(Type from, boolean decimal) {
        Class<?> sort = from.clazz;

        if (from.dynamic) {
            return definition.DefType;
        } else if ((sort == double.class) && decimal) {
            return definition.doubleType;
        } else if ((sort == float.class) && decimal) {
            return  definition.floatType;
        } else if (sort == long.class) {
            return definition.longType;
        } else if (sort == int.class || sort == char.class || sort == short.class || sort == byte.class) {
            return definition.intType;
        }

        return null;
    }

    public Type promoteNumeric(Type from0, Type from1, boolean decimal) {
        Class<?> sort0 = from0.clazz;
        Class<?> sort1 = from1.clazz;

        if (from0.dynamic || from1.dynamic) {
            return definition.DefType;
        }

        if (decimal) {
            if (sort0 == double.class || sort1 == double.class) {
                return definition.doubleType;
            } else if (sort0 == float.class || sort1 == float.class) {
                return definition.floatType;
            }
        }

        if (sort0 == long.class || sort1 == long.class) {
            return definition.longType;
        } else if (sort0 == int.class   || sort1 == int.class   ||
                   sort0 == char.class  || sort1 == char.class  ||
                   sort0 == short.class || sort1 == short.class ||
                   sort0 == byte.class  || sort1 == byte.class) {
            return definition.intType;
        }

        return null;
    }

    public Type promoteAdd(Type from0, Type from1) {
        Class<?> sort0 = from0.clazz;
        Class<?> sort1 = from1.clazz;

        if (sort0 == String.class || sort1 == String.class) {
            return definition.StringType;
        }

        return promoteNumeric(from0, from1, true);
    }

    public Type promoteXor(Type from0, Type from1) {
        Class<?> sort0 = from0.clazz;
        Class<?> sort1 = from1.clazz;

        if (from0.dynamic || from1.dynamic) {
            return definition.DefType;
        }

        if (sort0 == boolean.class || sort1 == boolean.class) {
            return definition.booleanType;
        }

        return promoteNumeric(from0, from1, false);
    }

    public Type promoteEquality(Type from0, Type from1) {
        Class<?> sort0 = from0.clazz;
        Class<?> sort1 = from1.clazz;

        if (from0.dynamic || from1.dynamic) {
            return definition.DefType;
        }

        if (sort0.isPrimitive() && sort1.isPrimitive()) {
            if (sort0 == boolean.class && sort1 == boolean.class) {
                return definition.booleanType;
            }

            return promoteNumeric(from0, from1, true);
        }

        return definition.ObjectType;
    }

    public Type promoteConditional(Type from0, Type from1, Object const0, Object const1) {
        if (from0.equals(from1)) {
            return from0;
        }

        Class<?> sort0 = from0.clazz;
        Class<?> sort1 = from1.clazz;

        if (from0.dynamic || from1.dynamic) {
            return definition.DefType;
        }

        if (sort0.isPrimitive() && sort1.isPrimitive()) {
            if (sort0 == boolean.class && sort1 == boolean.class) {
                return definition.booleanType;
            }

            if (sort0 == double.class || sort1 == double.class) {
                return definition.doubleType;
            } else if (sort0 == float.class || sort1 == float.class) {
                return definition.floatType;
            } else if (sort0 == long.class || sort1 == long.class) {
                return definition.longType;
            } else {
                if (sort0 == byte.class) {
                    if (sort1 == byte.class) {
                        return definition.byteType;
                    } else if (sort1 == short.class) {
                        if (const1 != null) {
                            final short constant = (short)const1;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return definition.byteType;
                            }
                        }

                        return definition.shortType;
                    } else if (sort1 == char.class) {
                        return definition.intType;
                    } else if (sort1 == int.class) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return definition.byteType;
                            }
                        }

                        return definition.intType;
                    }
                } else if (sort0 == short.class) {
                    if (sort1 == byte.class) {
                        if (const0 != null) {
                            final short constant = (short)const0;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return definition.byteType;
                            }
                        }

                        return definition.shortType;
                    } else if (sort1 == short.class) {
                        return definition.shortType;
                    } else if (sort1 == char.class) {
                        return definition.intType;
                    } else if (sort1 == int.class) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Short.MAX_VALUE && constant >= Short.MIN_VALUE) {
                                return definition.shortType;
                            }
                        }

                        return definition.intType;
                    }
                } else if (sort0 == char.class) {
                    if (sort1 == byte.class) {
                        return definition.intType;
                    } else if (sort1 == short.class) {
                        return definition.intType;
                    } else if (sort1 == char.class) {
                        return definition.charType;
                    } else if (sort1 == int.class) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Character.MAX_VALUE && constant >= Character.MIN_VALUE) {
                                return definition.byteType;
                            }
                        }

                        return definition.intType;
                    }
                } else if (sort0 == int.class) {
                    if (sort1 == byte.class) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return definition.byteType;
                            }
                        }

                        return definition.intType;
                    } else if (sort1 == short.class) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Short.MAX_VALUE && constant >= Short.MIN_VALUE) {
                                return definition.byteType;
                            }
                        }

                        return definition.intType;
                    } else if (sort1 == char.class) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Character.MAX_VALUE && constant >= Character.MIN_VALUE) {
                                return definition.byteType;
                            }
                        }

                        return definition.intType;
                    } else if (sort1 == int.class) {
                        return definition.intType;
                    }
                }
            }
        }

        // TODO: In the rare case we still haven't reached a correct promotion we need
        // TODO: to calculate the highest upper bound for the two types and return that.
        // TODO: However, for now we just return objectType that may require an extra cast.

        return definition.ObjectType;
    }
}
