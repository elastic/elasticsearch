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
                return Cast.unboxTo(definition.DefType, definition.BooleanType, explicit, definition.booleanType);
            } else if (expected.clazz == byte.class) {
                return Cast.unboxTo(definition.DefType, definition.ByteType, explicit, definition.byteType);
            } else if (expected.clazz == short.class) {
                return Cast.unboxTo(definition.DefType, definition.ShortType, explicit, definition.shortType);
            } else if (expected.clazz == char.class) {
                return Cast.unboxTo(definition.DefType, definition.CharacterType, explicit, definition.charType);
            } else if (expected.clazz == int.class) {
                return Cast.unboxTo(definition.DefType, definition.IntegerType, explicit, definition.intType);
            } else if (expected.clazz == long.class) {
                return Cast.unboxTo(definition.DefType, definition.LongType, explicit, definition.longType);
            } else if (expected.clazz == float.class) {
                return Cast.unboxTo(definition.DefType, definition.FloatType, explicit, definition.floatType);
            } else if (expected.clazz == double.class) {
                return Cast.unboxTo(definition.DefType, definition.DoubleType, explicit, definition.doubleType);
            }
        } else if (actual.clazz == Object.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return Cast.unboxTo(definition.ObjectType, definition.ByteType, true, definition.byteType);
            } else if (expected.clazz == short.class && explicit && internal) {
                return Cast.unboxTo(definition.ObjectType, definition.ShortType, true, definition.shortType);
            } else if (expected.clazz == char.class && explicit && internal) {
                return Cast.unboxTo(definition.ObjectType, definition.CharacterType, true, definition.charType);
            } else if (expected.clazz == int.class && explicit && internal) {
                return Cast.unboxTo(definition.ObjectType, definition.IntegerType, true, definition.intType);
            } else if (expected.clazz == long.class && explicit && internal) {
                return Cast.unboxTo(definition.ObjectType, definition.LongType, true, definition.longType);
            } else if (expected.clazz == float.class && explicit && internal) {
                return Cast.unboxTo(definition.ObjectType, definition.FloatType, true, definition.floatType);
            } else if (expected.clazz == double.class && explicit && internal) {
                return Cast.unboxTo(definition.ObjectType, definition.DoubleType, true, definition.doubleType);
            }
        } else if (actual.clazz == Number.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return Cast.unboxTo(definition.NumberType, definition.ByteType, true, definition.byteType);
            } else if (expected.clazz == short.class && explicit && internal) {
                return Cast.unboxTo(definition.NumberType, definition.ShortType, true, definition.shortType);
            } else if (expected.clazz == char.class && explicit && internal) {
                return Cast.unboxTo(definition.NumberType, definition.CharacterType, true, definition.charType);
            } else if (expected.clazz == int.class && explicit && internal) {
                return Cast.unboxTo(definition.NumberType, definition.IntegerType, true, definition.intType);
            } else if (expected.clazz == long.class && explicit && internal) {
                return Cast.unboxTo(definition.NumberType, definition.LongType, true, definition.longType);
            } else if (expected.clazz == float.class && explicit && internal) {
                return Cast.unboxTo(definition.NumberType, definition.FloatType, true, definition.floatType);
            } else if (expected.clazz == double.class && explicit && internal) {
                return Cast.unboxTo(definition.NumberType, definition.DoubleType, true, definition.doubleType);
            }
        } else if (actual.clazz == String.class) {
            if (expected.clazz == char.class && explicit) {
                return Cast.standard(definition.StringType, definition.charType, true);
            }
        } else if (actual.clazz == boolean.class) {
            if (expected.dynamic) {
                return Cast.boxFrom(definition.BooleanType, definition.DefType, explicit, definition.booleanType);
            } else if (expected.clazz == Object.class && internal) {
                return Cast.boxFrom(definition.BooleanType, definition.ObjectType, explicit, definition.booleanType);
            } else if (expected.clazz == Boolean.class && internal) {
                return Cast.boxTo(definition.booleanType, definition.booleanType, explicit, definition.booleanType);
            }
        } else if (actual.clazz == byte.class) {
            if (expected.dynamic) {
                return Cast.boxFrom(definition.ByteType, definition.DefType, explicit, definition.byteType);
            } else if (expected.clazz == Object.class && internal) {
                return Cast.boxFrom(definition.ByteType, definition.ObjectType, explicit, definition.byteType);
            } else if (expected.clazz == Number.class && internal) {
                return Cast.boxFrom(definition.ByteType, definition.NumberType, explicit, definition.byteType);
            } else if (expected.clazz == short.class) {
                return Cast.standard(definition.byteType, definition.shortType, explicit);
            } else if (expected.clazz == char.class && explicit) {
                return Cast.standard(definition.byteType, definition.charType, true);
            } else if (expected.clazz == int.class) {
                return Cast.standard(definition.byteType, definition.intType, explicit);
            } else if (expected.clazz == long.class) {
                return Cast.standard(definition.byteType, definition.longType, explicit);
            } else if (expected.clazz == float.class) {
                return Cast.standard(definition.byteType, definition.floatType, explicit);
            } else if (expected.clazz == double.class) {
                return Cast.standard(definition.byteType, definition.doubleType, explicit);
            } else if (expected.clazz == Byte.class && internal) {
                return Cast.boxTo(definition.byteType, definition.byteType, explicit, definition.byteType);
            } else if (expected.clazz == Short.class && internal) {
                return Cast.boxTo(definition.byteType, definition.shortType, explicit, definition.shortType);
            } else if (expected.clazz == Character.class && explicit && internal) {
                return Cast.boxTo(definition.byteType, definition.charType, true, definition.charType);
            } else if (expected.clazz == Integer.class && internal) {
                return Cast.boxTo(definition.byteType, definition.intType, explicit, definition.intType);
            } else if (expected.clazz == Long.class && internal) {
                return Cast.boxTo(definition.byteType, definition.longType, explicit, definition.longType);
            } else if (expected.clazz == Float.class && internal) {
                return Cast.boxTo(definition.byteType, definition.floatType, explicit, definition.floatType);
            } else if (expected.clazz == Double.class && internal) {
                return Cast.boxTo(definition.byteType, definition.doubleType, explicit, definition.doubleType);
            }
        } else if (actual.clazz == short.class) {
            if (expected.dynamic) {
                return Cast.boxFrom(definition.ShortType, definition.DefType, explicit, definition.shortType);
            } else if (expected.clazz == Object.class && internal) {
                return Cast.boxFrom(definition.ShortType, definition.ObjectType, explicit, definition.shortType);
            } else if (expected.clazz == Number.class && internal) {
                return Cast.boxFrom(definition.ShortType, definition.NumberType, explicit, definition.shortType);
            } else if (expected.clazz == byte.class && explicit) {
                return Cast.standard(definition.shortType, definition.byteType, true);
            } else if (expected.clazz == char.class && explicit) {
                return Cast.standard(definition.shortType, definition.charType, true);
            } else if (expected.clazz == int.class) {
                return Cast.standard(definition.shortType, definition.intType, explicit);
            } else if (expected.clazz == long.class) {
                return Cast.standard(definition.shortType, definition.longType, explicit);
            } else if (expected.clazz == float.class) {
                return Cast.standard(definition.shortType, definition.floatType, explicit);
            } else if (expected.clazz == double.class) {
                return Cast.standard(definition.shortType, definition.doubleType, explicit);
            } else if (expected.clazz == Byte.class && explicit && internal) {
                return Cast.boxTo(definition.shortType, definition.byteType, true, definition.byteType);
            } else if (expected.clazz == Short.class && internal) {
                return Cast.boxTo(definition.shortType, definition.shortType, explicit, definition.shortType);
            } else if (expected.clazz == Character.class && explicit && internal) {
                return Cast.boxTo(definition.shortType, definition.charType, true, definition.charType);
            } else if (expected.clazz == Integer.class && internal) {
                return Cast.boxTo(definition.shortType, definition.intType, explicit, definition.intType);
            } else if (expected.clazz == Long.class && internal) {
                return Cast.boxTo(definition.shortType, definition.longType, explicit, definition.longType);
            } else if (expected.clazz == Float.class && internal) {
                return Cast.boxTo(definition.shortType, definition.floatType, explicit, definition.floatType);
            } else if (expected.clazz == Double.class && internal) {
                return Cast.boxTo(definition.shortType, definition.doubleType, explicit, definition.doubleType);
            }
        } else if (actual.clazz == char.class) {
            if (expected.dynamic) {
                return Cast.boxFrom(definition.CharacterType, definition.DefType, explicit, definition.charType);
            } else if (expected.clazz == Object.class && internal) {
                return Cast.boxFrom(definition.CharacterType, definition.ObjectType, explicit, definition.charType);
            } else if (expected.clazz == Number.class && internal) {
                return Cast.boxFrom(definition.CharacterType, definition.NumberType, explicit, definition.charType);
            } else if (expected.clazz == String.class) {
                return Cast.standard(definition.charType, definition.StringType, explicit);
            } else if (expected.clazz == byte.class && explicit) {
                return Cast.standard(definition.charType, definition.byteType, true);
            } else if (expected.clazz == short.class && explicit) {
                return Cast.standard(definition.charType, definition.shortType, true);
            } else if (expected.clazz == int.class) {
                return Cast.standard(definition.charType, definition.intType, explicit);
            } else if (expected.clazz == long.class) {
                return Cast.standard(definition.charType, definition.longType, explicit);
            } else if (expected.clazz == float.class) {
                return Cast.standard(definition.charType, definition.floatType, explicit);
            } else if (expected.clazz == double.class) {
                return Cast.standard(definition.charType, definition.doubleType, explicit);
            } else if (expected.clazz == Byte.class && explicit && internal) {
                return Cast.boxTo(definition.charType, definition.byteType, true, definition.byteType);
            } else if (expected.clazz == Short.class && internal) {
                return Cast.boxTo(definition.charType, definition.shortType, explicit, definition.shortType);
            } else if (expected.clazz == Character.class && internal) {
                return Cast.boxTo(definition.charType, definition.charType, true, definition.charType);
            } else if (expected.clazz == Integer.class && internal) {
                return Cast.boxTo(definition.charType, definition.intType, explicit, definition.intType);
            } else if (expected.clazz == Long.class && internal) {
                return Cast.boxTo(definition.charType, definition.longType, explicit, definition.longType);
            } else if (expected.clazz == Float.class && internal) {
                return Cast.boxTo(definition.charType, definition.floatType, explicit, definition.floatType);
            } else if (expected.clazz == Double.class && internal) {
                return Cast.boxTo(definition.charType, definition.doubleType, explicit, definition.doubleType);
            }
        } else if (actual.clazz == int.class) {
            if (expected.dynamic) {
                return Cast.boxFrom(definition.IntegerType, definition.DefType, explicit, definition.intType);
            } else if (expected.clazz == Object.class && internal) {
                return Cast.boxFrom(definition.IntegerType, definition.ObjectType, explicit, definition.intType);
            } else if (expected.clazz == Number.class && internal) {
                return Cast.boxFrom(definition.IntegerType, definition.NumberType, explicit, definition.intType);
            } else if (expected.clazz == byte.class && explicit) {
                return Cast.standard(definition.intType, definition.byteType, true);
            } else if (expected.clazz == char.class && explicit) {
                return Cast.standard(definition.intType, definition.charType, true);
            } else if (expected.clazz == short.class && explicit) {
                return Cast.standard(definition.intType, definition.shortType, true);
            } else if (expected.clazz == long.class) {
                return Cast.standard(definition.intType, definition.longType, explicit);
            } else if (expected.clazz == float.class) {
                return Cast.standard(definition.intType, definition.floatType, explicit);
            } else if (expected.clazz == double.class) {
                return Cast.standard(definition.intType, definition.doubleType, explicit);
            } else if (expected.clazz == Byte.class && explicit && internal) {
                return Cast.boxTo(definition.intType, definition.byteType, true, definition.byteType);
            } else if (expected.clazz == Short.class && explicit && internal) {
                return Cast.boxTo(definition.intType, definition.shortType, true, definition.shortType);
            } else if (expected.clazz == Character.class && explicit && internal) {
                return Cast.boxTo(definition.intType, definition.charType, true, definition.charType);
            } else if (expected.clazz == Integer.class && internal) {
                return Cast.boxTo(definition.intType, definition.intType, explicit, definition.intType);
            } else if (expected.clazz == Long.class && internal) {
                return Cast.boxTo(definition.intType, definition.longType, explicit, definition.longType);
            } else if (expected.clazz == Float.class && internal) {
                return Cast.boxTo(definition.intType, definition.floatType, explicit, definition.floatType);
            } else if (expected.clazz == Double.class && internal) {
                return Cast.boxTo(definition.intType, definition.doubleType, explicit, definition.doubleType);
            }
        } else if (actual.clazz == long.class) {
            if (expected.dynamic) {
                return Cast.boxFrom(definition.LongType, definition.DefType, explicit, definition.longType);
            } else if (expected.clazz == Object.class && internal) {
                return Cast.boxFrom(definition.LongType, definition.ObjectType, explicit, definition.longType);
            } else if (expected.clazz == Number.class && internal) {
                return Cast.boxFrom(definition.LongType, definition.NumberType, explicit, definition.longType);
            } else if (expected.clazz == byte.class && explicit) {
                return Cast.standard(definition.longType, definition.byteType, true);
            } else if (expected.clazz == char.class && explicit) {
                return Cast.standard(definition.longType, definition.charType, true);
            } else if (expected.clazz == short.class && explicit) {
                return Cast.standard(definition.longType, definition.shortType, true);
            } else if (expected.clazz == int.class && explicit) {
                return Cast.standard(definition.longType, definition.intType, true);
            } else if (expected.clazz == float.class) {
                return Cast.standard(definition.longType, definition.floatType, explicit);
            } else if (expected.clazz == double.class) {
                return Cast.standard(definition.longType, definition.doubleType, explicit);
            } else if (expected.clazz == Byte.class && explicit && internal) {
                return Cast.boxTo(definition.longType, definition.byteType, true, definition.byteType);
            } else if (expected.clazz == Short.class && explicit && internal) {
                return Cast.boxTo(definition.longType, definition.shortType, true, definition.shortType);
            } else if (expected.clazz == Character.class && explicit && internal) {
                return Cast.boxTo(definition.longType, definition.charType, true, definition.charType);
            } else if (expected.clazz == Integer.class && explicit && internal) {
                return Cast.boxTo(definition.longType, definition.intType, true, definition.intType);
            } else if (expected.clazz == Long.class && internal) {
                return Cast.boxTo(definition.longType, definition.longType, explicit, definition.longType);
            } else if (expected.clazz == Float.class && internal) {
                return Cast.boxTo(definition.longType, definition.floatType, explicit, definition.floatType);
            } else if (expected.clazz == Double.class && internal) {
                return Cast.boxTo(definition.longType, definition.doubleType, explicit, definition.doubleType);
            }
        } else if (actual.clazz == float.class) {
            if (expected.dynamic) {
                return Cast.boxFrom(definition.FloatType, definition.DefType, explicit, definition.floatType);
            } else if (expected.clazz == Object.class && internal) {
                return Cast.boxFrom(definition.FloatType, definition.ObjectType, explicit, definition.floatType);
            } else if (expected.clazz == Number.class && internal) {
                return Cast.boxFrom(definition.FloatType, definition.NumberType, explicit, definition.floatType);
            } else if (expected.clazz == byte.class && explicit) {
                return Cast.standard(definition.floatType, definition.byteType, true);
            } else if (expected.clazz == char.class && explicit) {
                return Cast.standard(definition.floatType, definition.charType, true);
            } else if (expected.clazz == short.class && explicit) {
                return Cast.standard(definition.floatType, definition.shortType, true);
            } else if (expected.clazz == int.class && explicit) {
                return Cast.standard(definition.floatType, definition.intType, true);
            } else if (expected.clazz == long.class && explicit) {
                return Cast.standard(definition.floatType, definition.longType, true);
            } else if (expected.clazz == double.class) {
                return Cast.standard(definition.floatType, definition.doubleType, explicit);
            } else if (expected.clazz == Byte.class && explicit && internal) {
                return Cast.boxTo(definition.floatType, definition.byteType, true, definition.byteType);
            } else if (expected.clazz == Short.class && explicit && internal) {
                return Cast.boxTo(definition.floatType, definition.shortType, true, definition.shortType);
            } else if (expected.clazz == Character.class && explicit && internal) {
                return Cast.boxTo(definition.floatType, definition.charType, true, definition.charType);
            } else if (expected.clazz == Integer.class && explicit && internal) {
                return Cast.boxTo(definition.floatType, definition.intType, true, definition.intType);
            } else if (expected.clazz == Long.class && explicit && internal) {
                return Cast.boxTo(definition.floatType, definition.longType, true, definition.longType);
            } else if (expected.clazz == Float.class && internal) {
                return Cast.boxTo(definition.floatType, definition.floatType, explicit, definition.floatType);
            } else if (expected.clazz == Double.class && internal) {
                return Cast.boxTo(definition.floatType, definition.doubleType, explicit, definition.doubleType);
            }
        } else if (actual.clazz == double.class) {
            if (expected.dynamic) {
                return Cast.boxFrom(definition.DoubleType, definition.DefType, explicit, definition.doubleType);
            } else if (expected.clazz == Object.class && internal) {
                return Cast.boxFrom(definition.DoubleType, definition.ObjectType, explicit, definition.doubleType);
            } else if (expected.clazz == Number.class && internal) {
                return Cast.boxFrom(definition.DoubleType, definition.NumberType, explicit, definition.doubleType);
            } else if (expected.clazz == byte.class && explicit) {
                return Cast.standard(definition.doubleType, definition.byteType, true);
            } else if (expected.clazz == char.class && explicit) {
                return Cast.standard(definition.doubleType, definition.charType, true);
            } else if (expected.clazz == short.class && explicit) {
                return Cast.standard(definition.doubleType, definition.shortType, true);
            } else if (expected.clazz == int.class && explicit) {
                return Cast.standard(definition.doubleType, definition.intType, true);
            } else if (expected.clazz == long.class && explicit) {
                return Cast.standard(definition.doubleType, definition.longType, true);
            } else if (expected.clazz == float.class && explicit) {
                return Cast.standard(definition.doubleType, definition.floatType, true);
            } else if (expected.clazz == Byte.class && explicit && internal) {
                return Cast.boxTo(definition.doubleType, definition.byteType, true, definition.byteType);
            } else if (expected.clazz == Short.class && explicit && internal) {
                return Cast.boxTo(definition.doubleType, definition.shortType, true, definition.shortType);
            } else if (expected.clazz == Character.class && explicit && internal) {
                return Cast.boxTo(definition.doubleType, definition.charType, true, definition.charType);
            } else if (expected.clazz == Integer.class && explicit && internal) {
                return Cast.boxTo(definition.doubleType, definition.intType, true, definition.intType);
            } else if (expected.clazz == Long.class && explicit && internal) {
                return Cast.boxTo(definition.doubleType, definition.longType, true, definition.longType);
            } else if (expected.clazz == Float.class && explicit && internal) {
                return Cast.boxTo(definition.doubleType, definition.floatType, true, definition.floatType);
            } else if (expected.clazz == Double.class && internal) {
                return Cast.boxTo(definition.doubleType, definition.doubleType, explicit, definition.doubleType);
            }
        } else if (actual.clazz == Boolean.class) {
            if (expected.clazz == boolean.class && internal) {
                return Cast.unboxFrom(definition.booleanType, definition.booleanType, explicit, definition.booleanType);
            }
        } else if (actual.clazz == Byte.class) {
            if (expected.clazz == byte.class && internal) {
                return Cast.unboxFrom(definition.byteType, definition.byteType, explicit, definition.byteType);
            } else if (expected.clazz == short.class && internal) {
                return Cast.unboxFrom(definition.byteType, definition.shortType, explicit, definition.byteType);
            } else if (expected.clazz == char.class && explicit && internal) {
                return Cast.unboxFrom(definition.byteType, definition.charType, true, definition.byteType);
            } else if (expected.clazz == int.class && internal) {
                return Cast.unboxFrom(definition.byteType, definition.intType, explicit, definition.byteType);
            } else if (expected.clazz == long.class && internal) {
                return Cast.unboxFrom(definition.byteType, definition.longType, explicit, definition.byteType);
            } else if (expected.clazz == float.class && internal) {
                return Cast.unboxFrom(definition.byteType, definition.floatType, explicit, definition.byteType);
            } else if (expected.clazz == double.class && internal) {
                return Cast.unboxFrom(definition.byteType, definition.doubleType, explicit, definition.byteType);
            }
        } else if (actual.clazz == Short.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return Cast.unboxFrom(definition.shortType, definition.byteType, true, definition.shortType);
            } else if (expected.clazz == short.class && internal) {
                return Cast.unboxFrom(definition.shortType, definition.shortType, explicit, definition.shortType);
            } else if (expected.clazz == char.class && explicit && internal) {
                return Cast.unboxFrom(definition.shortType, definition.charType, true, definition.shortType);
            } else if (expected.clazz == int.class && internal) {
                return Cast.unboxFrom(definition.shortType, definition.intType, explicit, definition.shortType);
            } else if (expected.clazz == long.class && internal) {
                return Cast.unboxFrom(definition.shortType, definition.longType, explicit, definition.shortType);
            } else if (expected.clazz == float.class && internal) {
                return Cast.unboxFrom(definition.shortType, definition.floatType, explicit, definition.shortType);
            } else if (expected.clazz == double.class && internal) {
                return Cast.unboxFrom(definition.shortType, definition.doubleType, explicit, definition.shortType);
            }
        } else if (actual.clazz == Character.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return Cast.unboxFrom(definition.charType, definition.byteType, true, definition.charType);
            } else if (expected.clazz == short.class && explicit && internal) {
                return Cast.unboxFrom(definition.charType, definition.shortType, true, definition.charType);
            } else if (expected.clazz == char.class && internal) {
                return Cast.unboxFrom(definition.charType, definition.charType, explicit, definition.charType);
            } else if (expected.clazz == int.class && internal) {
                return Cast.unboxFrom(definition.charType, definition.intType, explicit, definition.charType);
            } else if (expected.clazz == long.class && internal) {
                return Cast.unboxFrom(definition.charType, definition.longType, explicit, definition.charType);
            } else if (expected.clazz == float.class && internal) {
                return Cast.unboxFrom(definition.charType, definition.floatType, explicit, definition.charType);
            } else if (expected.clazz == double.class && internal) {
                return Cast.unboxFrom(definition.charType, definition.doubleType, explicit, definition.charType);
            }
        } else if (actual.clazz == Integer.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return Cast.unboxFrom(definition.intType, definition.byteType, true, definition.intType);
            } else if (expected.clazz == short.class && explicit && internal) {
                return Cast.unboxFrom(definition.intType, definition.shortType, true, definition.intType);
            } else if (expected.clazz == char.class && explicit && internal) {
                return Cast.unboxFrom(definition.intType, definition.charType, true, definition.intType);
            } else if (expected.clazz == int.class && internal) {
                return Cast.unboxFrom(definition.intType, definition.intType, explicit, definition.intType);
            } else if (expected.clazz == long.class && internal) {
                return Cast.unboxFrom(definition.intType, definition.longType, explicit, definition.intType);
            } else if (expected.clazz == float.class && internal) {
                return Cast.unboxFrom(definition.intType, definition.floatType, explicit, definition.intType);
            } else if (expected.clazz == double.class && internal) {
                return Cast.unboxFrom(definition.intType, definition.doubleType, explicit, definition.intType);
            }
        } else if (actual.clazz == Long.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return Cast.unboxFrom(definition.longType, definition.byteType, true, definition.longType);
            } else if (expected.clazz == short.class && explicit && internal) {
                return Cast.unboxFrom(definition.longType, definition.shortType, true, definition.longType);
            } else if (expected.clazz == char.class && explicit && internal) {
                return Cast.unboxFrom(definition.longType, definition.charType, true, definition.longType);
            } else if (expected.clazz == int.class && explicit && internal) {
                return Cast.unboxFrom(definition.longType, definition.intType, true, definition.longType);
            } else if (expected.clazz == long.class && internal) {
                return Cast.unboxFrom(definition.longType, definition.longType, explicit, definition.longType);
            } else if (expected.clazz == float.class && internal) {
                return Cast.unboxFrom(definition.longType, definition.floatType, explicit, definition.longType);
            } else if (expected.clazz == double.class && internal) {
                return Cast.unboxFrom(definition.longType, definition.doubleType, explicit, definition.longType);
            }
        } else if (actual.clazz == Float.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return Cast.unboxFrom(definition.floatType, definition.byteType, true, definition.floatType);
            } else if (expected.clazz == short.class && explicit && internal) {
                return Cast.unboxFrom(definition.floatType, definition.shortType, true, definition.floatType);
            } else if (expected.clazz == char.class && explicit && internal) {
                return Cast.unboxFrom(definition.floatType, definition.charType, true, definition.floatType);
            } else if (expected.clazz == int.class && explicit && internal) {
                return Cast.unboxFrom(definition.floatType, definition.intType, true, definition.floatType);
            } else if (expected.clazz == long.class && explicit && internal) {
                return Cast.unboxFrom(definition.floatType, definition.longType, true, definition.floatType);
            } else if (expected.clazz == float.class && internal) {
                return Cast.unboxFrom(definition.floatType, definition.floatType, explicit, definition.floatType);
            } else if (expected.clazz == double.class && internal) {
                return Cast.unboxFrom(definition.floatType, definition.doubleType, explicit, definition.floatType);
            }
        } else if (actual.clazz == Double.class) {
            if (expected.clazz == byte.class && explicit && internal) {
                return Cast.unboxFrom(definition.doubleType, definition.byteType, true, definition.doubleType);
            } else if (expected.clazz == short.class && explicit && internal) {
                return Cast.unboxFrom(definition.doubleType, definition.shortType, true, definition.doubleType);
            } else if (expected.clazz == char.class && explicit && internal) {
                return Cast.unboxFrom(definition.doubleType, definition.charType, true, definition.doubleType);
            } else if (expected.clazz == int.class && explicit && internal) {
                return Cast.unboxFrom(definition.doubleType, definition.intType, true, definition.doubleType);
            } else if (expected.clazz == long.class && explicit && internal) {
                return Cast.unboxFrom(definition.doubleType, definition.longType, true, definition.doubleType);
            } else if (expected.clazz == float.class && explicit && internal) {
                return Cast.unboxFrom(definition.doubleType, definition.floatType, true, definition.doubleType);
            } else if (expected.clazz == double.class && internal) {
                return Cast.unboxFrom(definition.doubleType, definition.doubleType, explicit, definition.doubleType);
            }
        }

        if (    actual.dynamic                                   ||
                (actual.clazz != void.class && expected.dynamic) ||
                expected.clazz.isAssignableFrom(actual.clazz)    ||
                (actual.clazz.isAssignableFrom(expected.clazz) && explicit)) {
            return Cast.standard(actual, expected, explicit);
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
