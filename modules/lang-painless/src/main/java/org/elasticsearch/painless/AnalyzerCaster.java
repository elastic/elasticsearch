/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;

import java.util.Objects;

/**
 * Used during the analysis phase to collect legal type casts and promotions
 * for type-checking and later to write necessary casts in the bytecode.
 */
public final class AnalyzerCaster {

    public static PainlessCast getLegalCast(Location location, Class<?> actual, Class<?> expected, boolean explicit, boolean internal) {
        Objects.requireNonNull(actual);
        Objects.requireNonNull(expected);

        if (actual == expected) {
            return null;
        }

        if (actual == def.class) {
            if (expected == boolean.class) {
                return PainlessCast.originalTypetoTargetType(def.class, boolean.class, explicit);
            } else if (expected == byte.class) {
                return PainlessCast.originalTypetoTargetType(def.class, byte.class, explicit);
            } else if (expected == short.class) {
                return PainlessCast.originalTypetoTargetType(def.class, short.class, explicit);
            } else if (expected == char.class) {
                return PainlessCast.originalTypetoTargetType(def.class, char.class, explicit);
            } else if (expected == int.class) {
                return PainlessCast.originalTypetoTargetType(def.class, int.class, explicit);
            } else if (expected == long.class) {
                return PainlessCast.originalTypetoTargetType(def.class, long.class, explicit);
            } else if (expected == float.class) {
                return PainlessCast.originalTypetoTargetType(def.class, float.class, explicit);
            } else if (expected == double.class) {
                return PainlessCast.originalTypetoTargetType(def.class, double.class, explicit);
            } else if (expected == Boolean.class) {
                return PainlessCast.originalTypetoTargetType(def.class, Boolean.class, explicit);
            } else if (expected == Byte.class) {
                return PainlessCast.originalTypetoTargetType(def.class, Byte.class, explicit);
            } else if (expected == Short.class) {
                return PainlessCast.originalTypetoTargetType(def.class, Short.class, explicit);
            } else if (expected == Character.class) {
                return PainlessCast.originalTypetoTargetType(def.class, Character.class, explicit);
            } else if (expected == Integer.class) {
                return PainlessCast.originalTypetoTargetType(def.class, Integer.class, explicit);
            } else if (expected == Long.class) {
                return PainlessCast.originalTypetoTargetType(def.class, Long.class, explicit);
            } else if (expected == Float.class) {
                return PainlessCast.originalTypetoTargetType(def.class, Float.class, explicit);
            } else if (expected == Double.class) {
                return PainlessCast.originalTypetoTargetType(def.class, Double.class, explicit);
            }
        } else if (actual == String.class) {
            if (expected == char.class && explicit) {
                return PainlessCast.originalTypetoTargetType(String.class, char.class, true);
            }
        } else if (actual == boolean.class) {
            if (expected == def.class) {
                return PainlessCast.boxOriginalType(Boolean.class, def.class, explicit, boolean.class);
            } else if (expected == Object.class && internal) {
                return PainlessCast.boxOriginalType(Boolean.class, Object.class, explicit, boolean.class);
            } else if (expected == Boolean.class && internal) {
                return PainlessCast.boxTargetType(boolean.class, boolean.class, explicit, boolean.class);
            }
        } else if (actual == byte.class) {
            if (expected == def.class) {
                return PainlessCast.boxOriginalType(Byte.class, def.class, explicit, byte.class);
            } else if (expected == Object.class && internal) {
                return PainlessCast.boxOriginalType(Byte.class, Object.class, explicit, byte.class);
            } else if (expected == Number.class && internal) {
                return PainlessCast.boxOriginalType(Byte.class, Number.class, explicit, byte.class);
            } else if (expected == short.class) {
                return PainlessCast.originalTypetoTargetType(byte.class, short.class, explicit);
            } else if (expected == char.class && explicit) {
                return PainlessCast.originalTypetoTargetType(byte.class, char.class, true);
            } else if (expected == int.class) {
                return PainlessCast.originalTypetoTargetType(byte.class, int.class, explicit);
            } else if (expected == long.class) {
                return PainlessCast.originalTypetoTargetType(byte.class, long.class, explicit);
            } else if (expected == float.class) {
                return PainlessCast.originalTypetoTargetType(byte.class, float.class, explicit);
            } else if (expected == double.class) {
                return PainlessCast.originalTypetoTargetType(byte.class, double.class, explicit);
            } else if (expected == Byte.class && internal) {
                return PainlessCast.boxTargetType(byte.class, byte.class, explicit, byte.class);
            } else if (expected == Short.class && internal) {
                return PainlessCast.boxTargetType(byte.class, short.class, explicit, short.class);
            } else if (expected == Integer.class && internal) {
                return PainlessCast.boxTargetType(byte.class, int.class, explicit, int.class);
            } else if (expected == Long.class && internal) {
                return PainlessCast.boxTargetType(byte.class, long.class, explicit, long.class);
            } else if (expected == Float.class && internal) {
                return PainlessCast.boxTargetType(byte.class, float.class, explicit, float.class);
            } else if (expected == Double.class && internal) {
                return PainlessCast.boxTargetType(byte.class, double.class, explicit, double.class);
            }
        } else if (actual == short.class) {
            if (expected == def.class) {
                return PainlessCast.boxOriginalType(Short.class, def.class, explicit, short.class);
            } else if (expected == Object.class && internal) {
                return PainlessCast.boxOriginalType(Short.class, Object.class, explicit, short.class);
            } else if (expected == Number.class && internal) {
                return PainlessCast.boxOriginalType(Short.class, Number.class, explicit, short.class);
            } else if (expected == byte.class && explicit) {
                return PainlessCast.originalTypetoTargetType(short.class, byte.class, true);
            } else if (expected == char.class && explicit) {
                return PainlessCast.originalTypetoTargetType(short.class, char.class, true);
            } else if (expected == int.class) {
                return PainlessCast.originalTypetoTargetType(short.class, int.class, explicit);
            } else if (expected == long.class) {
                return PainlessCast.originalTypetoTargetType(short.class, long.class, explicit);
            } else if (expected == float.class) {
                return PainlessCast.originalTypetoTargetType(short.class, float.class, explicit);
            } else if (expected == double.class) {
                return PainlessCast.originalTypetoTargetType(short.class, double.class, explicit);
            } else if (expected == Short.class && internal) {
                return PainlessCast.boxTargetType(short.class, short.class, explicit, short.class);
            } else if (expected == Integer.class && internal) {
                return PainlessCast.boxTargetType(short.class, int.class, explicit, int.class);
            } else if (expected == Long.class && internal) {
                return PainlessCast.boxTargetType(short.class, long.class, explicit, long.class);
            } else if (expected == Float.class && internal) {
                return PainlessCast.boxTargetType(short.class, float.class, explicit, float.class);
            } else if (expected == Double.class && internal) {
                return PainlessCast.boxTargetType(short.class, double.class, explicit, double.class);
            }
        } else if (actual == char.class) {
            if (expected == def.class) {
                return PainlessCast.boxOriginalType(Character.class, def.class, explicit, char.class);
            } else if (expected == Object.class && internal) {
                return PainlessCast.boxOriginalType(Character.class, Object.class, explicit, char.class);
            } else if (expected == Number.class && internal) {
                return PainlessCast.boxOriginalType(Character.class, Number.class, explicit, char.class);
            } else if (expected == String.class && explicit) {
                return PainlessCast.originalTypetoTargetType(char.class, String.class, true);
            } else if (expected == byte.class && explicit) {
                return PainlessCast.originalTypetoTargetType(char.class, byte.class, true);
            } else if (expected == short.class && explicit) {
                return PainlessCast.originalTypetoTargetType(char.class, short.class, true);
            } else if (expected == int.class) {
                return PainlessCast.originalTypetoTargetType(char.class, int.class, explicit);
            } else if (expected == long.class) {
                return PainlessCast.originalTypetoTargetType(char.class, long.class, explicit);
            } else if (expected == float.class) {
                return PainlessCast.originalTypetoTargetType(char.class, float.class, explicit);
            } else if (expected == double.class) {
                return PainlessCast.originalTypetoTargetType(char.class, double.class, explicit);
            } else if (expected == Character.class && internal) {
                return PainlessCast.boxTargetType(char.class, char.class, true, char.class);
            } else if (expected == Integer.class && internal) {
                return PainlessCast.boxTargetType(char.class, int.class, explicit, int.class);
            } else if (expected == Long.class && internal) {
                return PainlessCast.boxTargetType(char.class, long.class, explicit, long.class);
            } else if (expected == Float.class && internal) {
                return PainlessCast.boxTargetType(char.class, float.class, explicit, float.class);
            } else if (expected == Double.class && internal) {
                return PainlessCast.boxTargetType(char.class, double.class, explicit, double.class);
            }
        } else if (actual == int.class) {
            if (expected == def.class) {
                return PainlessCast.boxOriginalType(Integer.class, def.class, explicit, int.class);
            } else if (expected == Object.class && internal) {
                return PainlessCast.boxOriginalType(Integer.class, Object.class, explicit, int.class);
            } else if (expected == Number.class && internal) {
                return PainlessCast.boxOriginalType(Integer.class, Number.class, explicit, int.class);
            } else if (expected == byte.class && explicit) {
                return PainlessCast.originalTypetoTargetType(int.class, byte.class, true);
            } else if (expected == char.class && explicit) {
                return PainlessCast.originalTypetoTargetType(int.class, char.class, true);
            } else if (expected == short.class && explicit) {
                return PainlessCast.originalTypetoTargetType(int.class, short.class, true);
            } else if (expected == long.class) {
                return PainlessCast.originalTypetoTargetType(int.class, long.class, explicit);
            } else if (expected == float.class) {
                return PainlessCast.originalTypetoTargetType(int.class, float.class, explicit);
            } else if (expected == double.class) {
                return PainlessCast.originalTypetoTargetType(int.class, double.class, explicit);
            } else if (expected == Integer.class && internal) {
                return PainlessCast.boxTargetType(int.class, int.class, explicit, int.class);
            } else if (expected == Long.class && internal) {
                return PainlessCast.boxTargetType(int.class, long.class, explicit, long.class);
            } else if (expected == Float.class && internal) {
                return PainlessCast.boxTargetType(int.class, float.class, explicit, float.class);
            } else if (expected == Double.class && internal) {
                return PainlessCast.boxTargetType(int.class, double.class, explicit, double.class);
            }
        } else if (actual == long.class) {
            if (expected == def.class) {
                return PainlessCast.boxOriginalType(Long.class, def.class, explicit, long.class);
            } else if (expected == Object.class && internal) {
                return PainlessCast.boxOriginalType(Long.class, Object.class, explicit, long.class);
            } else if (expected == Number.class && internal) {
                return PainlessCast.boxOriginalType(Long.class, Number.class, explicit, long.class);
            } else if (expected == byte.class && explicit) {
                return PainlessCast.originalTypetoTargetType(long.class, byte.class, true);
            } else if (expected == char.class && explicit) {
                return PainlessCast.originalTypetoTargetType(long.class, char.class, true);
            } else if (expected == short.class && explicit) {
                return PainlessCast.originalTypetoTargetType(long.class, short.class, true);
            } else if (expected == int.class && explicit) {
                return PainlessCast.originalTypetoTargetType(long.class, int.class, true);
            } else if (expected == float.class) {
                return PainlessCast.originalTypetoTargetType(long.class, float.class, explicit);
            } else if (expected == double.class) {
                return PainlessCast.originalTypetoTargetType(long.class, double.class, explicit);
            } else if (expected == Long.class && internal) {
                return PainlessCast.boxTargetType(long.class, long.class, explicit, long.class);
            } else if (expected == Float.class && internal) {
                return PainlessCast.boxTargetType(long.class, float.class, explicit, float.class);
            } else if (expected == Double.class && internal) {
                return PainlessCast.boxTargetType(long.class, double.class, explicit, double.class);
            }
        } else if (actual == float.class) {
            if (expected == def.class) {
                return PainlessCast.boxOriginalType(Float.class, def.class, explicit, float.class);
            } else if (expected == Object.class && internal) {
                return PainlessCast.boxOriginalType(Float.class, Object.class, explicit, float.class);
            } else if (expected == Number.class && internal) {
                return PainlessCast.boxOriginalType(Float.class, Number.class, explicit, float.class);
            } else if (expected == byte.class && explicit) {
                return PainlessCast.originalTypetoTargetType(float.class, byte.class, true);
            } else if (expected == char.class && explicit) {
                return PainlessCast.originalTypetoTargetType(float.class, char.class, true);
            } else if (expected == short.class && explicit) {
                return PainlessCast.originalTypetoTargetType(float.class, short.class, true);
            } else if (expected == int.class && explicit) {
                return PainlessCast.originalTypetoTargetType(float.class, int.class, true);
            } else if (expected == long.class && explicit) {
                return PainlessCast.originalTypetoTargetType(float.class, long.class, true);
            } else if (expected == double.class) {
                return PainlessCast.originalTypetoTargetType(float.class, double.class, explicit);
            } else if (expected == Float.class && internal) {
                return PainlessCast.boxTargetType(float.class, float.class, explicit, float.class);
            } else if (expected == Double.class && internal) {
                return PainlessCast.boxTargetType(float.class, double.class, explicit, double.class);
            }
        } else if (actual == double.class) {
            if (expected == def.class) {
                return PainlessCast.boxOriginalType(Double.class, def.class, explicit, double.class);
            } else if (expected == Object.class && internal) {
                return PainlessCast.boxOriginalType(Double.class, Object.class, explicit, double.class);
            } else if (expected == Number.class && internal) {
                return PainlessCast.boxOriginalType(Double.class, Number.class, explicit, double.class);
            } else if (expected == byte.class && explicit) {
                return PainlessCast.originalTypetoTargetType(double.class, byte.class, true);
            } else if (expected == char.class && explicit) {
                return PainlessCast.originalTypetoTargetType(double.class, char.class, true);
            } else if (expected == short.class && explicit) {
                return PainlessCast.originalTypetoTargetType(double.class, short.class, true);
            } else if (expected == int.class && explicit) {
                return PainlessCast.originalTypetoTargetType(double.class, int.class, true);
            } else if (expected == long.class && explicit) {
                return PainlessCast.originalTypetoTargetType(double.class, long.class, true);
            } else if (expected == float.class && explicit) {
                return PainlessCast.originalTypetoTargetType(double.class, float.class, true);
            } else if (expected == Double.class && internal) {
                return PainlessCast.boxTargetType(double.class, double.class, explicit, double.class);
            }
        } else if (actual == Boolean.class) {
            if (expected == boolean.class && internal) {
                return PainlessCast.unboxOriginalType(boolean.class, boolean.class, explicit, boolean.class);
            }
        } else if (actual == Byte.class) {
            if (expected == byte.class && internal) {
                return PainlessCast.unboxOriginalType(byte.class, byte.class, explicit, byte.class);
            } else if (expected == short.class && internal) {
                return PainlessCast.unboxOriginalType(byte.class, short.class, explicit, byte.class);
            } else if (expected == int.class && internal) {
                return PainlessCast.unboxOriginalType(byte.class, int.class, explicit, byte.class);
            } else if (expected == long.class && internal) {
                return PainlessCast.unboxOriginalType(byte.class, long.class, explicit, byte.class);
            } else if (expected == float.class && internal) {
                return PainlessCast.unboxOriginalType(byte.class, float.class, explicit, byte.class);
            } else if (expected == double.class && internal) {
                return PainlessCast.unboxOriginalType(byte.class, double.class, explicit, byte.class);
            } else if (expected == Short.class && internal) {
                return PainlessCast.unboxOriginalTypeToBoxTargetType(explicit, byte.class, short.class);
            } else if (expected == Integer.class && internal) {
                return PainlessCast.unboxOriginalTypeToBoxTargetType(explicit, byte.class, int.class);
            } else if (expected == Long.class && internal) {
                return PainlessCast.unboxOriginalTypeToBoxTargetType(explicit, byte.class, long.class);
            } else if (expected == Float.class && internal) {
                return PainlessCast.unboxOriginalTypeToBoxTargetType(explicit, byte.class, float.class);
            } else if (expected == Double.class && internal) {
                return PainlessCast.unboxOriginalTypeToBoxTargetType(explicit, byte.class, double.class);
            }
        } else if (actual == Short.class) {
            if (expected == short.class && internal) {
                return PainlessCast.unboxOriginalType(short.class, short.class, explicit, short.class);
            } else if (expected == int.class && internal) {
                return PainlessCast.unboxOriginalType(short.class, int.class, explicit, short.class);
            } else if (expected == long.class && internal) {
                return PainlessCast.unboxOriginalType(short.class, long.class, explicit, short.class);
            } else if (expected == float.class && internal) {
                return PainlessCast.unboxOriginalType(short.class, float.class, explicit, short.class);
            } else if (expected == double.class && internal) {
                return PainlessCast.unboxOriginalType(short.class, double.class, explicit, short.class);
            } else if (expected == Integer.class && internal) {
                return PainlessCast.unboxOriginalTypeToBoxTargetType(explicit, short.class, int.class);
            } else if (expected == Long.class && internal) {
                return PainlessCast.unboxOriginalTypeToBoxTargetType(explicit, short.class, long.class);
            } else if (expected == Float.class && internal) {
                return PainlessCast.unboxOriginalTypeToBoxTargetType(explicit, short.class, float.class);
            } else if (expected == Double.class && internal) {
                return PainlessCast.unboxOriginalTypeToBoxTargetType(explicit, short.class, double.class);
            }
        } else if (actual == Character.class) {
            if (expected == char.class && internal) {
                return PainlessCast.unboxOriginalType(char.class, char.class, explicit, char.class);
            } else if (expected == int.class && internal) {
                return PainlessCast.unboxOriginalType(char.class, int.class, explicit, char.class);
            } else if (expected == long.class && internal) {
                return PainlessCast.unboxOriginalType(char.class, long.class, explicit, char.class);
            } else if (expected == float.class && internal) {
                return PainlessCast.unboxOriginalType(char.class, float.class, explicit, char.class);
            } else if (expected == double.class && internal) {
                return PainlessCast.unboxOriginalType(char.class, double.class, explicit, char.class);
            } else if (expected == Integer.class && internal) {
                return PainlessCast.unboxOriginalTypeToBoxTargetType(explicit, char.class, int.class);
            } else if (expected == Long.class && internal) {
                return PainlessCast.unboxOriginalTypeToBoxTargetType(explicit, char.class, long.class);
            } else if (expected == Float.class && internal) {
                return PainlessCast.unboxOriginalTypeToBoxTargetType(explicit, char.class, float.class);
            } else if (expected == Double.class && internal) {
                return PainlessCast.unboxOriginalTypeToBoxTargetType(explicit, char.class, double.class);
            }
        } else if (actual == Integer.class) {
            if (expected == int.class && internal) {
                return PainlessCast.unboxOriginalType(int.class, int.class, explicit, int.class);
            } else if (expected == long.class && internal) {
                return PainlessCast.unboxOriginalType(int.class, long.class, explicit, int.class);
            } else if (expected == float.class && internal) {
                return PainlessCast.unboxOriginalType(int.class, float.class, explicit, int.class);
            } else if (expected == double.class && internal) {
                return PainlessCast.unboxOriginalType(int.class, double.class, explicit, int.class);
            } else if (expected == Long.class && internal) {
                return PainlessCast.unboxOriginalTypeToBoxTargetType(explicit, int.class, long.class);
            } else if (expected == Float.class && internal) {
                return PainlessCast.unboxOriginalTypeToBoxTargetType(explicit, int.class, float.class);
            } else if (expected == Double.class && internal) {
                return PainlessCast.unboxOriginalTypeToBoxTargetType(explicit, int.class, double.class);
            }
        } else if (actual == Long.class) {
            if (expected == long.class && internal) {
                return PainlessCast.unboxOriginalType(long.class, long.class, explicit, long.class);
            } else if (expected == float.class && internal) {
                return PainlessCast.unboxOriginalType(long.class, float.class, explicit, long.class);
            } else if (expected == double.class && internal) {
                return PainlessCast.unboxOriginalType(long.class, double.class, explicit, long.class);
            } else if (expected == Float.class && internal) {
                return PainlessCast.unboxOriginalTypeToBoxTargetType(explicit, long.class, float.class);
            } else if (expected == Double.class && internal) {
                return PainlessCast.unboxOriginalTypeToBoxTargetType(explicit, long.class, double.class);
            }
        } else if (actual == Float.class) {
            if (expected == float.class && internal) {
                return PainlessCast.unboxOriginalType(float.class, float.class, explicit, float.class);
            } else if (expected == double.class && internal) {
                return PainlessCast.unboxOriginalType(float.class, double.class, explicit, float.class);
            } else if (expected == Double.class && internal) {
                return PainlessCast.unboxOriginalTypeToBoxTargetType(explicit, float.class, double.class);
            }
        } else if (actual == Double.class) {
            if (expected == double.class && internal) {
                return PainlessCast.unboxOriginalType(double.class, double.class, explicit, double.class);
            }
        }

        if ((actual == def.class && expected != void.class)
            || (actual != void.class && expected == def.class)
            || expected.isAssignableFrom(actual)
            || (actual.isAssignableFrom(expected) && explicit)) {
            return PainlessCast.originalTypetoTargetType(actual, expected, explicit);
        } else {
            throw location.createError(
                new ClassCastException(
                    "Cannot cast from "
                        + "["
                        + PainlessLookupUtility.typeToCanonicalTypeName(actual)
                        + "] to "
                        + "["
                        + PainlessLookupUtility.typeToCanonicalTypeName(expected)
                        + "]."
                )
            );
        }
    }

    public static Object constCast(Location location, Object constant, PainlessCast cast) {
        Class<?> fsort = cast.originalType;
        Class<?> tsort = cast.targetType;

        if (fsort == tsort) {
            return constant;
        } else if (fsort == String.class && tsort == char.class) {
            return Utility.StringTochar((String) constant);
        } else if (fsort == char.class && tsort == String.class) {
            return Utility.charToString((char) constant);
        } else if (fsort.isPrimitive() && fsort != boolean.class && tsort.isPrimitive() && tsort != boolean.class) {
            Number number;

            if (fsort == char.class) {
                number = (int) (char) constant;
            } else {
                number = (Number) constant;
            }

            if (tsort == byte.class) return number.byteValue();
            else if (tsort == short.class) return number.shortValue();
            else if (tsort == char.class) return (char) number.intValue();
            else if (tsort == int.class) return number.intValue();
            else if (tsort == long.class) return number.longValue();
            else if (tsort == float.class) return number.floatValue();
            else if (tsort == double.class) return number.doubleValue();
            else {
                throw location.createError(
                    new IllegalStateException(
                        "Cannot cast from "
                            + "["
                            + cast.originalType.getCanonicalName()
                            + "] to ["
                            + cast.targetType.getCanonicalName()
                            + "]."
                    )
                );
            }
        } else {
            throw location.createError(
                new IllegalStateException(
                    "Cannot cast from " + "[" + cast.originalType.getCanonicalName() + "] to [" + cast.targetType.getCanonicalName() + "]."
                )
            );
        }
    }

    public static Class<?> promoteNumeric(Class<?> from, boolean decimal) {
        if (from == def.class || from == double.class && decimal || from == float.class && decimal || from == long.class) {
            return from;
        } else if (from == int.class || from == char.class || from == short.class || from == byte.class) {
            return int.class;
        }

        return null;
    }

    public static Class<?> promoteNumeric(Class<?> from0, Class<?> from1, boolean decimal) {
        if (from0 == def.class || from1 == def.class) {
            return def.class;
        }

        if (decimal) {
            if (from0 == double.class || from1 == double.class) {
                return double.class;
            } else if (from0 == float.class || from1 == float.class) {
                return float.class;
            }
        }

        if (from0 == long.class || from1 == long.class) {
            return long.class;
        } else if (from0 == int.class
            || from1 == int.class
            || from0 == char.class
            || from1 == char.class
            || from0 == short.class
            || from1 == short.class
            || from0 == byte.class
            || from1 == byte.class) {
                return int.class;
            }

        return null;
    }

    public static Class<?> promoteAdd(Class<?> from0, Class<?> from1) {
        if (from0 == String.class || from1 == String.class) {
            return String.class;
        }

        return promoteNumeric(from0, from1, true);
    }

    public static Class<?> promoteXor(Class<?> from0, Class<?> from1) {
        if (from0 == def.class || from1 == def.class) {
            return def.class;
        }

        if (from0 == boolean.class || from1 == boolean.class) {
            return boolean.class;
        }

        return promoteNumeric(from0, from1, false);
    }

    public static Class<?> promoteEquality(Class<?> from0, Class<?> from1) {
        if (from0 == String.class && from1 == String.class) {
            return String.class;
        }

        if (from0 == def.class || from1 == def.class) {
            return def.class;
        }

        if (from0.isPrimitive() && from1.isPrimitive()) {
            if (from0 == boolean.class && from1 == boolean.class) {
                return boolean.class;
            }

            return promoteNumeric(from0, from1, true);
        }

        return Object.class;
    }

    public static Class<?> promoteConditional(Class<?> from0, Class<?> from1) {
        if (from0 == from1) {
            return from0;
        }

        if (from0 == def.class || from1 == def.class) {
            return def.class;
        }

        if (from0.isPrimitive() && from0 != boolean.class && from1.isPrimitive() && from1 != boolean.class) {
            if (from0 == double.class || from1 == double.class) {
                return double.class;
            } else if (from0 == float.class || from1 == float.class) {
                return float.class;
            } else if (from0 == long.class || from1 == long.class) {
                return long.class;
            } else if (from0 == int.class || from1 == int.class) {
                return int.class;
            } else if (from0 == char.class) {
                if (from1 == short.class || from1 == byte.class) {
                    return int.class;
                } else {
                    return null;
                }
            } else if (from1 == char.class) {
                if (from0 == short.class || from0 == byte.class) {
                    return int.class;
                } else {
                    return null;
                }
            } else {
                return null;
            }
        }

        // TODO: In the rare case we still haven't reached a correct promotion we need
        // TODO: to calculate the highest upper bound for the two types and return that.
        // TODO: However, for now we just return objectType that may require an extra cast.

        return Object.class;
    }

    private AnalyzerCaster() {

    }
}
