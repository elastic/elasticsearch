/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Dynamic operators for painless.
 * <p>
 * Each operator must "support" the following types:
 * {@code int,long,float,double,boolean,Object}. Operators can throw exceptions if
 * the type is illegal. The {@code Object} type must be a "generic" handler that
 * handles all legal types: it must be convertible to every possible legal signature.
 */
@SuppressWarnings("unused")
public class DefMath {

    // Unary not: only applicable to integral types

    private static int not(int v) {
        return ~v;
    }

    private static long not(long v) {
        return ~v;
    }

    private static float not(float v) {
        throw new ClassCastException("Cannot apply not [~] to type [float]");
    }

    private static double not(double v) {
        throw new ClassCastException("Cannot apply not [~] to type [double]");
    }

    private static boolean not(boolean v) {
        throw new ClassCastException("Cannot apply not [~] to type [boolean]");
    }

    private static Object not(Object unary) {
        if (unary instanceof Long) {
            return ~(Long) unary;
        } else if (unary instanceof Integer) {
            return ~(Integer) unary;
        } else if (unary instanceof Short) {
            return ~(Short) unary;
        } else if (unary instanceof Character) {
            return ~(Character) unary;
        } else if (unary instanceof Byte) {
            return ~(Byte) unary;
        }

        throw new ClassCastException("Cannot apply [~] operation to type " + "[" + unary.getClass().getCanonicalName() + "].");
    }

    // unary negation and plus: applicable to all numeric types

    private static int neg(int v) {
        return -v;
    }

    private static long neg(long v) {
        return -v;
    }

    private static float neg(float v) {
        return -v;
    }

    private static double neg(double v) {
        return -v;
    }

    private static boolean neg(boolean v) {
        throw new ClassCastException("Cannot apply [-] operation to type [boolean]");
    }

    private static Object neg(final Object unary) {
        if (unary instanceof Double) {
            return -(double) unary;
        } else if (unary instanceof Long) {
            return -(long) unary;
        } else if (unary instanceof Integer) {
            return -(int) unary;
        } else if (unary instanceof Float) {
            return -(float) unary;
        } else if (unary instanceof Short) {
            return -(short) unary;
        } else if (unary instanceof Character) {
            return -(char) unary;
        } else if (unary instanceof Byte) {
            return -(byte) unary;
        }

        throw new ClassCastException("Cannot apply [-] operation to type " + "[" + unary.getClass().getCanonicalName() + "].");
    }

    private static int plus(int v) {
        return +v;
    }

    private static long plus(long v) {
        return +v;
    }

    private static float plus(float v) {
        return +v;
    }

    private static double plus(double v) {
        return +v;
    }

    private static boolean plus(boolean v) {
        throw new ClassCastException("Cannot apply [+] operation to type [boolean]");
    }

    private static Object plus(final Object unary) {
        if (unary instanceof Double) {
            return +(double) unary;
        } else if (unary instanceof Long) {
            return +(long) unary;
        } else if (unary instanceof Integer) {
            return +(int) unary;
        } else if (unary instanceof Float) {
            return +(float) unary;
        } else if (unary instanceof Short) {
            return +(short) unary;
        } else if (unary instanceof Character) {
            return +(char) unary;
        } else if (unary instanceof Byte) {
            return +(byte) unary;
        }

        throw new ClassCastException("Cannot apply [+] operation to type " + "[" + unary.getClass().getCanonicalName() + "].");
    }

    // multiplication/division/remainder/subtraction: applicable to all integer types

    private static int mul(int a, int b) {
        return a * b;
    }

    private static long mul(long a, long b) {
        return a * b;
    }

    private static float mul(float a, float b) {
        return a * b;
    }

    private static double mul(double a, double b) {
        return a * b;
    }

    private static boolean mul(boolean a, boolean b) {
        throw new ClassCastException("Cannot apply [*] operation to type [boolean]");
    }

    private static Object mul(Object left, Object right) {
        if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double) {
                    return ((Number) left).doubleValue() * ((Number) right).doubleValue();
                } else if (left instanceof Float || right instanceof Float) {
                    return ((Number) left).floatValue() * ((Number) right).floatValue();
                } else if (left instanceof Long || right instanceof Long) {
                    return ((Number) left).longValue() * ((Number) right).longValue();
                } else {
                    return ((Number) left).intValue() * ((Number) right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double) {
                    return ((Number) left).doubleValue() * (char) right;
                } else if (left instanceof Long) {
                    return ((Number) left).longValue() * (char) right;
                } else if (left instanceof Float) {
                    return ((Number) left).floatValue() * (char) right;
                } else {
                    return ((Number) left).intValue() * (char) right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (char) left * ((Number) right).doubleValue();
                } else if (right instanceof Long) {
                    return (char) left * ((Number) right).longValue();
                } else if (right instanceof Float) {
                    return (char) left * ((Number) right).floatValue();
                } else {
                    return (char) left * ((Number) right).intValue();
                }
            } else if (right instanceof Character) {
                return (char) left * (char) right;
            }
        }

        throw new ClassCastException(
            "Cannot apply [*] operation to types "
                + "["
                + left.getClass().getCanonicalName()
                + "] and ["
                + right.getClass().getCanonicalName()
                + "]."
        );
    }

    private static int div(int a, int b) {
        return a / b;
    }

    private static long div(long a, long b) {
        return a / b;
    }

    private static float div(float a, float b) {
        return a / b;
    }

    private static double div(double a, double b) {
        return a / b;
    }

    private static boolean div(boolean a, boolean b) {
        throw new ClassCastException("Cannot apply [/] operation to type [boolean]");
    }

    private static Object div(Object left, Object right) {
        if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double) {
                    return ((Number) left).doubleValue() / ((Number) right).doubleValue();
                } else if (left instanceof Float || right instanceof Float) {
                    return ((Number) left).floatValue() / ((Number) right).floatValue();
                } else if (left instanceof Long || right instanceof Long) {
                    return ((Number) left).longValue() / ((Number) right).longValue();
                } else {
                    return ((Number) left).intValue() / ((Number) right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double) {
                    return ((Number) left).doubleValue() / (char) right;
                } else if (left instanceof Long) {
                    return ((Number) left).longValue() / (char) right;
                } else if (left instanceof Float) {
                    return ((Number) left).floatValue() / (char) right;
                } else {
                    return ((Number) left).intValue() / (char) right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (char) left / ((Number) right).doubleValue();
                } else if (right instanceof Long) {
                    return (char) left / ((Number) right).longValue();
                } else if (right instanceof Float) {
                    return (char) left / ((Number) right).floatValue();
                } else {
                    return (char) left / ((Number) right).intValue();
                }
            } else if (right instanceof Character) {
                return (char) left / (char) right;
            }
        }

        throw new ClassCastException(
            "Cannot apply [/] operation to types "
                + "["
                + left.getClass().getCanonicalName()
                + "] and ["
                + right.getClass().getCanonicalName()
                + "]."
        );
    }

    private static int rem(int a, int b) {
        return a % b;
    }

    private static long rem(long a, long b) {
        return a % b;
    }

    private static float rem(float a, float b) {
        return a % b;
    }

    private static double rem(double a, double b) {
        return a % b;
    }

    private static boolean rem(boolean a, boolean b) {
        throw new ClassCastException("Cannot apply [%] operation to type [boolean]");
    }

    private static Object rem(Object left, Object right) {
        if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double) {
                    return ((Number) left).doubleValue() % ((Number) right).doubleValue();
                } else if (left instanceof Float || right instanceof Float) {
                    return ((Number) left).floatValue() % ((Number) right).floatValue();
                } else if (left instanceof Long || right instanceof Long) {
                    return ((Number) left).longValue() % ((Number) right).longValue();
                } else {
                    return ((Number) left).intValue() % ((Number) right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double) {
                    return ((Number) left).doubleValue() % (char) right;
                } else if (left instanceof Long) {
                    return ((Number) left).longValue() % (char) right;
                } else if (left instanceof Float) {
                    return ((Number) left).floatValue() % (char) right;
                } else {
                    return ((Number) left).intValue() % (char) right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (char) left % ((Number) right).doubleValue();
                } else if (right instanceof Long) {
                    return (char) left % ((Number) right).longValue();
                } else if (right instanceof Float) {
                    return (char) left % ((Number) right).floatValue();
                } else {
                    return (char) left % ((Number) right).intValue();
                }
            } else if (right instanceof Character) {
                return (char) left % (char) right;
            }
        }

        throw new ClassCastException(
            "Cannot apply [%] operation to types "
                + "["
                + left.getClass().getCanonicalName()
                + "] and ["
                + right.getClass().getCanonicalName()
                + "]."
        );
    }

    // addition: applicable to all numeric types.
    // additionally, if either type is a string, the other type can be any arbitrary type (including null)

    private static int add(int a, int b) {
        return a + b;
    }

    private static long add(long a, long b) {
        return a + b;
    }

    private static float add(float a, float b) {
        return a + b;
    }

    private static double add(double a, double b) {
        return a + b;
    }

    private static boolean add(boolean a, boolean b) {
        throw new ClassCastException("Cannot apply [+] operation to type [boolean]");
    }

    private static Object add(Object left, Object right) {
        if (left instanceof String) {
            return (String) left + right;
        } else if (right instanceof String) {
            return left + (String) right;
        } else if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double) {
                    return ((Number) left).doubleValue() + ((Number) right).doubleValue();
                } else if (left instanceof Float || right instanceof Float) {
                    return ((Number) left).floatValue() + ((Number) right).floatValue();
                } else if (left instanceof Long || right instanceof Long) {
                    return ((Number) left).longValue() + ((Number) right).longValue();
                } else {
                    return ((Number) left).intValue() + ((Number) right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double) {
                    return ((Number) left).doubleValue() + (char) right;
                } else if (left instanceof Long) {
                    return ((Number) left).longValue() + (char) right;
                } else if (left instanceof Float) {
                    return ((Number) left).floatValue() + (char) right;
                } else {
                    return ((Number) left).intValue() + (char) right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (char) left + ((Number) right).doubleValue();
                } else if (right instanceof Long) {
                    return (char) left + ((Number) right).longValue();
                } else if (right instanceof Float) {
                    return (char) left + ((Number) right).floatValue();
                } else {
                    return (char) left + ((Number) right).intValue();
                }
            } else if (right instanceof Character) {
                return (char) left + (char) right;
            }
        }

        throw new ClassCastException(
            "Cannot apply [+] operation to types "
                + "["
                + left.getClass().getCanonicalName()
                + "] and ["
                + right.getClass().getCanonicalName()
                + "]."
        );
    }

    private static int sub(int a, int b) {
        return a - b;
    }

    private static long sub(long a, long b) {
        return a - b;
    }

    private static float sub(float a, float b) {
        return a - b;
    }

    private static double sub(double a, double b) {
        return a - b;
    }

    private static boolean sub(boolean a, boolean b) {
        throw new ClassCastException("Cannot apply [-] operation to type [boolean]");
    }

    private static Object sub(Object left, Object right) {
        if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double) {
                    return ((Number) left).doubleValue() - ((Number) right).doubleValue();
                } else if (left instanceof Float || right instanceof Float) {
                    return ((Number) left).floatValue() - ((Number) right).floatValue();
                } else if (left instanceof Long || right instanceof Long) {
                    return ((Number) left).longValue() - ((Number) right).longValue();
                } else {
                    return ((Number) left).intValue() - ((Number) right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double) {
                    return ((Number) left).doubleValue() - (char) right;
                } else if (left instanceof Long) {
                    return ((Number) left).longValue() - (char) right;
                } else if (left instanceof Float) {
                    return ((Number) left).floatValue() - (char) right;
                } else {
                    return ((Number) left).intValue() - (char) right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (char) left - ((Number) right).doubleValue();
                } else if (right instanceof Long) {
                    return (char) left - ((Number) right).longValue();
                } else if (right instanceof Float) {
                    return (char) left - ((Number) right).floatValue();
                } else {
                    return (char) left - ((Number) right).intValue();
                }
            } else if (right instanceof Character) {
                return (char) left - (char) right;
            }
        }

        throw new ClassCastException(
            "Cannot apply [-] operation to types "
                + "["
                + left.getClass().getCanonicalName()
                + "] and ["
                + right.getClass().getCanonicalName()
                + "]."
        );
    }

    // eq: applicable to any arbitrary type, including nulls for both arguments!!!

    private static boolean eq(int a, int b) {
        return a == b;
    }

    private static boolean eq(long a, long b) {
        return a == b;
    }

    private static boolean eq(float a, float b) {
        return a == b;
    }

    private static boolean eq(double a, double b) {
        return a == b;
    }

    private static boolean eq(boolean a, boolean b) {
        return a == b;
    }

    private static boolean eq(Object left, Object right) {
        if (left != null && right != null) {
            if (left instanceof Double) {
                if (right instanceof Number) {
                    return (double) left == ((Number) right).doubleValue();
                } else if (right instanceof Character) {
                    return (double) left == (char) right;
                }
            } else if (right instanceof Double) {
                if (left instanceof Number) {
                    return ((Number) left).doubleValue() == (double) right;
                } else if (left instanceof Character) {
                    return (char) left == ((Number) right).doubleValue();
                }
            } else if (left instanceof Float) {
                if (right instanceof Number) {
                    return (float) left == ((Number) right).floatValue();
                } else if (right instanceof Character) {
                    return (float) left == (char) right;
                }
            } else if (right instanceof Float) {
                if (left instanceof Number) {
                    return ((Number) left).floatValue() == (float) right;
                } else if (left instanceof Character) {
                    return (char) left == ((Number) right).floatValue();
                }
            } else if (left instanceof Long) {
                if (right instanceof Number) {
                    return (long) left == ((Number) right).longValue();
                } else if (right instanceof Character) {
                    return (long) left == (char) right;
                }
            } else if (right instanceof Long) {
                if (left instanceof Number) {
                    return ((Number) left).longValue() == (long) right;
                } else if (left instanceof Character) {
                    return (char) left == ((Number) right).longValue();
                }
            } else if (left instanceof Number) {
                if (right instanceof Number) {
                    return ((Number) left).intValue() == ((Number) right).intValue();
                } else if (right instanceof Character) {
                    return ((Number) left).intValue() == (char) right;
                }
            } else if (right instanceof Number && left instanceof Character) {
                return (char) left == ((Number) right).intValue();
            } else if (left instanceof Character && right instanceof Character) {
                return (char) left == (char) right;
            }

            return left.equals(right);
        }

        return left == null && right == null;
    }

    // comparison operators: applicable for any numeric type

    private static boolean lt(int a, int b) {
        return a < b;
    }

    private static boolean lt(long a, long b) {
        return a < b;
    }

    private static boolean lt(float a, float b) {
        return a < b;
    }

    private static boolean lt(double a, double b) {
        return a < b;
    }

    private static boolean lt(boolean a, boolean b) {
        throw new ClassCastException("Cannot apply [<] operation to type [boolean]");
    }

    private static boolean lt(Object left, Object right) {
        if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double) {
                    return ((Number) left).doubleValue() < ((Number) right).doubleValue();
                } else if (left instanceof Float || right instanceof Float) {
                    return ((Number) left).floatValue() < ((Number) right).floatValue();
                } else if (left instanceof Long || right instanceof Long) {
                    return ((Number) left).longValue() < ((Number) right).longValue();
                } else {
                    return ((Number) left).intValue() < ((Number) right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double) {
                    return ((Number) left).doubleValue() < (char) right;
                } else if (left instanceof Long) {
                    return ((Number) left).longValue() < (char) right;
                } else if (left instanceof Float) {
                    return ((Number) left).floatValue() < (char) right;
                } else {
                    return ((Number) left).intValue() < (char) right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (char) left < ((Number) right).doubleValue();
                } else if (right instanceof Long) {
                    return (char) left < ((Number) right).longValue();
                } else if (right instanceof Float) {
                    return (char) left < ((Number) right).floatValue();
                } else {
                    return (char) left < ((Number) right).intValue();
                }
            } else if (right instanceof Character) {
                return (char) left < (char) right;
            }
        }

        throw new ClassCastException(
            "Cannot apply [<] operation to types "
                + "["
                + left.getClass().getCanonicalName()
                + "] and ["
                + right.getClass().getCanonicalName()
                + "]."
        );
    }

    private static boolean lte(int a, int b) {
        return a <= b;
    }

    private static boolean lte(long a, long b) {
        return a <= b;
    }

    private static boolean lte(float a, float b) {
        return a <= b;
    }

    private static boolean lte(double a, double b) {
        return a <= b;
    }

    private static boolean lte(boolean a, boolean b) {
        throw new ClassCastException("Cannot apply [<=] operation to type [boolean]");
    }

    private static boolean lte(Object left, Object right) {
        if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double) {
                    return ((Number) left).doubleValue() <= ((Number) right).doubleValue();
                } else if (left instanceof Float || right instanceof Float) {
                    return ((Number) left).floatValue() <= ((Number) right).floatValue();
                } else if (left instanceof Long || right instanceof Long) {
                    return ((Number) left).longValue() <= ((Number) right).longValue();
                } else {
                    return ((Number) left).intValue() <= ((Number) right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double) {
                    return ((Number) left).doubleValue() <= (char) right;
                } else if (left instanceof Long) {
                    return ((Number) left).longValue() <= (char) right;
                } else if (left instanceof Float) {
                    return ((Number) left).floatValue() <= (char) right;
                } else {
                    return ((Number) left).intValue() <= (char) right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (char) left <= ((Number) right).doubleValue();
                } else if (right instanceof Long) {
                    return (char) left <= ((Number) right).longValue();
                } else if (right instanceof Float) {
                    return (char) left <= ((Number) right).floatValue();
                } else {
                    return (char) left <= ((Number) right).intValue();
                }
            } else if (right instanceof Character) {
                return (char) left <= (char) right;
            }
        }

        throw new ClassCastException(
            "Cannot apply [<=] operation to types "
                + "["
                + left.getClass().getCanonicalName()
                + "] and ["
                + right.getClass().getCanonicalName()
                + "]."
        );
    }

    private static boolean gt(int a, int b) {
        return a > b;
    }

    private static boolean gt(long a, long b) {
        return a > b;
    }

    private static boolean gt(float a, float b) {
        return a > b;
    }

    private static boolean gt(double a, double b) {
        return a > b;
    }

    private static boolean gt(boolean a, boolean b) {
        throw new ClassCastException("Cannot apply [>] operation to type [boolean]");
    }

    private static boolean gt(Object left, Object right) {
        if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double) {
                    return ((Number) left).doubleValue() > ((Number) right).doubleValue();
                } else if (left instanceof Float || right instanceof Float) {
                    return ((Number) left).floatValue() > ((Number) right).floatValue();
                } else if (left instanceof Long || right instanceof Long) {
                    return ((Number) left).longValue() > ((Number) right).longValue();
                } else {
                    return ((Number) left).intValue() > ((Number) right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double) {
                    return ((Number) left).doubleValue() > (char) right;
                } else if (left instanceof Long) {
                    return ((Number) left).longValue() > (char) right;
                } else if (left instanceof Float) {
                    return ((Number) left).floatValue() > (char) right;
                } else {
                    return ((Number) left).intValue() > (char) right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (char) left > ((Number) right).doubleValue();
                } else if (right instanceof Long) {
                    return (char) left > ((Number) right).longValue();
                } else if (right instanceof Float) {
                    return (char) left > ((Number) right).floatValue();
                } else {
                    return (char) left > ((Number) right).intValue();
                }
            } else if (right instanceof Character) {
                return (char) left > (char) right;
            }
        }

        throw new ClassCastException(
            "Cannot apply [>] operation to types "
                + "["
                + left.getClass().getCanonicalName()
                + "] and ["
                + right.getClass().getCanonicalName()
                + "]."
        );
    }

    private static boolean gte(int a, int b) {
        return a >= b;
    }

    private static boolean gte(long a, long b) {
        return a >= b;
    }

    private static boolean gte(float a, float b) {
        return a >= b;
    }

    private static boolean gte(double a, double b) {
        return a >= b;
    }

    private static boolean gte(boolean a, boolean b) {
        throw new ClassCastException("Cannot apply [>=] operation to type [boolean]");
    }

    private static boolean gte(Object left, Object right) {
        if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double) {
                    return ((Number) left).doubleValue() >= ((Number) right).doubleValue();
                } else if (left instanceof Float || right instanceof Float) {
                    return ((Number) left).floatValue() >= ((Number) right).floatValue();
                } else if (left instanceof Long || right instanceof Long) {
                    return ((Number) left).longValue() >= ((Number) right).longValue();
                } else {
                    return ((Number) left).intValue() >= ((Number) right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double) {
                    return ((Number) left).doubleValue() >= (char) right;
                } else if (left instanceof Long) {
                    return ((Number) left).longValue() >= (char) right;
                } else if (left instanceof Float) {
                    return ((Number) left).floatValue() >= (char) right;
                } else {
                    return ((Number) left).intValue() >= (char) right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (char) left >= ((Number) right).doubleValue();
                } else if (right instanceof Long) {
                    return (char) left >= ((Number) right).longValue();
                } else if (right instanceof Float) {
                    return (char) left >= ((Number) right).floatValue();
                } else {
                    return (char) left >= ((Number) right).intValue();
                }
            } else if (right instanceof Character) {
                return (char) left >= (char) right;
            }
        }

        throw new ClassCastException(
            "Cannot apply [>] operation to types "
                + "["
                + left.getClass().getCanonicalName()
                + "] and ["
                + right.getClass().getCanonicalName()
                + "]."
        );
    }

    // helper methods to convert an integral according to numeric promotion
    // this is used by the generic code for bitwise and shift operators

    private static long longIntegralValue(Object o) {
        if (o instanceof Long) {
            return (long) o;
        } else if (o instanceof Integer || o instanceof Short || o instanceof Byte) {
            return ((Number) o).longValue();
        } else if (o instanceof Character) {
            return (char) o;
        } else {
            throw new ClassCastException("Cannot convert [" + o.getClass().getCanonicalName() + "] to an integral value.");
        }
    }

    private static int intIntegralValue(Object o) {
        if (o instanceof Integer || o instanceof Short || o instanceof Byte) {
            return ((Number) o).intValue();
        } else if (o instanceof Character) {
            return (char) o;
        } else {
            throw new ClassCastException("Cannot convert [" + o.getClass().getCanonicalName() + "] to an integral value.");
        }
    }

    // bitwise operators: valid only for integral types

    private static int and(int a, int b) {
        return a & b;
    }

    private static long and(long a, long b) {
        return a & b;
    }

    private static float and(float a, float b) {
        throw new ClassCastException("Cannot apply [&] operation to type [float]");
    }

    private static double and(double a, double b) {
        throw new ClassCastException("Cannot apply [&] operation to type [float]");
    }

    private static boolean and(boolean a, boolean b) {
        return a & b;
    }

    private static Object and(Object left, Object right) {
        if (left instanceof Boolean && right instanceof Boolean) {
            return (boolean) left & (boolean) right;
        } else if (left instanceof Long || right instanceof Long) {
            return longIntegralValue(left) & longIntegralValue(right);
        } else {
            return intIntegralValue(left) & intIntegralValue(right);
        }
    }

    private static int xor(int a, int b) {
        return a ^ b;
    }

    private static long xor(long a, long b) {
        return a ^ b;
    }

    private static float xor(float a, float b) {
        throw new ClassCastException("Cannot apply [^] operation to type [float]");
    }

    private static double xor(double a, double b) {
        throw new ClassCastException("Cannot apply [^] operation to type [float]");
    }

    private static boolean xor(boolean a, boolean b) {
        return a ^ b;
    }

    private static Object xor(Object left, Object right) {
        if (left instanceof Boolean && right instanceof Boolean) {
            return (boolean) left ^ (boolean) right;
        } else if (left instanceof Long || right instanceof Long) {
            return longIntegralValue(left) ^ longIntegralValue(right);
        } else {
            return intIntegralValue(left) ^ intIntegralValue(right);
        }
    }

    private static int or(int a, int b) {
        return a | b;
    }

    private static long or(long a, long b) {
        return a | b;
    }

    private static float or(float a, float b) {
        throw new ClassCastException("Cannot apply [|] operation to type [float]");
    }

    private static double or(double a, double b) {
        throw new ClassCastException("Cannot apply [|] operation to type [float]");
    }

    private static boolean or(boolean a, boolean b) {
        return a | b;
    }

    private static Object or(Object left, Object right) {
        if (left instanceof Boolean && right instanceof Boolean) {
            return (boolean) left | (boolean) right;
        } else if (left instanceof Long || right instanceof Long) {
            return longIntegralValue(left) | longIntegralValue(right);
        } else {
            return intIntegralValue(left) | intIntegralValue(right);
        }
    }

    // shift operators, valid for any integral types, but does not promote.
    // we implement all shifts as long shifts, because the extra bits are ignored anyway.

    private static int lsh(int a, long b) {
        return a << b;
    }

    private static long lsh(long a, long b) {
        return a << b;
    }

    private static float lsh(float a, long b) {
        throw new ClassCastException("Cannot apply [<<] operation to type [float]");
    }

    private static double lsh(double a, long b) {
        throw new ClassCastException("Cannot apply [<<] operation to type [double]");
    }

    private static boolean lsh(boolean a, long b) {
        throw new ClassCastException("Cannot apply [<<] operation to type [boolean]");
    }

    public static Object lsh(Object left, long right) {
        if (left instanceof Long) {
            return (long) (left) << right;
        } else {
            return intIntegralValue(left) << right;
        }
    }

    private static int rsh(int a, long b) {
        return a >> b;
    }

    private static long rsh(long a, long b) {
        return a >> b;
    }

    private static float rsh(float a, long b) {
        throw new ClassCastException("Cannot apply [>>] operation to type [float]");
    }

    private static double rsh(double a, long b) {
        throw new ClassCastException("Cannot apply [>>] operation to type [double]");
    }

    private static boolean rsh(boolean a, long b) {
        throw new ClassCastException("Cannot apply [>>] operation to type [boolean]");
    }

    public static Object rsh(Object left, long right) {
        if (left instanceof Long) {
            return (long) left >> right;
        } else {
            return intIntegralValue(left) >> right;
        }
    }

    private static int ush(int a, long b) {
        return a >>> b;
    }

    private static long ush(long a, long b) {
        return a >>> b;
    }

    private static float ush(float a, long b) {
        throw new ClassCastException("Cannot apply [>>>] operation to type [float]");
    }

    private static double ush(double a, long b) {
        throw new ClassCastException("Cannot apply [>>>] operation to type [double]");
    }

    private static boolean ush(boolean a, long b) {
        throw new ClassCastException("Cannot apply [>>>] operation to type [boolean]");
    }

    public static Object ush(Object left, long right) {
        if (left instanceof Long) {
            return (long) (left) >>> right;
        } else {
            return intIntegralValue(left) >>> right;
        }
    }

    /**
     * unboxes a class to its primitive type, or returns the original
     * class if its not a boxed type.
     */
    private static Class<?> unbox(Class<?> clazz) {
        return MethodType.methodType(clazz).unwrap().returnType();
    }

    /** Unary promotion. All Objects are promoted to Object. */
    private static Class<?> promote(Class<?> clazz) {
        // if either is a non-primitive type -> Object.
        if (clazz.isPrimitive() == false) {
            return Object.class;
        }
        // always promoted to integer
        if (clazz == byte.class || clazz == short.class || clazz == char.class || clazz == int.class) {
            return int.class;
        } else {
            return clazz;
        }
    }

    /** Binary promotion. */
    private static Class<?> promote(Class<?> a, Class<?> b) {
        // if either is a non-primitive type -> Object.
        if (a.isPrimitive() == false || b.isPrimitive() == false) {
            return Object.class;
        }

        // boolean -> boolean
        if (a == boolean.class && b == boolean.class) {
            return boolean.class;
        }

        // ordinary numeric promotion
        if (a == double.class || b == double.class) {
            return double.class;
        } else if (a == float.class || b == float.class) {
            return float.class;
        } else if (a == long.class || b == long.class) {
            return long.class;
        } else {
            return int.class;
        }
    }

    private static final MethodHandles.Lookup PRIVATE_METHOD_HANDLES_LOOKUP = MethodHandles.lookup();

    private static final Map<Class<?>, Map<String, MethodHandle>> TYPE_OP_MAPPING = Collections.unmodifiableMap(
        Stream.of(boolean.class, int.class, long.class, float.class, double.class, Object.class)
            .collect(Collectors.toMap(Function.identity(), type -> {
                try {
                    Map<String, MethodHandle> map = new HashMap<>();
                    MethodType unary = MethodType.methodType(type, type);
                    MethodType binary = MethodType.methodType(type, type, type);
                    MethodType comparison = MethodType.methodType(boolean.class, type, type);
                    MethodType shift = MethodType.methodType(type, type, long.class);
                    Class<?> clazz = PRIVATE_METHOD_HANDLES_LOOKUP.lookupClass();
                    map.put("not", PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(clazz, "not", unary));
                    map.put("neg", PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(clazz, "neg", unary));
                    map.put("plus", PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(clazz, "plus", unary));
                    map.put("mul", PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(clazz, "mul", binary));
                    map.put("div", PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(clazz, "div", binary));
                    map.put("rem", PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(clazz, "rem", binary));
                    map.put("add", PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(clazz, "add", binary));
                    map.put("sub", PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(clazz, "sub", binary));
                    map.put("and", PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(clazz, "and", binary));
                    map.put("or", PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(clazz, "or", binary));
                    map.put("xor", PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(clazz, "xor", binary));
                    map.put("eq", PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(clazz, "eq", comparison));
                    map.put("lt", PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(clazz, "lt", comparison));
                    map.put("lte", PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(clazz, "lte", comparison));
                    map.put("gt", PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(clazz, "gt", comparison));
                    map.put("gte", PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(clazz, "gte", comparison));
                    map.put("lsh", PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(clazz, "lsh", shift));
                    map.put("rsh", PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(clazz, "rsh", shift));
                    map.put("ush", PRIVATE_METHOD_HANDLES_LOOKUP.findStatic(clazz, "ush", shift));
                    return map;
                } catch (ReflectiveOperationException e) {
                    throw new AssertionError(e);
                }
            }))
    );

    /** Returns an appropriate method handle for a unary or shift operator, based only on the receiver (LHS) */
    public static MethodHandle lookupUnary(Class<?> receiverClass, String name) {
        MethodHandle handle = TYPE_OP_MAPPING.get(promote(unbox(receiverClass))).get(name);
        if (handle == null) {
            throw new ClassCastException("Cannot apply operator [" + name + "] to type [" + receiverClass + "]");
        }
        return handle;
    }

    /** Returns an appropriate method handle for a binary operator, based on promotion of the LHS and RHS arguments */
    public static MethodHandle lookupBinary(Class<?> classA, Class<?> classB, String name) {
        MethodHandle handle = TYPE_OP_MAPPING.get(promote(promote(unbox(classA)), promote(unbox(classB)))).get(name);
        if (handle == null) {
            throw new ClassCastException("Cannot apply operator [" + name + "] to types [" + classA + "] and [" + classB + "]");
        }
        return handle;
    }

    /** Returns a generic method handle for any operator, that can handle all valid signatures, nulls, corner cases */
    public static MethodHandle lookupGeneric(String name) {
        return TYPE_OP_MAPPING.get(Object.class).get(name);
    }

    /**
     * Slow dynamic cast: casts {@code returnValue} to the runtime type of {@code lhs}
     * based upon inspection. If {@code lhs} is null, no cast takes place.
     * This is used for the generic fallback case of compound assignment.
     */
    static Object dynamicReceiverCast(Object returnValue, Object lhs) {
        if (lhs != null) {
            return dynamicCast(lhs.getClass(), returnValue);
        } else {
            return returnValue;
        }
    }

    /**
     * Slow dynamic cast: casts {@code value} to an instance of {@code clazz}
     * based upon inspection. If {@code lhs} is null, no cast takes place.
     */
    static Object dynamicCast(Class<?> clazz, Object value) {
        if (value != null) {
            if (clazz == value.getClass()) {
                return value;
            }
            if (clazz == Integer.class) {
                return getNumber(value).intValue();
            } else if (clazz == Long.class) {
                return getNumber(value).longValue();
            } else if (clazz == Double.class) {
                return getNumber(value).doubleValue();
            } else if (clazz == Float.class) {
                return getNumber(value).floatValue();
            } else if (clazz == Short.class) {
                return getNumber(value).shortValue();
            } else if (clazz == Byte.class) {
                return getNumber(value).byteValue();
            } else if (clazz == Character.class) {
                return (char) getNumber(value).intValue();
            }
            return clazz.cast(value);
        } else {
            return value;
        }
    }

    /** Slowly returns a Number for o. Just for supporting dynamicCast */
    static Number getNumber(Object o) {
        if (o instanceof Number) {
            return (Number) o;
        } else if (o instanceof Character) {
            return Integer.valueOf((char) o);
        } else {
            throw new ClassCastException("Cannot convert [" + o.getClass() + "] to a Number");
        }
    }

    private static final MethodHandle DYNAMIC_CAST;
    private static final MethodHandle DYNAMIC_RECEIVER_CAST;
    static {
        final MethodHandles.Lookup methodHandlesLookup = MethodHandles.lookup();
        try {
            DYNAMIC_CAST = methodHandlesLookup.findStatic(
                methodHandlesLookup.lookupClass(),
                "dynamicCast",
                MethodType.methodType(Object.class, Class.class, Object.class)
            );
            DYNAMIC_RECEIVER_CAST = methodHandlesLookup.findStatic(
                methodHandlesLookup.lookupClass(),
                "dynamicReceiverCast",
                MethodType.methodType(Object.class, Object.class, Object.class)
            );
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /** Looks up generic method, with a dynamic cast to the receiver's type. (compound assignment) */
    public static MethodHandle dynamicCast(MethodHandle target) {
        // adapt dynamic receiver cast to the generic method
        MethodHandle cast = DYNAMIC_RECEIVER_CAST.asType(
            MethodType.methodType(target.type().returnType(), target.type().returnType(), target.type().parameterType(0))
        );
        // drop the RHS parameter
        cast = MethodHandles.dropArguments(cast, 2, target.type().parameterType(1));
        // combine: f(x,y) -> g(f(x,y), x, y);
        return MethodHandles.foldArguments(cast, target);
    }

    /** Looks up generic method, with a dynamic cast to the specified type. (explicit assignment) */
    public static MethodHandle dynamicCast(MethodHandle target, Class<?> desired) {
        // adapt dynamic cast to the generic method
        desired = MethodType.methodType(desired).wrap().returnType();
        // bind to the boxed type
        MethodHandle cast = DYNAMIC_CAST.bindTo(desired);
        return MethodHandles.filterReturnValue(target, cast);
    }

    /** Forces a cast to class A for target (only if types differ) */
    public static MethodHandle cast(Class<?> classA, MethodHandle target) {
        MethodType newType = MethodType.methodType(classA).unwrap();
        MethodType targetType = MethodType.methodType(target.type().returnType()).unwrap();

        // don't do a conversion if types are the same. explicitCastArguments has this opto,
        // but we do it explicitly, to make the boolean check simpler
        if (newType.returnType() == targetType.returnType()) {
            return target;
        }

        // we don't allow the to/from boolean conversions of explicitCastArguments
        if (newType.returnType() == boolean.class || targetType.returnType() == boolean.class) {
            throw new ClassCastException("Cannot cast " + targetType.returnType() + " to " + newType.returnType());
        }

        // null return values are not possible for our arguments.
        return MethodHandles.explicitCastArguments(target, target.type().changeReturnType(newType.returnType()));
    }
}
