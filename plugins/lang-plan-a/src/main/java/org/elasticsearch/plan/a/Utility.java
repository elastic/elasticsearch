package org.elasticsearch.plan.a;

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

public class Utility {
    public static boolean NumberToboolean(final Number value) {
        return value.longValue() != 0;
    }

    public static char NumberTochar(final Number value) {
        return (char)value.intValue();
    }

    public static Boolean NumberToBoolean(final Number value) {
        return value.longValue() != 0;
    }

    public static Byte NumberToByte(final Number value) {
        return value == null ? null : value.byteValue();
    }

    public static Short NumberToShort(final Number value) {
        return value == null ? null : value.shortValue();
    }

    public static Character NumberToCharacter(final Number value) {
        return value == null ? null : (char)value.intValue();
    }

    public static Integer NumberToInteger(final Number value) {
        return value == null ? null : value.intValue();
    }

    public static Long NumberToLong(final Number value) {
        return value == null ? null : value.longValue();
    }

    public static Float NumberToFloat(final Number value) {
        return value == null ? null : value.floatValue();
    }

    public static Double NumberToDouble(final Number value) {
        return value == null ? null : value.doubleValue();
    }

    public static byte booleanTobyte(final boolean value) {
        return (byte)(value ? 1 : 0);
    }

    public static short booleanToshort(final boolean value) {
        return (short)(value ? 1 : 0);
    }

    public static char booleanTochar(final boolean value) {
        return (char)(value ? 1 : 0);
    }

    public static int booleanToint(final boolean value) {
        return value ? 1 : 0;
    }

    public static long booleanTolong(final boolean value) {
        return value ? 1 : 0;
    }

    public static float booleanTofloat(final boolean value) {
        return value ? 1 : 0;
    }

    public static double booleanTodouble(final boolean value) {
        return value ? 1 : 0;
    }

    public static Integer booleanToInteger(final boolean value) {
        return value ? 1 : 0;
    }

    public static byte BooleanTobyte(final Boolean value) {
        return (byte)(value ? 1 : 0);
    }

    public static short BooleanToshort(final Boolean value) {
        return (short)(value ? 1 : 0);
    }

    public static char BooleanTochar(final Boolean value) {
        return (char)(value ? 1 : 0);
    }

    public static int BooleanToint(final Boolean value) {
        return value ? 1 : 0;
    }

    public static long BooleanTolong(final Boolean value) {
        return value ? 1 : 0;
    }

    public static float BooleanTofloat(final Boolean value) {
        return value ? 1 : 0;
    }

    public static double BooleanTodouble(final Boolean value) {
        return value ? 1 : 0;
    }

    public static Byte BooleanToByte(final Boolean value) {
        return value == null ? null : (byte)(value ? 1 : 0);
    }

    public static Short BooleanToShort(final Boolean value) {
        return value == null ? null : (short)(value ? 1 : 0);
    }

    public static Character BooleanToCharacter(final Boolean value) {
        return value == null ? null : (char)(value ? 1 : 0);
    }

    public static Integer BooleanToInteger(final Boolean value) {
        return value == null ? null : value ? 1 : 0;
    }

    public static Long BooleanToLong(final Boolean value) {
        return value == null ? null : value ? 1L : 0L;
    }

    public static Float BooleanToFloat(final Boolean value) {
        return value == null ? null : value ? 1F : 0F;
    }

    public static Double BooleanToDouble(final Boolean value) {
        return value == null ? null : value ? 1D : 0D;
    }

    public static boolean byteToboolean(final byte value) {
        return value != 0;
    }

    public static Short byteToShort(final byte value) {
        return (short)value;
    }

    public static Character byteToCharacter(final byte value) {
        return (char)(byte)value;
    }

    public static Integer byteToInteger(final byte value) {
        return (int)value;
    }

    public static Long byteToLong(final byte value) {
        return (long)value;
    }

    public static Float byteToFloat(final byte value) {
        return (float)value;
    }

    public static Double byteToDouble(final byte value) {
        return (double)value;
    }

    public static boolean ByteToboolean(final Byte value) {
        return value != 0;
    }

    public static char ByteTochar(final Byte value) {
        return (char)value.byteValue();
    }

    public static boolean shortToboolean(final short value) {
        return value != 0;
    }

    public static Byte shortToByte(final short value) {
        return (byte)value;
    }

    public static Character shortToCharacter(final short value) {
        return (char)(short)value;
    }

    public static Integer shortToInteger(final short value) {
        return (int)value;
    }

    public static Long shortToLong(final short value) {
        return (long)value;
    }

    public static Float shortToFloat(final short value) {
        return (float)value;
    }

    public static Double shortToDouble(final short value) {
        return (double)value;
    }
    
    public static boolean ShortToboolean(final Short value) {
        return value != 0;
    }
    
    public static char ShortTochar(final Short value) {
        return (char)value.shortValue();
    }

    public static boolean charToboolean(final char value) {
        return value != 0;
    }

    public static Byte charToByte(final char value) {
        return (byte)value;
    }

    public static Short charToShort(final char value) {
        return (short)value;
    }

    public static Integer charToInteger(final char value) {
        return (int)value;
    }

    public static Long charToLong(final char value) {
        return (long)value;
    }

    public static Float charToFloat(final char value) {
        return (float)value;
    }

    public static Double charToDouble(final char value) {
        return (double)value;
    }

    public static boolean CharacterToboolean(final Character value) {
        return value != 0;
    }

    public static byte CharacterTobyte(final Character value) {
        return (byte)value.charValue();
    }

    public static short CharacterToshort(final Character value) {
        return (short)value.charValue();
    }

    public static int CharacterToint(final Character value) {
        return (int)value;
    }

    public static long CharacterTolong(final Character value) {
        return (long)value;
    }

    public static float CharacterTofloat(final Character value) {
        return (float)value;
    }

    public static double CharacterTodouble(final Character value) {
        return (double)value;
    }

    public static Boolean CharacterToBoolean(final Character value) {
        return value == null ? null : value != 0;
    }

    public static Byte CharacterToByte(final Character value) {
        return value == null ? null : (byte)value.charValue();
    }

    public static Short CharacterToShort(final Character value) {
        return value == null ? null : (short)value.charValue();
    }

    public static Integer CharacterToInteger(final Character value) {
        return value == null ? null : (int)value;
    }

    public static Long CharacterToLong(final Character value) {
        return value == null ? null : (long)value;
    }

    public static Float CharacterToFloat(final Character value) {
        return value == null ? null : (float)value;
    }

    public static Double CharacterToDouble(final Character value) {
        return value == null ? null : (double)value;
    }

    public static boolean intToboolean(final int value) {
        return value != 0;
    }

    public static Byte intToByte(final int value) {
        return (byte)value;
    }

    public static Short intToShort(final int value) {
        return (short)value;
    }

    public static Character intToCharacter(final int value) {
        return (char)(int)value;
    }

    public static Long intToLong(final int value) {
        return (long)value;
    }

    public static Float intToFloat(final int value) {
        return (float)value;
    }

    public static Double intToDouble(final int value) {
        return (double)value;
    }
    
    public static boolean IntegerToboolean(final Integer value) {
        return value != 0;
    }

    public static char IntegerTochar(final Integer value) {
        return (char)value.intValue();
    }

    public static boolean longToboolean(final long value) {
        return value != 0;
    }

    public static Byte longToByte(final long value) {
        return (byte)value;
    }

    public static Short longToShort(final long value) {
        return (short)value;
    }

    public static Character longToCharacter(final long value) {
        return (char)(long)value;
    }

    public static Integer longToInteger(final long value) {
        return (int)value;
    }

    public static Float longToFloat(final long value) {
        return (float)value;
    }

    public static Double longToDouble(final long value) {
        return (double)value;
    }
    
    public static boolean LongToboolean(final Long value) {
        return value != 0;
    }

    public static char LongTochar(final Long value) {
        return (char)value.longValue();
    }

    public static boolean floatToboolean(final float value) {
        return value != 0;
    }

    public static Byte floatToByte(final float value) {
        return (byte)value;
    }

    public static Short floatToShort(final float value) {
        return (short)value;
    }

    public static Character floatToCharacter(final float value) {
        return (char)(float)value;
    }

    public static Integer floatToInteger(final float value) {
        return (int)value;
    }

    public static Long floatToLong(final float value) {
        return (long)value;
    }

    public static Double floatToDouble(final float value) {
        return (double)value;
    }
    
    public static boolean FloatToboolean(final Float value) {
        return value != 0;
    }
    
    public static char FloatTochar(final Float value) {
        return (char)value.floatValue();
    }

    public static boolean doubleToboolean(final double value) {
        return value != 0;
    }

    public static Byte doubleToByte(final double value) {
        return (byte)value;
    }

    public static Short doubleToShort(final double value) {
        return (short)value;
    }

    public static Character doubleToCharacter(final double value) {
        return (char)(double)value;
    }

    public static Integer doubleToInteger(final double value) {
        return (int)value;
    }

    public static Long doubleToLong(final double value) {
        return (long)value;
    }
    
    public static Float doubleToFloat(final double value) {
        return (float)value;
    }
    
    public static boolean DoubleToboolean(final Double value) {
        return value != 0;
    }
    
    public static char DoubleTochar(final Double value) {
        return (char)value.doubleValue();
    }
    
    // although divide by zero is guaranteed, the special overflow case is not caught.
    // its not needed for remainder because it is not possible there.
    // see https://docs.oracle.com/javase/specs/jls/se8/html/jls-15.html#jls-15.17.2
    
    /**
     * Integer divide without overflow
     * @throws ArithmeticException on overflow or divide-by-zero
     */
    public static int divideWithoutOverflow(int x, int y) {
       if (x == Integer.MIN_VALUE && y == -1) {
           throw new ArithmeticException("integer overflow");
       }
       return x / y;
    }
    
    /**
     * Long divide without overflow
     * @throws ArithmeticException on overflow or divide-by-zero
     */
    public static long divideWithoutOverflow(long x, long y) {
        if (x == Long.MIN_VALUE && y == -1L) {
            throw new ArithmeticException("long overflow");
        }
        return x / y;
    }

    // byte, short, and char are promoted to int for normal operations,
    // so the JDK exact methods are typically used, and the result has a wider range.
    // but compound assignments and increment/decrement operators (e.g. byte b = Byte.MAX_VALUE; b++;)
    // implicitly cast back to the original type: so these need to be checked against the original range.

    /**
     * Like {@link Math#toIntExact(long)} but for byte range.
     */
    public static byte toByteExact(int value) {
        byte s = (byte) value;
        if (s != value) {
            throw new ArithmeticException("byte overflow");
        }
        return s;
    }

    /**
     * Like {@link Math#toIntExact(long)} but for byte range.
     */
    public static byte toByteExact(long value) {
        byte s = (byte) value;
        if (s != value) {
            throw new ArithmeticException("byte overflow");
        }
        return s;
    }

    /**
     * Like {@link Math#toIntExact(long)} but for byte range.
     */
    public static byte toByteWithoutOverflow(float value) {
        if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
            throw new ArithmeticException("byte overflow");
        }
        return (byte)value;
    }

    /**
     * Like {@link Math#toIntExact(long)} but for byte range.
     */
    public static byte toByteWithoutOverflow(double value) {
        if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
            throw new ArithmeticException("byte overflow");
        }
        return (byte)value;
    }

    /**
     * Like {@link Math#toIntExact(long)} but for short range.
     */
    public static short toShortExact(int value) {
        short s = (short) value;
        if (s != value) {
            throw new ArithmeticException("short overflow");
        }
        return s;
    }

    /**
     * Like {@link Math#toIntExact(long)} but for short range.
     */
    public static short toShortExact(long value) {
        short s = (short) value;
        if (s != value) {
            throw new ArithmeticException("short overflow");
        }
        return s;
    }

    /**
     * Like {@link Math#toIntExact(long)} but for short range.
     */
    public static short toShortWithoutOverflow(float value) {
        if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
            throw new ArithmeticException("short overflow");
        }
        return (short)value;
    }

    /**
     * Like {@link Math#toIntExact(long)} but for short range.
     */
    public static short toShortExact(double value) {
        if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
            throw new ArithmeticException("short overflow");
        }
        return (short)value;
    }

    /**
     * Like {@link Math#toIntExact(long)} but for char range.
     */
    public static char toCharExact(int value) {
        char s = (char) value;
        if (s != value) {
            throw new ArithmeticException("char overflow");
        }
        return s;
    }

    /**
     * Like {@link Math#toIntExact(long)} but for char range.
     */
    public static char toCharExact(long value) {
        char s = (char) value;
        if (s != value) {
            throw new ArithmeticException("char overflow");
        }
        return s;
    }

    /**
     * Like {@link Math#toIntExact(long)} but for char range.
     */
    public static char toCharWithoutOverflow(float value) {
        if (value < Character.MIN_VALUE || value > Character.MAX_VALUE) {
            throw new ArithmeticException("char overflow");
        }
        return (char)value;
    }

    /**
     * Like {@link Math#toIntExact(long)} but for char range.
     */
    public static char toCharWithoutOverflow(double value) {
        if (value < Character.MIN_VALUE || value > Character.MAX_VALUE) {
            throw new ArithmeticException("char overflow");
        }
        return (char)value;
    }

    /**
     * Like {@link Math#toIntExact(long)} but for int range.
     */
    public static int toIntWithoutOverflow(float value) {
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new ArithmeticException("int overflow");
        }
        return (int)value;
    }

    /**
     * Like {@link Math#toIntExact(long)} but for int range.
     */
    public static int toIntWithoutOverflow(double value) {
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new ArithmeticException("int overflow");
        }
        return (int)value;
    }

    /**
     * Like {@link Math#toIntExact(long)} but for long range.
     */
    public static long toLongExactWithoutOverflow(float value) {
        if (value < Long.MIN_VALUE || value > Long.MAX_VALUE) {
            throw new ArithmeticException("long overflow");
        }
        return (long)value;
    }

    /**
     * Like {@link Math#toIntExact(long)} but for long range.
     */
    public static float toLongExactWithoutOverflow(double value) {
        if (value < Long.MIN_VALUE || value > Long.MAX_VALUE) {
            throw new ArithmeticException("long overflow");
        }
        return (long)value;
    }

    /**
     * Like {@link Math#toIntExact(long)} but for float range.
     */
    public static float toFloatWithoutOverflow(double value) {
        if (value < Float.MIN_VALUE || value > Float.MAX_VALUE) {
            throw new ArithmeticException("float overflow");
        }
        return (float)value;
    }

    /**
     * Checks for overflow, result is infinite but operands are finite
     * @throws ArithmeticException if overflow occurred
     */
    private static float checkInfFloat(float x, float y, float z) {
        if (Float.isInfinite(z)) {
            if (Float.isFinite(x) && Float.isFinite(y)) {
                throw new ArithmeticException("float overflow");
            }
        }
        return z;
    }
    
    /**
     * Checks for NaN, result is NaN but operands are finite
     * @throws ArithmeticException if overflow occurred
     */
    private static float checkNaNFloat(float x, float y, float z) {
        if (Float.isNaN(z)) {
            if (Float.isFinite(x) && Float.isFinite(y)) {
                throw new ArithmeticException("NaN");
            }
        }
        return z;
    }
    
    /**
     * Checks for NaN, result is infinite but operands are finite
     * @throws ArithmeticException if overflow occurred
     */
    private static double checkInfDouble(double x, double y, double z) {
        if (Double.isInfinite(z)) {
            if (Double.isFinite(x) && Double.isFinite(y)) {
                throw new ArithmeticException("double overflow");
            }
        }
        return z;
    }
    
    /**
     * Checks for NaN, result is NaN but operands are finite
     * @throws ArithmeticException if overflow occurred
     */
    private static double checkNaNDouble(double x, double y, double z) {
        if (Double.isNaN(z)) {
            if (Double.isFinite(x) && Double.isFinite(y)) {
                throw new ArithmeticException("NaN");
            }
        }
        return z;
    }
    
    /**
     * Adds two floats but throws {@code ArithmeticException}
     * if the result overflows.
     */
    public static float addWithoutOverflow(float x, float y) {
        return checkInfFloat(x, y, x + y);
    }
    
    /**
     * Adds two doubles but throws {@code ArithmeticException}
     * if the result overflows.
     */
    public static double addWithoutOverflow(double x, double y) {
        return checkInfDouble(x, y, x + y);
    }
    
    /**
     * Subtracts two floats but throws {@code ArithmeticException}
     * if the result overflows.
     */
    public static float subtractWithoutOverflow(float x, float y) {
        return checkInfFloat(x, y, x - y);
    }
    
    /**
     * Subtracts two doubles but throws {@code ArithmeticException}
     * if the result overflows.
     */
    public static double subtractWithoutOverflow(double x, double y) {
        return checkInfDouble(x, y , x - y);
    }
    
    /**
     * Multiplies two floats but throws {@code ArithmeticException}
     * if the result overflows.
     */
    public static float multiplyWithoutOverflow(float x, float y) {
        return checkInfFloat(x, y, x * y);
    }
    
    /**
     * Multiplies two doubles but throws {@code ArithmeticException}
     * if the result overflows.
     */
    public static double multiplyWithoutOverflow(double x, double y) {
        return checkInfDouble(x, y, x * y);
    }
    
    /**
     * Divides two floats but throws {@code ArithmeticException}
     * if the result overflows, or would create NaN from finite
     * inputs ({@code x == 0, y == 0})
     */
    public static float divideWithoutOverflow(float x, float y) {
        return checkNaNFloat(x, y, checkInfFloat(x, y, x / y));
    }
    
    /**
     * Divides two doubles but throws {@code ArithmeticException}
     * if the result overflows, or would create NaN from finite
     * inputs ({@code x == 0, y == 0})
     */
    public static double divideWithoutOverflow(double x, double y) {
        return checkNaNDouble(x, y, checkInfDouble(x, y, x / y));
    }
    
    /**
     * Takes remainder two floats but throws {@code ArithmeticException}
     * if the result would create NaN from finite inputs ({@code y == 0})
     */
    public static float remainderWithoutOverflow(float x, float y) {
        return checkNaNFloat(x, y, x % y);
    }
    
    /**
     * Divides two doubles but throws {@code ArithmeticException}
     * if the result would create NaN from finite inputs ({@code y == 0})
     */
    public static double remainderWithoutOverflow(double x, double y) {
        return checkNaNDouble(x, y, x % y);
    }

    public static boolean checkEquals(final Object left, final Object right) {
        if (left != null && right != null) {
            return left.equals(right);
        }

        return left == null && right == null;
    }

    private Utility() {}
}
