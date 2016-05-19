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

/**
 * A set of methods for non-native boxing and non-native
 * exact math operations used at both compile-time and runtime.
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
        return (char)value;
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
        return (char)value;
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

    public static String charToString(final char value) {
        return String.valueOf(value);
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
        return value;
    }

    public static long CharacterTolong(final Character value) {
        return value;
    }

    public static float CharacterTofloat(final Character value) {
        return value;
    }

    public static double CharacterTodouble(final Character value) {
        return value;
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

    public static String CharacterToString(final Character value) {
        return value == null ? null : value.toString();
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
        return (char)value;
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
        return (char)value;
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
        return (char)value;
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
        return (char)value;
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

    public static char StringTochar(final String value) {
        if (value.length() != 1) {
            throw new ClassCastException("Cannot cast [String] with length greater than one to [char].");
        }

        return value.charAt(0);
    }

    public static Character StringToCharacter(final String value) {
        if (value == null) {
            return null;
        }

        if (value.length() != 1) {
            throw new ClassCastException("Cannot cast [String] with length greater than one to [Character].");
        }

        return value.charAt(0);
    }

    public static boolean checkEquals(final Object left, final Object right) {
        if (left != null) {
            return left.equals(right);
        }

        return right == null || right.equals(null);
    }

    private Utility() {}
}
