/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import java.nio.LongBuffer;

/**
 * Very simple variable byte encoding that always uses 64bit units.  The idea is that the next few values
 * are smashed into 64 bits using a few bits to indicate how they are fitted in and the rest of the bits
 * to fit each value into equal-sized chunks.
 *
 * In this encoding, 4 bits are used to indicate how the remaining 60 bits are divided. The possible ways are shown
 * in the following table:
 * <table>
 * <tr><th>Code</th><th>Arrangement</th></tr>
 * <tr><td>14</td> <td>1 X 60BITS</tr>
 * <tr><td>13</td> <td>2 X 30BITS</tr>
 * <tr><td>12</td> <td>3 X 20BITS</tr>
 * <tr><td>11</td> <td>4 X 15BITS</tr>
 * <tr><td>10</td> <td>5 X 12BITS</tr>
 * <tr><td>9</td>  <td>6 X 10BITS</tr>
 * <tr><td>8</td>  <td> 7 X 8BITS</tr>
 * <tr><td>7</td>  <td> 8 X 7BITS </tr>
 * <tr><td>6</td>  <td>10 X 6BITS</tr>
 * <tr><td>5</td>  <td>12 X 5BITS</tr>
 * <tr><td>4</td>  <td>15 X 4BITS</tr>
 * <tr><td>3</td>  <td>20 X 3BITS</tr>
 * <tr><td>2</td>  <td>30 X 2BITS</tr>
 * <tr><td>1</td>  <td>60 X 1BITS</tr>
 * <caption>Size codes for Simple64 compression</caption>
 * </table>
 */
public class Simple64 {

    private static final int NUM_DATA_BITS = 60;
    private static final int BITS_30_MASK = (1 << 30) - 1;
    private static final int BITS_20_MASK = (1 << 20) - 1;
    private static final int BITS_15_MASK = (1 << 15) - 1;
    private static final int BITS_12_MASK = (1 << 12) - 1;
    private static final int BITS_11_MASK = (1 << 11) - 1;
    private static final int BITS_10_MASK = (1 << 10) - 1;
    private static final int BITS_8_MASK = (1 << 8) - 1; // 4 bits unused, then the last value take them
    private static final int BITS_7_MASK = (1 << 7) - 1; // 4 bits unused, then the last value take them
    private static final int BITS_6_MASK = (1 << 6) - 1;
    private static final int BITS_5_MASK = (1 << 5) - 1;
    private static final int BITS_4_MASK = (1 << 4) - 1;
    private static final int BITS_3_MASK = (1 << 3) - 1;
    private static final int BITS_2_MASK = (1 << 2) - 1;
    private static final int BITS_1_MASK = (1 << 1) - 1;

    private static final int STATUS_1NUM_60BITS = 14;
    private static final int STATUS_2NUM_30BITS = 13;
    private static final int STATUS_3NUM_20BITS = 12;
    private static final int STATUS_4NUM_15BITS = 11;
    private static final int STATUS_5NUM_12BITS = 10;
    private static final int STATUS_6NUM_10BITS = 9;
    private static final int STATUS_7NUM_8BITS = 8;
    private static final int STATUS_8NUM_7BITS = 7;
    private static final int STATUS_10NUM_6BITS = 6;
    private static final int STATUS_12NUM_5BITS = 5;
    private static final int STATUS_15NUM_4BITS = 4;
    private static final int STATUS_20NUM_3BITS = 3;
    private static final int STATUS_30NUM_2BITS = 2;
    private static final int STATUS_60NUM_1BITS = 1;

    private int inputCompressable = 1;
    private int minBits = 1;
    private long maxFitPlus1 = (1 << minBits);
    private final long[] pending = new long[100];     // nocommit -- 60 or 61 should do?
    private int inputCount;

    private void reset() {
        inputCompressable = 1;
        minBits = 1;
        inputCount = 0;
        maxFitPlus1 = (1 << minBits);
    }

    // nocommit -- need low level test that streaming api
    // didn't break anything

    // Returns 0 if no new long written, else returns number
    // of input values and out[0] has the long to write
    public int add(long v, long[] out) {
        // System.out.println("S64.add v=" + v + " " + (1 + inputCount - inputCompressable) + " waiting");
        pending[inputCount++] = v;
        while (inputCompressable <= inputCount) {
            final long nextData = pending[(inputCompressable - 1)];
            // System.out.println(" cycle: data=" + nextData);
            while ((nextData >= maxFitPlus1) && (minBits < NUM_DATA_BITS)) {
                // System.out.println(" cycle maxFitPlus1=" + maxFitPlus1 + " minBits=" + minBits);
                if ((minBits == 7) && (inputCompressable == 8) && (nextData < (maxFitPlus1 << 4))) {
                    break;
                } else if ((minBits == 8) && (inputCompressable == 7) && (nextData < (maxFitPlus1 << 4))) {
                    break;
                } else {
                    // System.out.println(" advance");
                    minBits++;
                    maxFitPlus1 <<= 1;
                    if ((inputCompressable * minBits) > NUM_DATA_BITS) {
                        inputCompressable--;
                        // System.out.println(" hard break");
                        break;
                    }
                }
            }
            inputCompressable++;

            // System.out.println(" minBits=" + minBits + " count=" + (inputCompressable-1) + " inputCount=" + inputCount);

            if ((inputCompressable * minBits) > NUM_DATA_BITS) {
                // Time to compress!
                inputCompressable--;
                // System.out.println(" FLUSH count=" + inputCompressable);

                // nocommit -- it should always be > 0... right??
                assert inputCompressable > 0;

                // Check whether a bigger number of bits can be used:
                while ((inputCompressable * (minBits + 1)) <= NUM_DATA_BITS) {
                    minBits++;
                    // System.out.println(" incr minBits=" + minBits);
                }

                /*
                  if (((inputCompressable+1) * minBits) <= NUM_DATA_BITS) {
                  // not enough input available for minBits
                  minBits++;
                  // do not compress all available input
                  inputCompressable = NUM_DATA_BITS / minBits;
                  }
                */

                // Put compression method in status bits and encode input data
                long s9;
                final int consumed;
                switch (minBits) { // add status bits and later input values
                    case 60:
                        s9 = STATUS_1NUM_60BITS;
                        s9 |= pending[0] << 4;
                        consumed = 1;
                        break;
                    case 30:
                        s9 = STATUS_2NUM_30BITS;
                        // nocommit -- make a single expr instead of |'ing ?
                        s9 |= pending[0] << 4;
                        s9 |= pending[1] << 34;
                        consumed = 2;
                        break;
                    case 20:
                        s9 = STATUS_3NUM_20BITS;
                        s9 |= pending[0] << 4;
                        s9 |= pending[1] << 24;
                        s9 |= pending[2] << 44;
                        consumed = 3;
                        break;
                    case 15:
                        s9 = STATUS_4NUM_15BITS;
                        s9 |= pending[0] << 4;
                        s9 |= pending[1] << 19;
                        s9 |= pending[2] << 34;
                        s9 |= pending[3] << 49;
                        consumed = 4;
                        break;
                    case 12:
                        s9 = STATUS_5NUM_12BITS;
                        s9 |= pending[0] << 4;
                        s9 |= pending[1] << 16;
                        s9 |= pending[2] << 28;
                        s9 |= pending[3] << 40;
                        s9 |= pending[4] << 52;
                        consumed = 5;
                        break;
                    case 10:
                        s9 = STATUS_6NUM_10BITS;
                        s9 |= pending[0] << 4;
                        s9 |= pending[1] << 14;
                        s9 |= pending[2] << 24;
                        s9 |= pending[3] << 34;
                        s9 |= pending[4] << 44;
                        s9 |= pending[5] << 54;
                        consumed = 6;
                        break;
                    case 8:
                        s9 = STATUS_7NUM_8BITS;
                        s9 |= pending[0] << 4;
                        s9 |= pending[1] << 12;
                        s9 |= pending[2] << 20;
                        s9 |= pending[3] << 28;
                        s9 |= pending[4] << 36;
                        s9 |= pending[5] << 44;
                        s9 |= pending[6] << 52; // 4 more bits
                        consumed = 7;
                        break;
                    case 7:
                        s9 = STATUS_8NUM_7BITS;
                        s9 |= pending[0] << 4;
                        s9 |= pending[1] << 11;
                        s9 |= pending[2] << 18;
                        s9 |= pending[3] << 25;
                        s9 |= pending[4] << 32;
                        s9 |= pending[5] << 39;
                        s9 |= pending[6] << 46;
                        s9 |= pending[7] << 53; // 4 more bits
                        consumed = 8;
                        break;
                    case 6:
                        s9 = STATUS_10NUM_6BITS;
                        s9 |= pending[0] << 4;
                        s9 |= pending[1] << 10;
                        s9 |= pending[2] << 16;
                        s9 |= pending[3] << 22;
                        s9 |= pending[4] << 28;
                        s9 |= pending[5] << 34;
                        s9 |= pending[6] << 40;
                        s9 |= pending[7] << 46;
                        s9 |= pending[8] << 52;
                        s9 |= pending[9] << 58;
                        consumed = 10;
                        break;
                    case 5:
                        s9 = STATUS_12NUM_5BITS;
                        s9 |= pending[0] << 4;
                        s9 |= pending[1] << 9;
                        s9 |= pending[2] << 14;
                        s9 |= pending[3] << 19;
                        s9 |= pending[4] << 24;
                        s9 |= pending[5] << 29;
                        s9 |= pending[6] << 34;
                        s9 |= pending[7] << 39;
                        s9 |= pending[8] << 44;
                        s9 |= pending[9] << 49;
                        s9 |= pending[10] << 54;
                        s9 |= pending[11] << 59;
                        consumed = 12;
                        break;
                    case 4:
                        s9 = STATUS_15NUM_4BITS;
                        s9 |= pending[0] << 4;
                        s9 |= pending[1] << 8;
                        s9 |= pending[2] << 12;
                        s9 |= pending[3] << 16;
                        s9 |= pending[4] << 20;
                        s9 |= pending[5] << 24;
                        s9 |= pending[6] << 28;
                        s9 |= pending[7] << 32;
                        s9 |= pending[8] << 36;
                        s9 |= pending[9] << 40;
                        s9 |= pending[10] << 44;
                        s9 |= pending[11] << 48;
                        s9 |= pending[12] << 52;
                        s9 |= pending[13] << 56;
                        s9 |= pending[14] << 60;
                        consumed = 15;
                        break;
                    case 3:
                        s9 = STATUS_20NUM_3BITS;
                        s9 |= pending[0] << 4;
                        s9 |= pending[1] << 7;
                        s9 |= pending[2] << 10;
                        s9 |= pending[3] << 13;
                        s9 |= pending[4] << 16;
                        s9 |= pending[5] << 19;
                        s9 |= pending[6] << 22;
                        s9 |= pending[7] << 25;
                        s9 |= pending[8] << 28;
                        s9 |= pending[9] << 31;
                        s9 |= pending[10] << 34;
                        s9 |= pending[11] << 37;
                        s9 |= pending[12] << 40;
                        s9 |= pending[13] << 43;
                        s9 |= pending[14] << 46;
                        s9 |= pending[15] << 49;
                        s9 |= pending[16] << 52;
                        s9 |= pending[17] << 55;
                        s9 |= pending[18] << 58;
                        s9 |= pending[19] << 61;
                        consumed = 20;
                        break;
                    case 2:
                        s9 = STATUS_30NUM_2BITS;
                        s9 |= pending[0] << 4;
                        s9 |= pending[1] << 6;
                        s9 |= pending[2] << 8;
                        s9 |= pending[3] << 10;
                        s9 |= pending[4] << 12;
                        s9 |= pending[5] << 14;
                        s9 |= pending[6] << 16;
                        s9 |= pending[7] << 18;
                        s9 |= pending[8] << 20;
                        s9 |= pending[9] << 22;
                        s9 |= pending[10] << 24;
                        s9 |= pending[11] << 26;
                        s9 |= pending[12] << 28;
                        s9 |= pending[13] << 30;
                        s9 |= pending[14] << 32;
                        s9 |= pending[15] << 34;
                        s9 |= pending[16] << 36;
                        s9 |= pending[17] << 38;
                        s9 |= pending[18] << 40;
                        s9 |= pending[19] << 42;
                        s9 |= pending[20] << 44;
                        s9 |= pending[21] << 46;
                        s9 |= pending[22] << 48;
                        s9 |= pending[23] << 50;
                        s9 |= pending[24] << 52;
                        s9 |= pending[25] << 54;
                        s9 |= pending[26] << 56;
                        s9 |= pending[27] << 58;
                        s9 |= pending[28] << 60;
                        s9 |= pending[29] << 62;
                        consumed = 30;
                        break;
                    case 1:
                        s9 = STATUS_60NUM_1BITS;
                        s9 |= pending[0] << 4;
                        s9 |= pending[1] << 5;
                        s9 |= pending[2] << 6;
                        s9 |= pending[3] << 7;
                        s9 |= pending[4] << 8;
                        s9 |= pending[5] << 9;
                        s9 |= pending[6] << 10;
                        s9 |= pending[7] << 11;
                        s9 |= pending[8] << 12;
                        s9 |= pending[9] << 13;
                        s9 |= pending[10] << 14;
                        s9 |= pending[11] << 15;
                        s9 |= pending[12] << 16;
                        s9 |= pending[13] << 17;
                        s9 |= pending[14] << 18;
                        s9 |= pending[15] << 19;
                        s9 |= pending[16] << 20;
                        s9 |= pending[17] << 21;
                        s9 |= pending[18] << 22;
                        s9 |= pending[19] << 23;
                        s9 |= pending[20] << 24;
                        s9 |= pending[21] << 25;
                        s9 |= pending[22] << 26;
                        s9 |= pending[23] << 27;
                        s9 |= pending[24] << 28;
                        s9 |= pending[25] << 29;
                        s9 |= pending[26] << 30;
                        s9 |= pending[27] << 31;
                        s9 |= pending[28] << 32;
                        s9 |= pending[29] << 33;
                        s9 |= pending[30] << 34;
                        s9 |= pending[31] << 35;
                        s9 |= pending[32] << 36;
                        s9 |= pending[33] << 37;
                        s9 |= pending[34] << 38;
                        s9 |= pending[35] << 39;
                        s9 |= pending[36] << 40;
                        s9 |= pending[37] << 41;
                        s9 |= pending[38] << 42;
                        s9 |= pending[39] << 43;
                        s9 |= pending[40] << 44;
                        s9 |= pending[41] << 45;
                        s9 |= pending[42] << 46;
                        s9 |= pending[43] << 47;
                        s9 |= pending[44] << 48;
                        s9 |= pending[45] << 49;
                        s9 |= pending[46] << 50;
                        s9 |= pending[47] << 51;
                        s9 |= pending[48] << 52;
                        s9 |= pending[49] << 53;
                        s9 |= pending[50] << 54;
                        s9 |= pending[51] << 55;
                        s9 |= pending[52] << 56;
                        s9 |= pending[53] << 57;
                        s9 |= pending[54] << 58;
                        s9 |= pending[55] << 59;
                        s9 |= pending[56] << 60;
                        s9 |= pending[57] << 61;
                        s9 |= pending[58] << 62;
                        s9 |= pending[59] << 63;
                        consumed = 60;
                        break;
                    default:
                        assert false;
                        s9 = 0;
                        consumed = 60;
                        // throw new Error("S98b.compressSingle internal error: unknown minBits: " + minBits);
                }

                final int leftover = inputCount - consumed;
                assert leftover >= 0 : "consumed=" + consumed + " vs " + inputCompressable;

                /*
                for(int x=0;x<consumed;x++) {
                  System.out.println(""+pending[x]);
                }
                */
                /*
                // like cd burning!  ;)
                {
                int[] test = new int[consumed];
                int ct = decompressSingle(s9, test, 0);
                assert ct == consumed;
                for(int x=0;x<ct;x++) {
                assert test[x] == pending[x];
                }
                }
                */

                // System.out.println(" return consumed=" + consumed);
                reset();

                // save leftovers:
                System.arraycopy(pending, consumed, pending, 0, leftover);
                inputCount = leftover;

                out[0] = s9;
                return consumed;
            }
        }

        return 0;
    }

    private static int compressSingle(final long[] uncompressed, final int inOffset, final int inSize, final LongBuffer compressedBuffer) {
        if (inSize < 1) {
            throw new IllegalArgumentException("Cannot compress input with non positive size " + inSize);
        }
        int inputCompressable = 1;
        int minBits = 1;
        long maxFitPlus1 = (1 << minBits);
        long nextData;

        do {
            nextData = uncompressed[inOffset + inputCompressable - 1];
            if (nextData < 0) {
                throw new IllegalArgumentException(
                    "Cannot compress negative input " + nextData + " (at index " + (inOffset + inputCompressable - 1) + ")"
                );
            }
            while ((nextData >= maxFitPlus1) && (minBits < NUM_DATA_BITS)) {
                if ((minBits == 7) && (inputCompressable == 8) && (nextData < (maxFitPlus1 << 4))) {
                    break;
                } else if ((minBits == 8) && (inputCompressable == 7) && (nextData < (maxFitPlus1 << 4))) {
                    break;
                } else {
                    minBits++;
                    maxFitPlus1 <<= 1;
                    if ((inputCompressable * minBits) > NUM_DATA_BITS) {
                        inputCompressable--;
                        break;
                    }
                }
            }
            inputCompressable++;
        } while (((inputCompressable * minBits) <= NUM_DATA_BITS) && (inputCompressable <= inSize));

        inputCompressable--;
        if (inputCompressable == 0) {
            throw new IllegalArgumentException(
                "Cannot compress input " + nextData + " with more than " + NUM_DATA_BITS + " bits (at offSet " + inOffset + ")"
            );
        }

        // Check whether a bigger number of bits can be used:
        while ((inputCompressable * (minBits + 1)) <= NUM_DATA_BITS) {
            minBits++;
        }

        if (((inputCompressable + 1) * minBits) <= NUM_DATA_BITS) {
            // not enough input available for minBits
            minBits++;
        }

        // Put compression method in status bits and encode input data
        long s9;
        switch (minBits) { // add status bits and later input values
            case 60:
                s9 = STATUS_1NUM_60BITS;
                s9 |= uncompressed[inOffset] << 4;
                compressedBuffer.put(s9);
                return 1;
            case 30:
                s9 = STATUS_2NUM_30BITS;
                s9 |= uncompressed[inOffset] << 4;
                s9 |= uncompressed[inOffset + 1] << 34;
                compressedBuffer.put(s9);
                return 2;
            case 20:
                s9 = STATUS_3NUM_20BITS;
                s9 |= uncompressed[inOffset] << 4;
                s9 |= uncompressed[inOffset + 1] << 24;
                s9 |= uncompressed[inOffset + 2] << 44;
                compressedBuffer.put(s9);
                return 3;
            case 15:
                s9 = STATUS_4NUM_15BITS;
                s9 |= uncompressed[inOffset] << 4;
                s9 |= uncompressed[inOffset + 1] << 19;
                s9 |= uncompressed[inOffset + 2] << 34;
                s9 |= uncompressed[inOffset + 3] << 49;
                compressedBuffer.put(s9);
                return 4;
            case 12:
                s9 = STATUS_5NUM_12BITS;
                s9 |= uncompressed[inOffset] << 4;
                s9 |= uncompressed[inOffset + 1] << 16;
                s9 |= uncompressed[inOffset + 2] << 28;
                s9 |= uncompressed[inOffset + 3] << 40;
                s9 |= uncompressed[inOffset + 4] << 52;
                compressedBuffer.put(s9);
                return 5;
            case 10:
                s9 = STATUS_6NUM_10BITS;
                s9 |= uncompressed[inOffset] << 4;
                s9 |= uncompressed[inOffset + 1] << 14;
                s9 |= uncompressed[inOffset + 2] << 24;
                s9 |= uncompressed[inOffset + 3] << 34;
                s9 |= uncompressed[inOffset + 4] << 44;
                s9 |= uncompressed[inOffset + 5] << 54;
                compressedBuffer.put(s9);
                return 6;
            case 8:
                s9 = STATUS_7NUM_8BITS;
                s9 |= uncompressed[inOffset] << 4;
                s9 |= uncompressed[inOffset + 1] << 12;
                s9 |= uncompressed[inOffset + 2] << 20;
                s9 |= uncompressed[inOffset + 3] << 28;
                s9 |= uncompressed[inOffset + 4] << 36;
                s9 |= uncompressed[inOffset + 5] << 44;
                s9 |= uncompressed[inOffset + 6] << 52; // 4 more bits
                compressedBuffer.put(s9);
                return 7;
            case 7:
                s9 = STATUS_8NUM_7BITS;
                s9 |= uncompressed[inOffset] << 4;
                s9 |= uncompressed[inOffset + 1] << 11;
                s9 |= uncompressed[inOffset + 2] << 18;
                s9 |= uncompressed[inOffset + 3] << 25;
                s9 |= uncompressed[inOffset + 4] << 32;
                s9 |= uncompressed[inOffset + 5] << 39;
                s9 |= uncompressed[inOffset + 6] << 46;
                s9 |= uncompressed[inOffset + 7] << 53; // 4 more bits
                compressedBuffer.put(s9);
                return 8;
            case 6:
                s9 = STATUS_10NUM_6BITS;
                s9 |= uncompressed[inOffset] << 4;
                s9 |= uncompressed[inOffset + 1] << 10;
                s9 |= uncompressed[inOffset + 2] << 16;
                s9 |= uncompressed[inOffset + 3] << 22;
                s9 |= uncompressed[inOffset + 4] << 28;
                s9 |= uncompressed[inOffset + 5] << 34;
                s9 |= uncompressed[inOffset + 6] << 40;
                s9 |= uncompressed[inOffset + 7] << 46;
                s9 |= uncompressed[inOffset + 8] << 52;
                s9 |= uncompressed[inOffset + 9] << 58;
                compressedBuffer.put(s9);
                return 10;
            case 5:
                s9 = STATUS_12NUM_5BITS;
                s9 |= uncompressed[inOffset] << 4;
                s9 |= uncompressed[inOffset + 1] << 9;
                s9 |= uncompressed[inOffset + 2] << 14;
                s9 |= uncompressed[inOffset + 3] << 19;
                s9 |= uncompressed[inOffset + 4] << 24;
                s9 |= uncompressed[inOffset + 5] << 29;
                s9 |= uncompressed[inOffset + 6] << 34;
                s9 |= uncompressed[inOffset + 7] << 39;
                s9 |= uncompressed[inOffset + 8] << 44;
                s9 |= uncompressed[inOffset + 9] << 49;
                s9 |= uncompressed[inOffset + 10] << 54;
                s9 |= uncompressed[inOffset + 11] << 59;
                compressedBuffer.put(s9);
                return 12;
            case 4:
                s9 = STATUS_15NUM_4BITS;
                s9 |= uncompressed[inOffset] << 4;
                s9 |= uncompressed[inOffset + 1] << 8;
                s9 |= uncompressed[inOffset + 2] << 12;
                s9 |= uncompressed[inOffset + 3] << 16;
                s9 |= uncompressed[inOffset + 4] << 20;
                s9 |= uncompressed[inOffset + 5] << 24;
                s9 |= uncompressed[inOffset + 6] << 28;
                s9 |= uncompressed[inOffset + 7] << 32;
                s9 |= uncompressed[inOffset + 8] << 36;
                s9 |= uncompressed[inOffset + 9] << 40;
                s9 |= uncompressed[inOffset + 10] << 44;
                s9 |= uncompressed[inOffset + 11] << 48;
                s9 |= uncompressed[inOffset + 12] << 52;
                s9 |= uncompressed[inOffset + 13] << 56;
                s9 |= uncompressed[inOffset + 14] << 60;
                compressedBuffer.put(s9);
                return 15;
            case 3:
                s9 = STATUS_20NUM_3BITS;
                s9 |= uncompressed[inOffset] << 4;
                s9 |= uncompressed[inOffset + 1] << 7;
                s9 |= uncompressed[inOffset + 2] << 10;
                s9 |= uncompressed[inOffset + 3] << 13;
                s9 |= uncompressed[inOffset + 4] << 16;
                s9 |= uncompressed[inOffset + 5] << 19;
                s9 |= uncompressed[inOffset + 6] << 22;
                s9 |= uncompressed[inOffset + 7] << 25;
                s9 |= uncompressed[inOffset + 8] << 28;
                s9 |= uncompressed[inOffset + 9] << 31;
                s9 |= uncompressed[inOffset + 10] << 34;
                s9 |= uncompressed[inOffset + 11] << 37;
                s9 |= uncompressed[inOffset + 12] << 40;
                s9 |= uncompressed[inOffset + 13] << 43;
                s9 |= uncompressed[inOffset + 14] << 46;
                s9 |= uncompressed[inOffset + 15] << 49;
                s9 |= uncompressed[inOffset + 16] << 52;
                s9 |= uncompressed[inOffset + 17] << 55;
                s9 |= uncompressed[inOffset + 18] << 58;
                s9 |= uncompressed[inOffset + 19] << 61;
                compressedBuffer.put(s9);
                return 20;
            case 2:
                s9 = STATUS_30NUM_2BITS;
                s9 |= uncompressed[inOffset] << 4;
                s9 |= uncompressed[inOffset + 1] << 6;
                s9 |= uncompressed[inOffset + 2] << 8;
                s9 |= uncompressed[inOffset + 3] << 10;
                s9 |= uncompressed[inOffset + 4] << 12;
                s9 |= uncompressed[inOffset + 5] << 14;
                s9 |= uncompressed[inOffset + 6] << 16;
                s9 |= uncompressed[inOffset + 7] << 18;
                s9 |= uncompressed[inOffset + 8] << 20;
                s9 |= uncompressed[inOffset + 9] << 22;
                s9 |= uncompressed[inOffset + 10] << 24;
                s9 |= uncompressed[inOffset + 11] << 26;
                s9 |= uncompressed[inOffset + 12] << 28;
                s9 |= uncompressed[inOffset + 13] << 30;
                s9 |= uncompressed[inOffset + 14] << 32;
                s9 |= uncompressed[inOffset + 15] << 34;
                s9 |= uncompressed[inOffset + 16] << 36;
                s9 |= uncompressed[inOffset + 17] << 38;
                s9 |= uncompressed[inOffset + 18] << 40;
                s9 |= uncompressed[inOffset + 19] << 42;
                s9 |= uncompressed[inOffset + 20] << 44;
                s9 |= uncompressed[inOffset + 21] << 46;
                s9 |= uncompressed[inOffset + 22] << 48;
                s9 |= uncompressed[inOffset + 23] << 50;
                s9 |= uncompressed[inOffset + 24] << 52;
                s9 |= uncompressed[inOffset + 25] << 54;
                s9 |= uncompressed[inOffset + 26] << 56;
                s9 |= uncompressed[inOffset + 27] << 58;
                s9 |= uncompressed[inOffset + 28] << 60;
                s9 |= uncompressed[inOffset + 29] << 62;
                compressedBuffer.put(s9);
                return 30;
            case 1:
                s9 = STATUS_60NUM_1BITS;
                s9 |= uncompressed[inOffset] << 4;
                s9 |= uncompressed[inOffset + 1] << 5;
                s9 |= uncompressed[inOffset + 2] << 6;
                s9 |= uncompressed[inOffset + 3] << 7;
                s9 |= uncompressed[inOffset + 4] << 8;
                s9 |= uncompressed[inOffset + 5] << 9;
                s9 |= uncompressed[inOffset + 6] << 10;
                s9 |= uncompressed[inOffset + 7] << 11;
                s9 |= uncompressed[inOffset + 8] << 12;
                s9 |= uncompressed[inOffset + 9] << 13;
                s9 |= uncompressed[inOffset + 10] << 14;
                s9 |= uncompressed[inOffset + 11] << 15;
                s9 |= uncompressed[inOffset + 12] << 16;
                s9 |= uncompressed[inOffset + 13] << 17;
                s9 |= uncompressed[inOffset + 14] << 18;
                s9 |= uncompressed[inOffset + 15] << 19;
                s9 |= uncompressed[inOffset + 16] << 20;
                s9 |= uncompressed[inOffset + 17] << 21;
                s9 |= uncompressed[inOffset + 18] << 22;
                s9 |= uncompressed[inOffset + 19] << 23;
                s9 |= uncompressed[inOffset + 20] << 24;
                s9 |= uncompressed[inOffset + 21] << 25;
                s9 |= uncompressed[inOffset + 22] << 26;
                s9 |= uncompressed[inOffset + 23] << 27;
                s9 |= uncompressed[inOffset + 24] << 28;
                s9 |= uncompressed[inOffset + 25] << 29;
                s9 |= uncompressed[inOffset + 26] << 30;
                s9 |= uncompressed[inOffset + 27] << 31;
                s9 |= uncompressed[inOffset + 28] << 32;
                s9 |= uncompressed[inOffset + 29] << 33;
                s9 |= uncompressed[inOffset + 30] << 34;
                s9 |= uncompressed[inOffset + 31] << 35;
                s9 |= uncompressed[inOffset + 32] << 36;
                s9 |= uncompressed[inOffset + 33] << 37;
                s9 |= uncompressed[inOffset + 34] << 38;
                s9 |= uncompressed[inOffset + 35] << 39;
                s9 |= uncompressed[inOffset + 36] << 40;
                s9 |= uncompressed[inOffset + 37] << 41;
                s9 |= uncompressed[inOffset + 38] << 42;
                s9 |= uncompressed[inOffset + 39] << 43;
                s9 |= uncompressed[inOffset + 40] << 44;
                s9 |= uncompressed[inOffset + 41] << 45;
                s9 |= uncompressed[inOffset + 42] << 46;
                s9 |= uncompressed[inOffset + 43] << 47;
                s9 |= uncompressed[inOffset + 44] << 48;
                s9 |= uncompressed[inOffset + 45] << 49;
                s9 |= uncompressed[inOffset + 46] << 50;
                s9 |= uncompressed[inOffset + 47] << 51;
                s9 |= uncompressed[inOffset + 48] << 52;
                s9 |= uncompressed[inOffset + 49] << 53;
                s9 |= uncompressed[inOffset + 50] << 54;
                s9 |= uncompressed[inOffset + 51] << 55;
                s9 |= uncompressed[inOffset + 52] << 56;
                s9 |= uncompressed[inOffset + 53] << 57;
                s9 |= uncompressed[inOffset + 54] << 58;
                s9 |= uncompressed[inOffset + 55] << 59;
                s9 |= uncompressed[inOffset + 56] << 60;
                s9 |= uncompressed[inOffset + 57] << 61;
                s9 |= uncompressed[inOffset + 58] << 62;
                s9 |= uncompressed[inOffset + 59] << 63;
                compressedBuffer.put(s9);
                return 60;
            default:
                throw new Error("S98b.compressSingle internal error: unknown minBits: " + minBits);
        }
    }

    private static int decompressSingle(final long s9, final long[] decompressed, final int outOffset) {
        switch ((int) (s9 & 15L)) {
            case STATUS_1NUM_60BITS:
                decompressed[outOffset] = s9 >>> 4;
                return 1;
            case STATUS_2NUM_30BITS:
                decompressed[outOffset] = (s9 >>> 4) & BITS_30_MASK;
                decompressed[outOffset + 1] = (s9 >>> 34) & BITS_30_MASK;
                return 2;
            case STATUS_3NUM_20BITS:
                decompressed[outOffset] = (s9 >>> 4) & BITS_20_MASK;
                decompressed[outOffset + 1] = ((s9 >>> 24)) & BITS_20_MASK;
                decompressed[outOffset + 2] = (s9 >>> 44) & BITS_20_MASK;
                return 3;
            case STATUS_4NUM_15BITS:
                decompressed[outOffset] = (s9 >>> 4) & BITS_15_MASK;
                decompressed[outOffset + 1] = (s9 >>> 19) & BITS_15_MASK;
                decompressed[outOffset + 2] = (s9 >>> 34) & BITS_15_MASK;
                decompressed[outOffset + 3] = (s9 >>> 49) & BITS_15_MASK;
                return 4;
            case STATUS_5NUM_12BITS:
                decompressed[outOffset] = (s9 >>> 4) & BITS_12_MASK;
                decompressed[outOffset + 1] = (s9 >>> 16) & BITS_12_MASK;
                decompressed[outOffset + 2] = (s9 >>> 28) & BITS_12_MASK;
                decompressed[outOffset + 3] = (s9 >>> 40) & BITS_12_MASK;
                decompressed[outOffset + 4] = (s9 >>> 52) & BITS_12_MASK;
                return 5;
            case STATUS_6NUM_10BITS:
                decompressed[outOffset] = (s9 >>> 4) & BITS_10_MASK;
                decompressed[outOffset + 1] = (s9 >>> 14) & BITS_10_MASK;
                decompressed[outOffset + 2] = (s9 >>> 24) & BITS_10_MASK;
                decompressed[outOffset + 3] = (s9 >>> 34) & BITS_10_MASK;
                decompressed[outOffset + 4] = (s9 >>> 44) & BITS_10_MASK;
                decompressed[outOffset + 5] = (s9 >>> 54) & BITS_10_MASK;
                return 6;
            case STATUS_7NUM_8BITS:
                decompressed[outOffset] = (s9 >>> 4) & BITS_8_MASK;
                decompressed[outOffset + 1] = (s9 >>> 12) & BITS_8_MASK;
                decompressed[outOffset + 2] = (s9 >>> 20) & BITS_8_MASK;
                decompressed[outOffset + 3] = (s9 >>> 28) & BITS_8_MASK;
                decompressed[outOffset + 4] = (s9 >>> 36) & BITS_8_MASK;
                decompressed[outOffset + 5] = (s9 >>> 44) & BITS_8_MASK;
                decompressed[outOffset + 6] = (s9 >>> 52) & BITS_12_MASK;
                return 7;
            case STATUS_8NUM_7BITS:
                decompressed[outOffset] = (s9 >>> 4) & BITS_7_MASK;
                decompressed[outOffset + 1] = (s9 >>> 11) & BITS_7_MASK;
                decompressed[outOffset + 2] = (s9 >>> 18) & BITS_7_MASK;
                decompressed[outOffset + 3] = (s9 >>> 25) & BITS_7_MASK;
                decompressed[outOffset + 4] = (s9 >>> 32) & BITS_7_MASK;
                decompressed[outOffset + 5] = (s9 >>> 39) & BITS_7_MASK;
                decompressed[outOffset + 6] = (s9 >>> 46) & BITS_7_MASK;
                decompressed[outOffset + 7] = (s9 >>> 53) & BITS_11_MASK;
                return 8;
            case STATUS_10NUM_6BITS:
                decompressed[outOffset] = (s9 >>> 4) & BITS_6_MASK;
                decompressed[outOffset + 1] = (s9 >>> 10) & BITS_6_MASK;
                decompressed[outOffset + 2] = (s9 >>> 16) & BITS_6_MASK;
                decompressed[outOffset + 3] = (s9 >>> 22) & BITS_6_MASK;
                decompressed[outOffset + 4] = (s9 >>> 28) & BITS_6_MASK;
                decompressed[outOffset + 5] = (s9 >>> 34) & BITS_6_MASK;
                decompressed[outOffset + 6] = (s9 >>> 40) & BITS_6_MASK;
                decompressed[outOffset + 7] = (s9 >>> 46) & BITS_6_MASK;
                decompressed[outOffset + 8] = (s9 >>> 52) & BITS_6_MASK;
                decompressed[outOffset + 9] = (s9 >>> 58) & BITS_6_MASK;
                return 10;
            case STATUS_12NUM_5BITS:
                decompressed[outOffset] = (s9 >>> 4) & BITS_5_MASK;
                decompressed[outOffset + 1] = (s9 >>> 9) & BITS_5_MASK;
                decompressed[outOffset + 2] = (s9 >>> 14) & BITS_5_MASK;
                decompressed[outOffset + 3] = (s9 >>> 19) & BITS_5_MASK;
                decompressed[outOffset + 4] = (s9 >>> 24) & BITS_5_MASK;
                decompressed[outOffset + 5] = (s9 >>> 29) & BITS_5_MASK;
                decompressed[outOffset + 6] = (s9 >>> 34) & BITS_5_MASK;
                decompressed[outOffset + 7] = (s9 >>> 39) & BITS_5_MASK;
                decompressed[outOffset + 8] = (s9 >>> 44) & BITS_5_MASK;
                decompressed[outOffset + 9] = (s9 >>> 49) & BITS_5_MASK;
                decompressed[outOffset + 10] = (s9 >>> 54) & BITS_5_MASK;
                decompressed[outOffset + 11] = (s9 >>> 59) & BITS_5_MASK;
                return 12;
            case STATUS_15NUM_4BITS:
                decompressed[outOffset] = (s9 >>> 4) & BITS_4_MASK;
                decompressed[outOffset + 1] = (s9 >>> 8) & BITS_4_MASK;
                decompressed[outOffset + 2] = (s9 >>> 12) & BITS_4_MASK;
                decompressed[outOffset + 3] = (s9 >>> 16) & BITS_4_MASK;
                decompressed[outOffset + 4] = (s9 >>> 20) & BITS_4_MASK;
                decompressed[outOffset + 5] = (s9 >>> 24) & BITS_4_MASK;
                decompressed[outOffset + 6] = (s9 >>> 28) & BITS_4_MASK;
                decompressed[outOffset + 6] = (s9 >>> 32) & BITS_4_MASK;
                decompressed[outOffset + 8] = (s9 >>> 36) & BITS_4_MASK;
                decompressed[outOffset + 9] = (s9 >>> 40) & BITS_4_MASK;
                decompressed[outOffset + 10] = (s9 >>> 44) & BITS_4_MASK;
                decompressed[outOffset + 11] = (s9 >>> 48) & BITS_4_MASK;
                decompressed[outOffset + 12] = (s9 >>> 52) & BITS_4_MASK;
                decompressed[outOffset + 13] = (s9 >>> 56) & BITS_4_MASK;
                decompressed[outOffset + 14] = (s9 >>> 60) & BITS_4_MASK;
                return 15;
            case STATUS_20NUM_3BITS:
                decompressed[outOffset] = (s9 >>> 4) & BITS_3_MASK;
                decompressed[outOffset + 1] = (s9 >>> 7) & BITS_3_MASK;
                decompressed[outOffset + 2] = (s9 >>> 10) & BITS_3_MASK;
                decompressed[outOffset + 3] = (s9 >>> 13) & BITS_3_MASK;
                decompressed[outOffset + 4] = (s9 >>> 16) & BITS_3_MASK;
                decompressed[outOffset + 5] = (s9 >>> 19) & BITS_3_MASK;
                decompressed[outOffset + 6] = (s9 >>> 22) & BITS_3_MASK;
                decompressed[outOffset + 7] = (s9 >>> 25) & BITS_3_MASK;
                decompressed[outOffset + 8] = (s9 >>> 28) & BITS_3_MASK;
                decompressed[outOffset + 9] = (s9 >>> 31) & BITS_3_MASK;
                decompressed[outOffset + 10] = (s9 >>> 34) & BITS_3_MASK;
                decompressed[outOffset + 11] = (s9 >>> 37) & BITS_3_MASK;
                decompressed[outOffset + 12] = (s9 >>> 40) & BITS_3_MASK;
                decompressed[outOffset + 13] = (s9 >>> 43) & BITS_3_MASK;
                decompressed[outOffset + 14] = (s9 >>> 46) & BITS_3_MASK;
                decompressed[outOffset + 15] = (s9 >>> 49) & BITS_3_MASK;
                decompressed[outOffset + 16] = (s9 >>> 52) & BITS_3_MASK;
                decompressed[outOffset + 17] = (s9 >>> 55) & BITS_3_MASK;
                decompressed[outOffset + 18] = (s9 >>> 58) & BITS_3_MASK;
                decompressed[outOffset + 19] = (s9 >>> 61) & BITS_3_MASK;
                return 20;
            case STATUS_30NUM_2BITS:
                decompressed[outOffset] = (s9 >>> 4) & BITS_2_MASK;
                decompressed[outOffset + 1] = (s9 >>> 6) & BITS_2_MASK;
                decompressed[outOffset + 2] = (s9 >>> 8) & BITS_2_MASK;
                decompressed[outOffset + 3] = (s9 >>> 10) & BITS_2_MASK;
                decompressed[outOffset + 4] = (s9 >>> 12) & BITS_2_MASK;
                decompressed[outOffset + 5] = (s9 >>> 14) & BITS_2_MASK;
                decompressed[outOffset + 6] = (s9 >>> 16) & BITS_2_MASK;
                decompressed[outOffset + 7] = (s9 >>> 18) & BITS_2_MASK;
                decompressed[outOffset + 8] = (s9 >>> 20) & BITS_2_MASK;
                decompressed[outOffset + 9] = (s9 >>> 22) & BITS_2_MASK;
                decompressed[outOffset + 10] = (s9 >>> 24) & BITS_2_MASK;
                decompressed[outOffset + 11] = (s9 >>> 26) & BITS_2_MASK;
                decompressed[outOffset + 12] = (s9 >>> 28) & BITS_2_MASK;
                decompressed[outOffset + 13] = (s9 >>> 30) & BITS_2_MASK;
                decompressed[outOffset + 14] = (s9 >>> 32) & BITS_2_MASK;
                decompressed[outOffset + 15] = (s9 >>> 34) & BITS_2_MASK;
                decompressed[outOffset + 16] = (s9 >>> 36) & BITS_2_MASK;
                decompressed[outOffset + 17] = (s9 >>> 38) & BITS_2_MASK;
                decompressed[outOffset + 18] = (s9 >>> 40) & BITS_2_MASK;
                decompressed[outOffset + 19] = (s9 >>> 42) & BITS_2_MASK;
                decompressed[outOffset + 20] = (s9 >>> 44) & BITS_2_MASK;
                decompressed[outOffset + 21] = (s9 >>> 46) & BITS_2_MASK;
                decompressed[outOffset + 22] = (s9 >>> 48) & BITS_2_MASK;
                decompressed[outOffset + 23] = (s9 >>> 50) & BITS_2_MASK;
                decompressed[outOffset + 24] = (s9 >>> 52) & BITS_2_MASK;
                decompressed[outOffset + 25] = (s9 >>> 54) & BITS_2_MASK;
                decompressed[outOffset + 26] = (s9 >>> 56) & BITS_2_MASK;
                decompressed[outOffset + 27] = (s9 >>> 58) & BITS_2_MASK;
                decompressed[outOffset + 28] = (s9 >>> 60) & BITS_2_MASK;
                decompressed[outOffset + 29] = (s9 >>> 62) & BITS_2_MASK;
                return 30;
            case STATUS_60NUM_1BITS:
                decompressed[outOffset] = (s9 >>> 4) & BITS_1_MASK;
                decompressed[outOffset + 1] = (s9 >>> 5) & BITS_1_MASK;
                decompressed[outOffset + 2] = (s9 >>> 6) & BITS_1_MASK;
                decompressed[outOffset + 3] = (s9 >>> 7) & BITS_1_MASK;
                decompressed[outOffset + 4] = (s9 >>> 8) & BITS_1_MASK;
                decompressed[outOffset + 5] = (s9 >>> 9) & BITS_1_MASK;
                decompressed[outOffset + 6] = (s9 >>> 10) & BITS_1_MASK;
                decompressed[outOffset + 7] = (s9 >>> 11) & BITS_1_MASK;
                decompressed[outOffset + 8] = (s9 >>> 12) & BITS_1_MASK;
                decompressed[outOffset + 9] = (s9 >>> 13) & BITS_1_MASK;
                decompressed[outOffset + 10] = (s9 >>> 14) & BITS_1_MASK;
                decompressed[outOffset + 11] = (s9 >>> 15) & BITS_1_MASK;
                decompressed[outOffset + 12] = (s9 >>> 16) & BITS_1_MASK;
                decompressed[outOffset + 13] = (s9 >>> 17) & BITS_1_MASK;
                decompressed[outOffset + 14] = (s9 >>> 18) & BITS_1_MASK;
                decompressed[outOffset + 15] = (s9 >>> 19) & BITS_1_MASK;
                decompressed[outOffset + 16] = (s9 >>> 20) & BITS_1_MASK;
                decompressed[outOffset + 17] = (s9 >>> 21) & BITS_1_MASK;
                decompressed[outOffset + 18] = (s9 >>> 22) & BITS_1_MASK;
                decompressed[outOffset + 19] = (s9 >>> 23) & BITS_1_MASK;
                decompressed[outOffset + 20] = (s9 >>> 24) & BITS_1_MASK;
                decompressed[outOffset + 21] = (s9 >>> 25) & BITS_1_MASK;
                decompressed[outOffset + 22] = (s9 >>> 26) & BITS_1_MASK;
                decompressed[outOffset + 23] = (s9 >>> 27) & BITS_1_MASK;
                decompressed[outOffset + 24] = (s9 >>> 28) & BITS_1_MASK;
                decompressed[outOffset + 25] = (s9 >>> 29) & BITS_1_MASK;
                decompressed[outOffset + 26] = (s9 >>> 30) & BITS_1_MASK;
                decompressed[outOffset + 27] = (s9 >>> 31) & BITS_1_MASK;
                decompressed[outOffset + 28] = (s9 >>> 32) & BITS_1_MASK;
                decompressed[outOffset + 29] = (s9 >>> 33) & BITS_1_MASK;
                decompressed[outOffset + 30] = (s9 >>> 34) & BITS_1_MASK;
                decompressed[outOffset + 31] = (s9 >>> 35) & BITS_1_MASK;
                decompressed[outOffset + 32] = (s9 >>> 36) & BITS_1_MASK;
                decompressed[outOffset + 33] = (s9 >>> 37) & BITS_1_MASK;
                decompressed[outOffset + 34] = (s9 >>> 38) & BITS_1_MASK;
                decompressed[outOffset + 35] = (s9 >>> 39) & BITS_1_MASK;
                decompressed[outOffset + 36] = (s9 >>> 40) & BITS_1_MASK;
                decompressed[outOffset + 37] = (s9 >>> 41) & BITS_1_MASK;
                decompressed[outOffset + 38] = (s9 >>> 42) & BITS_1_MASK;
                decompressed[outOffset + 39] = (s9 >>> 43) & BITS_1_MASK;
                decompressed[outOffset + 40] = (s9 >>> 44) & BITS_1_MASK;
                decompressed[outOffset + 41] = (s9 >>> 45) & BITS_1_MASK;
                decompressed[outOffset + 42] = (s9 >>> 46) & BITS_1_MASK;
                decompressed[outOffset + 43] = (s9 >>> 47) & BITS_1_MASK;
                decompressed[outOffset + 44] = (s9 >>> 48) & BITS_1_MASK;
                decompressed[outOffset + 45] = (s9 >>> 49) & BITS_1_MASK;
                decompressed[outOffset + 46] = (s9 >>> 50) & BITS_1_MASK;
                decompressed[outOffset + 47] = (s9 >>> 51) & BITS_1_MASK;
                decompressed[outOffset + 48] = (s9 >>> 52) & BITS_1_MASK;
                decompressed[outOffset + 49] = (s9 >>> 53) & BITS_1_MASK;
                decompressed[outOffset + 50] = (s9 >>> 54) & BITS_1_MASK;
                decompressed[outOffset + 51] = (s9 >>> 55) & BITS_1_MASK;
                decompressed[outOffset + 52] = (s9 >>> 56) & BITS_1_MASK;
                decompressed[outOffset + 53] = (s9 >>> 57) & BITS_1_MASK;
                decompressed[outOffset + 54] = (s9 >>> 58) & BITS_1_MASK;
                decompressed[outOffset + 55] = (s9 >>> 59) & BITS_1_MASK;
                decompressed[outOffset + 56] = (s9 >>> 60) & BITS_1_MASK;
                decompressed[outOffset + 57] = (s9 >>> 61) & BITS_1_MASK;
                decompressed[outOffset + 58] = (s9 >>> 62) & BITS_1_MASK;
                decompressed[outOffset + 59] = (s9 >>> 63) & BITS_1_MASK;
                return 60;
            default:
                throw new IllegalArgumentException("Unknown Simple9 status: " + (s9 >>> NUM_DATA_BITS));
        }
    }

    @SuppressWarnings("WeakerAccess")
    public static void compress(
        LongBuffer compressedBuffer,
        long[] unCompressedData,
        @SuppressWarnings("SameParameterValue") int offset,
        int size
    ) {
        int encoded;
        while (size > 0) {
            encoded = compressSingle(unCompressedData, offset, size, compressedBuffer);
            offset += encoded;
            size -= encoded;
        }
    }

    @SuppressWarnings("WeakerAccess")
    public static int decompress(LongBuffer compressedBuffer, long[] unCompressedData) {
        int totalOut = 0;

        compressedBuffer.rewind();
        int unComprSize = unCompressedData.length;
        while (unComprSize > 0) {
            final int decoded = decompressSingle(compressedBuffer.get(), unCompressedData, totalOut);
            unComprSize -= decoded;
            totalOut += decoded;
        }
        return totalOut;
    }

}
