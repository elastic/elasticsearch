package org.elasticsearch.index.analysis;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.test.junit.listeners.ReproduceInfoPrinter;
import org.junit.BeforeClass;

import java.util.Locale;

/**
 * @deprecated Remove when IndexableBinaryStringTools is removed.
 */
@Deprecated
@Listeners({
  ReproduceInfoPrinter.class
})
@ThreadLeakScope(Scope.NONE)
@TimeoutSuite(millis = TimeUnits.HOUR)
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
public class IndexableBinaryStringToolsTests extends LuceneTestCase {
  private static int NUM_RANDOM_TESTS;
  private static int MAX_RANDOM_BINARY_LENGTH;
  private static final String LINE_SEPARATOR = System.lineSeparator();

  @BeforeClass
  public static void beforeClass() throws Exception {
    NUM_RANDOM_TESTS = atLeast(200);
    MAX_RANDOM_BINARY_LENGTH = atLeast(300);
  }

  public void testSingleBinaryRoundTrip() {
    byte[] binary = new byte[] { (byte) 0x23, (byte) 0x98, (byte) 0x13,
        (byte) 0xE4, (byte) 0x76, (byte) 0x41, (byte) 0xB2, (byte) 0xC9,
        (byte) 0x7F, (byte) 0x0A, (byte) 0xA6, (byte) 0xD8 };

    int encodedLen = IndexableBinaryStringTools.getEncodedLength(binary, 0,
        binary.length);
    char encoded[] = new char[encodedLen];
    IndexableBinaryStringTools.encode(binary, 0, binary.length, encoded, 0,
        encoded.length);

    int decodedLen = IndexableBinaryStringTools.getDecodedLength(encoded, 0,
        encoded.length);
    byte decoded[] = new byte[decodedLen];
    IndexableBinaryStringTools.decode(encoded, 0, encoded.length, decoded, 0,
        decoded.length);

    assertEquals("Round trip decode/decode returned different results:"
        + LINE_SEPARATOR + "original: "
        + binaryDump(binary, binary.length)
        + LINE_SEPARATOR + " encoded: "
        + charArrayDump(encoded, encoded.length)
        + LINE_SEPARATOR + " decoded: "
        + binaryDump(decoded, decoded.length),
        binaryDump(binary, binary.length), binaryDump(decoded, decoded.length));
  }

  public void testEncodedSortability() {
    byte[] originalArray1 = new byte[MAX_RANDOM_BINARY_LENGTH];
    char[] originalString1 = new char[MAX_RANDOM_BINARY_LENGTH];
    char[] encoded1 = new char[MAX_RANDOM_BINARY_LENGTH * 10];
    byte[] original2 = new byte[MAX_RANDOM_BINARY_LENGTH];
    char[] originalString2 = new char[MAX_RANDOM_BINARY_LENGTH];
    char[] encoded2 = new char[MAX_RANDOM_BINARY_LENGTH * 10];

    for (int testNum = 0; testNum < NUM_RANDOM_TESTS; ++testNum) {
      int numBytes1 = random().nextInt(MAX_RANDOM_BINARY_LENGTH - 1) + 1; // Min == 1

      for (int byteNum = 0; byteNum < numBytes1; ++byteNum) {
        int randomInt = random().nextInt(0x100);
        originalArray1[byteNum] = (byte) randomInt;
        originalString1[byteNum] = (char) randomInt;
      }

      int numBytes2 = random().nextInt(MAX_RANDOM_BINARY_LENGTH - 1) + 1; // Min == 1

      for (int byteNum = 0; byteNum < numBytes2; ++byteNum) {
        int randomInt = random().nextInt(0x100);
        original2[byteNum] = (byte) randomInt;
        originalString2[byteNum] = (char) randomInt;
      }
      int originalComparison = new String(originalString1, 0, numBytes1)
          .compareTo(new String(originalString2, 0, numBytes2));
      originalComparison = originalComparison < 0 ? -1
          : originalComparison > 0 ? 1 : 0;

      int encodedLen1 = IndexableBinaryStringTools.getEncodedLength(
          originalArray1, 0, numBytes1);
      if (encodedLen1 > encoded1.length)
        encoded1 = new char[ArrayUtil.oversize(encodedLen1, Character.BYTES)];
      IndexableBinaryStringTools.encode(originalArray1, 0, numBytes1, encoded1,
          0, encodedLen1);

      int encodedLen2 = IndexableBinaryStringTools.getEncodedLength(original2,
          0, numBytes2);
      if (encodedLen2 > encoded2.length)
        encoded2 = new char[ArrayUtil.oversize(encodedLen2, Character.BYTES)];
      IndexableBinaryStringTools.encode(original2, 0, numBytes2, encoded2, 0,
          encodedLen2);

      int encodedComparison = new String(encoded1, 0, encodedLen1)
          .compareTo(new String(encoded2, 0, encodedLen2));
      encodedComparison = encodedComparison < 0 ? -1
          : encodedComparison > 0 ? 1 : 0;

      assertEquals("Test #" + (testNum + 1)
          + ": Original bytes and encoded chars compare differently:"
          + LINE_SEPARATOR + " binary 1: "
          + binaryDump(originalArray1, numBytes1)
          + LINE_SEPARATOR + " binary 2: "
          + binaryDump(original2, numBytes2)
          + LINE_SEPARATOR + "encoded 1: "
          + charArrayDump(encoded1, encodedLen1)
          + LINE_SEPARATOR + "encoded 2: "
          + charArrayDump(encoded2, encodedLen2)
          + LINE_SEPARATOR, originalComparison,
          encodedComparison);
    }
  }

  public void testEmptyInput() {
    byte[] binary = new byte[0];

    int encodedLen = IndexableBinaryStringTools.getEncodedLength(binary, 0,
        binary.length);
    char[] encoded = new char[encodedLen];
    IndexableBinaryStringTools.encode(binary, 0, binary.length, encoded, 0,
        encoded.length);

    int decodedLen = IndexableBinaryStringTools.getDecodedLength(encoded, 0,
        encoded.length);
    byte[] decoded = new byte[decodedLen];
    IndexableBinaryStringTools.decode(encoded, 0, encoded.length, decoded, 0,
        decoded.length);

    assertEquals("decoded empty input was not empty", decoded.length, 0);
  }

  public void testAllNullInput() {
    byte[] binary = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 };

    int encodedLen = IndexableBinaryStringTools.getEncodedLength(binary, 0,
        binary.length);
    char encoded[] = new char[encodedLen];
    IndexableBinaryStringTools.encode(binary, 0, binary.length, encoded, 0,
        encoded.length);

    int decodedLen = IndexableBinaryStringTools.getDecodedLength(encoded, 0,
        encoded.length);
    byte[] decoded = new byte[decodedLen];
    IndexableBinaryStringTools.decode(encoded, 0, encoded.length, decoded, 0,
        decoded.length);

    assertEquals("Round trip decode/decode returned different results:"
        + LINE_SEPARATOR + "  original: "
        + binaryDump(binary, binary.length)
        + LINE_SEPARATOR + "decodedBuf: "
        + binaryDump(decoded, decoded.length),
        binaryDump(binary, binary.length), binaryDump(decoded, decoded.length));
  }

  public void testRandomBinaryRoundTrip() {
    byte[] binary = new byte[MAX_RANDOM_BINARY_LENGTH];
    char[] encoded = new char[MAX_RANDOM_BINARY_LENGTH * 10];
    byte[] decoded = new byte[MAX_RANDOM_BINARY_LENGTH];
    for (int testNum = 0; testNum < NUM_RANDOM_TESTS; ++testNum) {
      int numBytes = random().nextInt(MAX_RANDOM_BINARY_LENGTH - 1) + 1; // Min == 1

      for (int byteNum = 0; byteNum < numBytes; ++byteNum) {
        binary[byteNum] = (byte) random().nextInt(0x100);
      }

      int encodedLen = IndexableBinaryStringTools.getEncodedLength(binary, 0,
          numBytes);
      if (encoded.length < encodedLen)
        encoded = new char[ArrayUtil.oversize(encodedLen, Character.BYTES)];
      IndexableBinaryStringTools.encode(binary, 0, numBytes, encoded, 0,
          encodedLen);

      int decodedLen = IndexableBinaryStringTools.getDecodedLength(encoded, 0,
          encodedLen);
      IndexableBinaryStringTools.decode(encoded, 0, encodedLen, decoded, 0,
          decodedLen);

      assertEquals("Test #" + (testNum + 1)
          + ": Round trip decode/decode returned different results:"
          + LINE_SEPARATOR + "  original: "
          + binaryDump(binary, numBytes) + LINE_SEPARATOR
          + "encodedBuf: " + charArrayDump(encoded, encodedLen)
          + LINE_SEPARATOR + "decodedBuf: "
          + binaryDump(decoded, decodedLen), binaryDump(binary, numBytes),
          binaryDump(decoded, decodedLen));
    }
  }

  public String binaryDump(byte[] binary, int numBytes) {
    StringBuilder buf = new StringBuilder();
    for (int byteNum = 0 ; byteNum < numBytes ; ++byteNum) {
      String hex = Integer.toHexString(binary[byteNum] & 0xFF);
      if (hex.length() == 1) {
        buf.append('0');
      }
      buf.append(hex.toUpperCase(Locale.ROOT));
      if (byteNum < numBytes - 1) {
        buf.append(' ');
      }
    }
    return buf.toString();
  }

  public String charArrayDump(char[] charArray, int numBytes) {
    StringBuilder buf = new StringBuilder();
    for (int charNum = 0 ; charNum < numBytes ; ++charNum) {
      String hex = Integer.toHexString(charArray[charNum]);
      for (int digit = 0 ; digit < 4 - hex.length() ; ++digit) {
        buf.append('0');
      }
      buf.append(hex.toUpperCase(Locale.ROOT));
      if (charNum < numBytes - 1) {
        buf.append(' ');
      }
    }
    return buf.toString();
  }
}
