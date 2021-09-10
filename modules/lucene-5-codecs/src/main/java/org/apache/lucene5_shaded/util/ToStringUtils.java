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
package org.apache.lucene5_shaded.util;

import org.apache.lucene5_shaded.search.BoostQuery;

/**
 * Helper methods to ease implementing {@link Object#toString()}.
 */
public final class ToStringUtils {

  private ToStringUtils() {} // no instance

  public static void byteArray(StringBuilder buffer, byte[] bytes) {
    for (int i = 0; i < bytes.length; i++) {
      buffer.append("b[").append(i).append("]=").append(bytes[i]);
      if (i < bytes.length - 1) {
        buffer.append(',');
      }

    }
  }

  private final static char [] HEX = "0123456789abcdef".toCharArray();

  public static String longHex(long x) {
    char [] asHex = new char [16];
    for (int i = 16; --i >= 0; x >>>= 4) {
      asHex[i] = HEX[(int) x & 0x0F];
    }
    return "0x" + new String(asHex);
  }

  /**
   * for printing boost only if not 1.0
   * @deprecated per-query boosts are deprecated in favour of {@link BoostQuery}
   */
  @Deprecated
  public static String boost(float boost) {
    if (boost != 1.0f) {
      return "^" + Float.toString(boost);
    } else return "";
  }
}
