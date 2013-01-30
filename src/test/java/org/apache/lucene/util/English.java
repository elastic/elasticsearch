package org.apache.lucene.util;
// LUCENE WATCH - if we use the test-framework we can trash this class
/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

/**
 * Converts numbers to english strings for testing.
 * @lucene.internal
 */ 
public final class English {

  private English() {} // no instance

  public static String longToEnglish(long i) {
    StringBuilder result = new StringBuilder();
    longToEnglish(i, result);
    return result.toString();
  }

  public static void longToEnglish(long i, StringBuilder result) {
    if (i == 0) {
      result.append("zero");
      return;
    }
    if (i < 0) {
      result.append("minus ");
      i = -i;
    }
    if (i >= 1000000000000000000l) {        // quadrillion
      longToEnglish(i / 1000000000000000000l, result);
      result.append("quintillion, ");
      i = i % 1000000000000000000l;
    }
    if (i >= 1000000000000000l) {        // quadrillion
      longToEnglish(i / 1000000000000000l, result);
      result.append("quadrillion, ");
      i = i % 1000000000000000l;
    }
    if (i >= 1000000000000l) {        // trillions
      longToEnglish(i / 1000000000000l, result);
      result.append("trillion, ");
      i = i % 1000000000000l;
    }
    if (i >= 1000000000) {        // billions
      longToEnglish(i / 1000000000, result);
      result.append("billion, ");
      i = i % 1000000000;
    }
    if (i >= 1000000) {          // millions
      longToEnglish(i / 1000000, result);
      result.append("million, ");
      i = i % 1000000;
    }
    if (i >= 1000) {          // thousands
      longToEnglish(i / 1000, result);
      result.append("thousand, ");
      i = i % 1000;
    }
    if (i >= 100) {          // hundreds
      longToEnglish(i / 100, result);
      result.append("hundred ");
      i = i % 100;
    }
    //we know we are smaller here so we can cast
    if (i >= 20) {
      switch (((int) i) / 10) {
        case 9:
          result.append("ninety");
          break;
        case 8:
          result.append("eighty");
          break;
        case 7:
          result.append("seventy");
          break;
        case 6:
          result.append("sixty");
          break;
        case 5:
          result.append("fifty");
          break;
        case 4:
          result.append("forty");
          break;
        case 3:
          result.append("thirty");
          break;
        case 2:
          result.append("twenty");
          break;
      }
      i = i % 10;
      if (i == 0)
        result.append(" ");
      else
        result.append("-");
    }
    switch ((int) i) {
      case 19:
        result.append("nineteen ");
        break;
      case 18:
        result.append("eighteen ");
        break;
      case 17:
        result.append("seventeen ");
        break;
      case 16:
        result.append("sixteen ");
        break;
      case 15:
        result.append("fifteen ");
        break;
      case 14:
        result.append("fourteen ");
        break;
      case 13:
        result.append("thirteen ");
        break;
      case 12:
        result.append("twelve ");
        break;
      case 11:
        result.append("eleven ");
        break;
      case 10:
        result.append("ten ");
        break;
      case 9:
        result.append("nine ");
        break;
      case 8:
        result.append("eight ");
        break;
      case 7:
        result.append("seven ");
        break;
      case 6:
        result.append("six ");
        break;
      case 5:
        result.append("five ");
        break;
      case 4:
        result.append("four ");
        break;
      case 3:
        result.append("three ");
        break;
      case 2:
        result.append("two ");
        break;
      case 1:
        result.append("one ");
        break;
      case 0:
        result.append("");
        break;
    }
  }


  public static String intToEnglish(int i) {
    StringBuilder result = new StringBuilder();
    longToEnglish(i, result);
    return result.toString();
  }

  public static void intToEnglish(int i, StringBuilder result) {
    longToEnglish(i, result);
  }
}
