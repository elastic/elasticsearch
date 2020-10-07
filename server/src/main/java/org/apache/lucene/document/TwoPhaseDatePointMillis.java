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
package org.apache.lucene.document;

import org.apache.lucene.queries.TwoPhaseDateRangeQuery;
import org.apache.lucene.search.Query;

import java.time.Instant;


public final class TwoPhaseDatePointMillis {


  public static Field[] createIndexableFields(String name, Instant... instants) {
      Field[] fields = new Field[2*instants.length];
      for (int i = 0; i < instants.length; i++) {
          fields[2*i] = new LongPoint(name, instants[i].getEpochSecond());
          fields[2*i+1] = new SortedNumericDocValuesField(name, instants[i].toEpochMilli());
      }
      return fields;
  }

  // static methods for generating queries


  public static Query newExactQuery(String field, Instant instant) {
      return newRangeQuery(field, instant, instant);
  }


  public static Query newRangeQuery(String field, Instant lowerValue, Instant upperValue) {
      return new TwoPhaseDateRangeQuery(field, lowerValue, upperValue) {

          @Override
          protected long toApproxPrecision(Instant instant) {
              return instant.getEpochSecond();
          }

          @Override
          protected long toExactPrecision(Instant instant) {
              return instant.toEpochMilli();
          }
      };
  }
}
