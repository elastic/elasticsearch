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

package org.elasticsearch.search.sort;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Set;

public class RandomSortDataGenerator {
    private RandomSortDataGenerator() {
        // this is a helper class only, doesn't need a constructor
    }

    public static QueryBuilder nestedFilter(QueryBuilder original) {
        @SuppressWarnings("rawtypes")
        QueryBuilder nested = null;
        while (nested == null || nested.equals(original)) {
            switch (ESTestCase.randomInt(2)) {
            case 0:
                nested = new MatchAllQueryBuilder();
                break;
            case 1:
                nested = new IdsQueryBuilder();
                break;
            default:
            case 2:
                nested = new TermQueryBuilder(ESTestCase.randomAsciiOfLengthBetween(1, 10), ESTestCase.randomDouble());
                break;
            }
            nested.boost((float) ESTestCase.randomDoubleBetween(0, 10, false));
        }
        return nested;
    }

    public static Object missing(Object original) {
        Object missing = null;
        Object otherMissing = original;

        while (missing == null || missing.equals(otherMissing)) {
          int missingId = ESTestCase.randomIntBetween(0, 4);
          switch (missingId) {
          case 0:
              missing = ("_last");
              break;
          case 1:
              missing = ("_first");
              break;
          case 2:
              missing = ESTestCase.randomAsciiOfLength(10);
              break;
          case 3:
              missing = ESTestCase.randomUnicodeOfCodepointLengthBetween(5, 15);
              break;
          case 4:
              missing = ESTestCase.randomInt();
              break;
          default:
              throw new IllegalStateException("Unknown missing type.");

          }
        }
        return missing;
    }

    /**
     * return a random {@link SortOrder} settings, except the one provided by parameter if set
     */
    public static SortOrder order(@Nullable SortOrder original) {
        if (original == null) {
            return ESTestCase.randomBoolean() ? SortOrder.ASC : SortOrder.DESC;
        }
        if (original.equals(SortOrder.ASC)) {
            return SortOrder.DESC;
        } else {
            return SortOrder.ASC;
        }
    }

}
