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

import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.ESTestCase;

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

}
