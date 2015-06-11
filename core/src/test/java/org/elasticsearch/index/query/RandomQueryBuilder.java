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

package org.elasticsearch.index.query;

import com.carrotsearch.randomizedtesting.generators.RandomInts;

import java.util.Random;

/**
 * Utility class for creating random QueryBuilders.
 * So far only leaf queries like {@link MatchAllQueryBuilder}, {@link TermQueryBuilder} or
 * {@link IdsQueryBuilder} are returned.
 */
public class RandomQueryBuilder {

    /**
     * @param r random seed
     * @return a random {@link QueryBuilder}
     */
    public static QueryBuilder create(Random r) {
        QueryBuilder query = null;
        switch (RandomInts.randomIntBetween(r, 0, 2)) {
        case 0:
            return new MatchAllQueryBuilderTest().createTestQueryBuilder();
        case 1:
            return new TermQueryBuilderTest().createTestQueryBuilder();
        case 2:
            return new IdsQueryBuilderTest().createTestQueryBuilder();
        }
        return query;
    }
}
