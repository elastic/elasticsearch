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
     * Create a new query of a random type
     * @param r random seed
     * @return a random {@link QueryBuilder}
     */
    public static QueryBuilder createQuery(Random r) {
        switch (RandomInts.randomIntBetween(r, 0, 3)) {
            case 0:
                return new MatchAllQueryBuilderTest().createTestQueryBuilder();
            case 1:
                return new TermQueryBuilderTest().createTestQueryBuilder();
            case 2:
                return new IdsQueryBuilderTest().createTestQueryBuilder();
            case 3:
                return EmptyQueryBuilder.PROTOTYPE;
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Create a new invalid query of a random type
     * @param r random seed
     * @return a random {@link QueryBuilder} that is invalid, meaning that calling validate against it
     * will return an error. We can rely on the fact that a single error will be returned per query.
     */
    public static QueryBuilder createInvalidQuery(Random r) {
        switch (RandomInts.randomIntBetween(r, 0, 3)) {
            case 0:
                return new TermQueryBuilder("", "test");
            case 1:
                return new BoostingQueryBuilder(new MatchAllQueryBuilder(), new MatchAllQueryBuilder()).negativeBoost(-1f);
            case 2:
                return new CommonTermsQueryBuilder("", "text");
            case 3:
                return new SimpleQueryStringBuilder(null);
            default:
                throw new UnsupportedOperationException();
        }
    }
}
