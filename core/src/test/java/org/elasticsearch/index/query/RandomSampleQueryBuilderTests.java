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


import org.apache.lucene.search.Query;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RandomSampleQueryBuilderTests extends AbstractQueryTestCase<RandomSampleQueryBuilder> {

    @Override
    protected RandomSampleQueryBuilder doCreateTestQueryBuilder() {
        RandomSampleQueryBuilder query;
        double p = randomDoubleBetween(0.00001, 0.999999, true);
        if (randomBoolean()) {
            query = new RandomSampleQueryBuilder(p, randomLong());
        } else {
            query = new RandomSampleQueryBuilder(p);
        }
        return query;
    }

    @Override
    protected void doAssertLuceneQuery(RandomSampleQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        assertThat(query, instanceOf(RandomSampleQuery.class));
    }

    public void testIllegalArguments() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new RandomSampleQueryBuilder(0.0));
        assertEquals("[probability] cannot be less than or equal to 0.0.", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new RandomSampleQueryBuilder(-5.0));
        assertEquals("[probability] cannot be less than or equal to 0.0.", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new RandomSampleQueryBuilder(1.0));
        assertEquals("[probability] cannot be greater than or equal to 1.0.", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new RandomSampleQueryBuilder(5.0));
        assertEquals("[probability] cannot be greater than or equal to 1.0.", e.getMessage());

    }


    public void testFromJson() throws IOException {
        String json =
            "{\n" +
                "  \"random_sample\" : {\n" +
                "    \"boost\" : 1.0,\n" +
                "    \"probability\" : 0.5\n" +
                "  }\n" +
                "}";
        RandomSampleQueryBuilder parsed = (RandomSampleQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertThat(parsed.getProbability(), equalTo(0.5));
        assertThat(parsed.getSeed(), nullValue());

        // try with seed
        json =
            "{\n" +
                "  \"random_sample\" : {\n" +
                "    \"boost\" : 1.0,\n" +
                "    \"probability\" : 0.5,\n" +
                "    \"seed\" : 123\n" +
                "  }\n" +
                "}";
        parsed = (RandomSampleQueryBuilder) parseQuery(json);
        assertThat(parsed.getProbability(), equalTo(0.5));
        assertThat(parsed.getSeed(), equalTo(123L));

    }
}
