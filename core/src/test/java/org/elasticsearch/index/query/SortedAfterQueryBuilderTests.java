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

import org.apache.lucene.queries.SearchAfterSortedDocQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class SortedAfterQueryBuilderTests extends AbstractQueryTestCase<SortedAfterQueryBuilder> {
    private static final String[] FIELD_NAMES = new String[] { STRING_FIELD_NAME_2, INT_FIELD_NAME, DATE_FIELD_NAME };
    private static String[] RANDOM_SORT_FIELDS;

    @BeforeClass
    private static void setRandomSortFields() {
        int numFields = randomIntBetween(1, 5);
        RANDOM_SORT_FIELDS = new String[numFields];
        for (int i = 0; i < numFields; i++) {
            RANDOM_SORT_FIELDS[i] = randomFrom(FIELD_NAMES);
        }
    }

    @AfterClass
    private static void unsetRandomSortFields() {
        RANDOM_SORT_FIELDS = null;
    }

    @Override
    protected Settings indexSettings() {
        return Settings.builder().putArray("index.sort.field", RANDOM_SORT_FIELDS).build();
    }

    @Override
    protected SortedAfterQueryBuilder doCreateTestQueryBuilder() {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        SortedAfterQueryBuilder builder = new SortedAfterQueryBuilder();
        Object[] values = new Object[RANDOM_SORT_FIELDS.length];
        for (int i = 0; i < values.length; i++) {
            switch (RANDOM_SORT_FIELDS[i]) {
                case STRING_FIELD_NAME_2:
                    values[i] = randomAlphaOfLengthBetween(1, 35);
                    break;
                case INT_FIELD_NAME:
                    values[i] = randomIntBetween(-10000, 10000);
                    break;
                case DATE_FIELD_NAME:
                    values[i] = randomNonNegativeLong();
                    break;
                default:
                    assert (false);
            }
        }
        builder.setSortValues(values);
        return builder;
    }

    @Override
    protected void doAssertLuceneQuery(SortedAfterQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        assertThat(query, instanceOf(SearchAfterSortedDocQuery.class));
    }

    @Override
    public void testUnknownField() {
        String marker = "#marker#";
        SortedAfterQueryBuilder testQuery;
        do {
            testQuery = createTestQueryBuilder();
        } while (testQuery.toString().contains(marker));
        testQuery.queryName(marker); // to find root query to add additional bogus field there
        String queryAsString = testQuery.toString().replace("\"" + marker + "\"", "\"" + marker + "\", \"bogusField\" : \"someValue\"");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parseQuery(queryAsString));
        assertThat(e.getMessage(), containsString("bogusField"));
    }
}
