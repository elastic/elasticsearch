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

package org.elasticsearch.index;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import java.util.Arrays;

import static org.elasticsearch.index.query.TermsFilterParser.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class TermsFilterIntegrationTests extends ElasticsearchIntegrationTest {

    private final ESLogger logger = Loggers.getLogger(TermsFilterIntegrationTests.class);

    public void testExecution() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", "f", "type=string"));
        ensureYellow();
        indexRandom(true,
                client().prepareIndex("test", "type").setSource("f", new String[] {"a", "b", "c"}),
                client().prepareIndex("test", "type").setSource("f", "b"));

        for (boolean cache : new boolean[] {false, true}) {
            logger.info("cache=" + cache);
            for (String execution : Arrays.asList(
                    EXECUTION_VALUE_PLAIN,
                    EXECUTION_VALUE_FIELDDATA,
                    EXECUTION_VALUE_BOOL,
                    EXECUTION_VALUE_BOOL_NOCACHE,
                    EXECUTION_VALUE_OR,
                    EXECUTION_VALUE_OR_NOCACHE)) {
                logger.info("Execution=" + execution);
                assertHitCount(client().prepareCount("test").setQuery(
                        QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),
                                FilterBuilders.termsFilter("f", "a", "b").execution(execution).cache(cache))).get(), 2L);
            }

            for (String execution : Arrays.asList(
                    EXECUTION_VALUE_AND,
                    EXECUTION_VALUE_AND_NOCACHE)) {
                logger.info("Execution=" + execution);
                assertHitCount(client().prepareCount("test").setQuery(
                        QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),
                                FilterBuilders.termsFilter("f", "a", "b").execution(execution).cache(cache))).get(), 1L);
            }
        }
    }

}
