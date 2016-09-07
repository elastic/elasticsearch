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

package org.elasticsearch.test;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.set.Sets;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static org.elasticsearch.test.AbstractQueryTestCase.alterateQueries;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Various test for {@link org.elasticsearch.test.AbstractQueryTestCase}
 */
public class AbstractQueryTestCaseTests extends ESTestCase {

    public void testAlterateQueries() throws IOException {
        List<Tuple<String, Boolean>> alterations = alterateQueries(singleton("{\"field\": \"value\"}"), null);
        assertAlterations(alterations, allOf(notNullValue(), hasEntry("{\"newField\":{\"field\":\"value\"}}", true)));

        alterations = alterateQueries(singleton("{\"term\":{\"field\": \"value\"}}"), null);
        assertAlterations(alterations, allOf(
            hasEntry("{\"newField\":{\"term\":{\"field\":\"value\"}}}", true),
            hasEntry("{\"term\":{\"newField\":{\"field\":\"value\"}}}", true))
        );

        alterations = alterateQueries(singleton("{\"bool\":{\"must\": [{\"match\":{\"field\":\"value\"}}]}}"), null);
        assertAlterations(alterations, allOf(
                hasEntry("{\"newField\":{\"bool\":{\"must\":[{\"match\":{\"field\":\"value\"}}]}}}", true),
                hasEntry("{\"bool\":{\"newField\":{\"must\":[{\"match\":{\"field\":\"value\"}}]}}}", true),
                hasEntry("{\"bool\":{\"must\":[{\"newField\":{\"match\":{\"field\":\"value\"}}}]}}", true),
                hasEntry("{\"bool\":{\"must\":[{\"match\":{\"newField\":{\"field\":\"value\"}}}]}}", true)
        ));

        alterations = alterateQueries(singleton("{\"function_score\":" +
                "{\"query\": {\"term\":{\"foo\": \"bar\"}}, \"script_score\": {\"script\":\"a + 1\", \"params\": {\"a\":0}}}}"), null);
        assertAlterations(alterations, allOf(
                hasEntry("{\"newField\":{\"function_score\":{\"query\":{\"term\":{\"foo\":\"bar\"}},\"script_score\":{\"script\":\"a + " +
                        "1\",\"params\":{\"a\":0}}}}}", true),
                hasEntry("{\"function_score\":{\"newField\":{\"query\":{\"term\":{\"foo\":\"bar\"}},\"script_score\":{\"script\":\"a + " +
                        "1\",\"params\":{\"a\":0}}}}}", true),
                hasEntry("{\"function_score\":{\"query\":{\"newField\":{\"term\":{\"foo\":\"bar\"}}},\"script_score\":{\"script\":\"a + " +
                        "1\",\"params\":{\"a\":0}}}}", true),
                hasEntry("{\"function_score\":{\"query\":{\"term\":{\"newField\":{\"foo\":\"bar\"}}},\"script_score\":{\"script\":\"a + " +
                        "1\",\"params\":{\"a\":0}}}}", true),
                hasEntry("{\"function_score\":{\"query\":{\"term\":{\"foo\":\"bar\"}},\"script_score\":{\"newField\":{\"script\":\"a + " +
                        "1\",\"params\":{\"a\":0}}}}}", true),
                hasEntry("{\"function_score\":{\"query\":{\"term\":{\"foo\":\"bar\"}},\"script_score\":{\"script\":\"a + 1\"," +
                        "\"params\":{\"newField\":{\"a\":0}}}}}", true)
        ));
    }

    public void testAlterateQueriesWithArbitraryContent() throws IOException {
        Set<String> arbitraryContentHolders = Sets.newHashSet("params", "doc");
        Set<String> queries = Sets.newHashSet(
                "{\"query\":{\"script\":\"test\",\"params\":{\"foo\":\"bar\"}}}",
                "{\"query\":{\"more_like_this\":{\"fields\":[\"a\",\"b\"],\"like\":{\"doc\":{\"c\":\"d\"}}}}}"
        );

        List<Tuple<String, Boolean>> alterations = alterateQueries(queries, arbitraryContentHolders);
        assertAlterations(alterations, allOf(
            hasEntry("{\"newField\":{\"query\":{\"script\":\"test\",\"params\":{\"foo\":\"bar\"}}}}", true),
            hasEntry("{\"query\":{\"newField\":{\"script\":\"test\",\"params\":{\"foo\":\"bar\"}}}}", true),
            hasEntry("{\"query\":{\"script\":\"test\",\"params\":{\"newField\":{\"foo\":\"bar\"}}}}", false)
        ));
        assertAlterations(alterations, allOf(
            hasEntry("{\"newField\":{\"query\":{\"more_like_this\":{\"fields\":[\"a\",\"b\"],\"like\":{\"doc\":{\"c\":\"d\"}}}}}}", true),
            hasEntry("{\"query\":{\"newField\":{\"more_like_this\":{\"fields\":[\"a\",\"b\"],\"like\":{\"doc\":{\"c\":\"d\"}}}}}}", true),
            hasEntry("{\"query\":{\"more_like_this\":{\"newField\":{\"fields\":[\"a\",\"b\"],\"like\":{\"doc\":{\"c\":\"d\"}}}}}}", true),
            hasEntry("{\"query\":{\"more_like_this\":{\"fields\":[\"a\",\"b\"],\"like\":{\"newField\":{\"doc\":{\"c\":\"d\"}}}}}}", true),
            hasEntry("{\"query\":{\"more_like_this\":{\"fields\":[\"a\",\"b\"],\"like\":{\"doc\":{\"newField\":{\"c\":\"d\"}}}}}}", false)
        ));
    }

    private static <K, V> void assertAlterations(List<Tuple<K, V>> alterations, Matcher<Map<K, V>> matcher) {
        assertThat(alterations.stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2)), matcher);
    }
}
