/*
 * Licensed to Elasticsearch (the "Author") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Author licenses this
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

package org.elasticsearch.index.analysis;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.*;

@ElasticsearchIntegrationTest.ClusterScope(numDataNodes = 1, scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class SimpleSmartChineseIntegrationTests extends ElasticsearchIntegrationTest {

    @Test
    public void testSmartcnAnalyzer() throws ExecutionException, InterruptedException {
        AnalyzeResponse response = client().admin().indices()
                .prepareAnalyze("叻出色").setAnalyzer("smartcn")
                .execute().get();

        assertThat(response, notNullValue());
        assertThat(response.getTokens().size(), is(2));
    }

    @Test
    public void testSmartcnTokenizer() throws ExecutionException, InterruptedException {
        AnalyzeResponse response = client().admin().indices()
                .prepareAnalyze("叻出色").setTokenizer("smartcn_sentence")
                .execute().get();

        assertThat(response, notNullValue());
        assertThat(response.getTokens().size(), is(1));
    }

    @Test
    public void testSmartcnTokenFilter() throws ExecutionException, InterruptedException {
        AnalyzeResponse response = client().admin().indices()
                .prepareAnalyze("叻出色").setTokenFilters("smartcn_word")
                .execute().get();

        assertThat(response, notNullValue());
        assertThat(response.getTokens().size(), is(3));
    }
}
