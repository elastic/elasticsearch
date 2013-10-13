/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.action.suggest;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder.SuggestionBuilder;
import org.elasticsearch.search.suggest.SuggestSearchTests;

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

/**
 */
public class SuggestActionTests extends SuggestSearchTests {

    protected Suggest searchSuggest(Client client, String suggestText, int expectShardsFailed, SuggestionBuilder<?>... suggestions) {
        SuggestRequestBuilder builder = client.prepareSuggest();

        if (suggestText != null) {
            builder.setSuggestText(suggestText);
        }
        for (SuggestionBuilder<?> suggestion : suggestions) {
            builder.addSuggestion(suggestion);
        }

        SuggestResponse actionGet = builder.execute().actionGet();
        assertThat(Arrays.toString(actionGet.getShardFailures()), actionGet.getFailedShards(), equalTo(expectShardsFailed));
        if (expectShardsFailed > 0) {
            throw new SearchPhaseExecutionException("suggest", "Suggest execution failed", new ShardSearchFailure[0]);
        }
        return actionGet.getSuggest();

    }
}
