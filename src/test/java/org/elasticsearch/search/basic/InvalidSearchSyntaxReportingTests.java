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

package org.elasticsearch.search.basic;

import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.transport.RemoteTransportException;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class InvalidSearchSyntaxReportingTests extends ElasticsearchIntegrationTest {

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    private void prepareData(int numShards) throws Exception {

        ImmutableSettings.Builder settingsBuilder = settingsBuilder().put(indexSettings());

        if (numShards > 0) {
            settingsBuilder.put(SETTING_NUMBER_OF_SHARDS, numShards);
        }

        client().admin().indices().create(createIndexRequest("test").settings(settingsBuilder)).actionGet();

        ensureGreen();
        refresh();
    }

    @Test
    public void testBadQueryReporting() throws Exception {
        assertAcked(prepareCreate("test"));
        ensureGreen();

        String badQuery = "{\"match_all\":{\"nonsense\":3}}";
        ToXContent errorReporter = null;
        try {
            client().prepareSearch("test").setQuery(badQuery).execute().actionGet();
        } catch (SearchPhaseExecutionException expected) {
            if (expected.hasXContent()) {
                errorReporter = expected;
            }
        }
        assertNotNull("Expected a class that can give breakdown of parse error", errorReporter);
        XContentBuilder jb = XContentFactory.jsonBuilder();
        errorReporter.toXContent(jb, ToXContent.EMPTY_PARAMS);
        String jsonResponse = jb.string();
        assertTrue("Bad section of query should be in message", jsonResponse.contains("nonsense"));
    }

    // A custom exception used to illustrate the possibility of new forms of
    // user input exception which can return arbitrary XContent in results
    private static final class CustomParseException extends Exception implements ToXContent {
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("parseFailure");
            builder.field("reason", "you have exceeded rate limits");
            builder.field("try_again_in", 5);
            builder.endObject();
            return builder;
        }
    }

    @Test
    public void testSerializationOfNewExceptionReporting() throws Exception {
        // For the bulk searches (deleteByQuery, multiSearch) the operations
        // rely
        // on ShardSearchFailure implementing Streamable correctly and
        // serializing arbitrary
        // errors
        ShardSearchFailure sf1 = new ShardSearchFailure(new RemoteTransportException("Remote", new CustomParseException()));
        assertTrue(sf1.hasXContent());
        BytesStreamOutput bos = new BytesStreamOutput();
        sf1.writeTo(bos);
        BytesReference bytes = bos.bytes();
        BytesStreamInput bis = new BytesStreamInput(bytes);
        sf1.readFrom(bis);
        XContentBuilder jb = XContentFactory.yamlBuilder();
        assertTrue(sf1.hasXContent());
        sf1.toXContent(jb, ToXContent.EMPTY_PARAMS);
        jb.close();
        String errorResponse = jb.string();
        assertTrue("error should include message", errorResponse.contains("you have exceeded rate limits"));
    }

    @Test
    public void testFailedMultiSearchWithWrongQuery_withFunctionScore() throws Exception {
        assertAcked(prepareCreate("test"));
        ensureGreen();

        String badQuery1 = "{\"match_all\":{\"nonsense\":3}}";
        String goodQuery = "{\"match_all\":{}}";
        String badQuery2 = "{\"match_all\":{\"nonsense2\":3}}";

        MultiSearchResponse response = client().prepareMultiSearch()
                // Add custom score query with missing script
                .add(client().prepareSearch("test").setQuery(badQuery1)).add(client().prepareSearch("test").setQuery(goodQuery))
                .add(client().prepareSearch("test").setQuery(badQuery2)).execute().actionGet();
        assertThat(response.getResponses().length, equalTo(3));
        assertThat(response.getResponses()[0].getFailureMessage(), notNullValue());
        assertThat(response.getResponses()[1].getFailureMessage(), nullValue());
        assertThat(response.getResponses()[2].getFailureMessage(), notNullValue());
    }
}
