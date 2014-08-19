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

package org.elasticsearch.action.count;


import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;

public class CountRequestBuilderTests extends ElasticsearchIntegrationTest {
    //This would work better as a TestCase but CountRequestBuilder construction
    //requires a client

    @Test
    public void testSearchRequestBuilderToString(){
        try
        {
            CountRequestBuilder countRequestBuilder = new CountRequestBuilder(client());
            XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
            contentBuilder.startObject()
                    .field("query")
                    .startObject()
                    .field("match_all")
                    .startObject()
                    .endObject()
                    .endObject()
                    .endObject();
            countRequestBuilder.setSource(contentBuilder.bytes());
            assertEquals(countRequestBuilder.toString(), XContentHelper.convertToJson(contentBuilder.bytes(), false, false));
        } catch (IOException ie) {
            throw new ElasticsearchException("Unable to create content builder", ie);
        }
        try
        {
            CountRequestBuilder countRequestBuilder = new CountRequestBuilder(client());
            XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
            logger.debug(contentBuilder.toString()); //This should not affect things
            contentBuilder.startObject()
                    .field("query")
                    .startObject()
                    .field("match_all")
                    .startObject()
                    .endObject()
                    .endObject()
                    .endObject();
            countRequestBuilder.setSource(contentBuilder.bytes());
            assertEquals(countRequestBuilder.toString(), XContentHelper.convertToJson(contentBuilder.bytes(), false, false));

        } catch (IOException ie) {
            throw new ElasticsearchException("Unable to create content builder", ie);
        }
    }

}