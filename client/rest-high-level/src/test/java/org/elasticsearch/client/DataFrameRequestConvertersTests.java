/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.dataframe.DeleteDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.PutDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfigTests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class DataFrameRequestConvertersTests extends ESTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public void testPutDataFrameTransform() throws IOException {
        PutDataFrameTransformRequest putRequest = new PutDataFrameTransformRequest(
                DataFrameTransformConfigTests.randomDataFrameTransformConfig());
        Request request = DataFrameRequestConverters.putDataFrameTransform(putRequest);

        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_data_frame/transforms/" + putRequest.getConfig().getId()));

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            DataFrameTransformConfig parsedConfig = DataFrameTransformConfig.PARSER.apply(parser, null);
            assertThat(parsedConfig, equalTo(putRequest.getConfig()));
        }
    }

    public void testDeleteDataFrameTransform() {
        DeleteDataFrameTransformRequest deleteRequest = new DeleteDataFrameTransformRequest("foo");
        Request request = DataFrameRequestConverters.deleteDataFrameTransform(deleteRequest);

        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_data_frame/transforms/foo"));
    }
}
