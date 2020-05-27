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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.cluster.metadata.IndexTemplateV2;
import org.elasticsearch.cluster.metadata.IndexTemplateV2.DataStreamTemplate;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AutoCreateActionTests extends ESTestCase {

    public void testResolveAutoCreateDataStreams() {
        Metadata metadata;
        {
            Metadata.Builder mdBuilder = new Metadata.Builder();
            DataStreamTemplate dataStreamTemplate = new DataStreamTemplate("@timestamp");
            mdBuilder.put("1", new IndexTemplateV2(List.of("legacy-logs-*"), null, null, 10L, null, null, null));
            mdBuilder.put("2", new IndexTemplateV2(List.of("logs-*"), null, null, 20L, null, null, dataStreamTemplate));
            mdBuilder.put("3", new IndexTemplateV2(List.of("logs-foobar"), null, null, 30L, null, null, dataStreamTemplate));
            metadata = mdBuilder.build();
        }

        CreateIndexRequest request = new CreateIndexRequest("logs-foobar");
        DataStreamTemplate result  = AutoCreateAction.resolveAutoCreateDataStream(request, metadata);
        assertThat(result, notNullValue());
        assertThat(result.getTimestampField(), equalTo("@timestamp"));

        request = new CreateIndexRequest("logs-barbaz");
        result  = AutoCreateAction.resolveAutoCreateDataStream(request, metadata);
        assertThat(result, notNullValue());
        assertThat(result.getTimestampField(), equalTo("@timestamp"));

        // An index that matches with a template without a data steam definition
        request = new CreateIndexRequest("legacy-logs-foobaz");
        result = AutoCreateAction.resolveAutoCreateDataStream(request, metadata);
        assertThat(result, nullValue());

        // An index that doesn't match with an index template
        request = new CreateIndexRequest("my-index");
        result = AutoCreateAction.resolveAutoCreateDataStream(request, metadata);
        assertThat(result, nullValue());
    }

}
