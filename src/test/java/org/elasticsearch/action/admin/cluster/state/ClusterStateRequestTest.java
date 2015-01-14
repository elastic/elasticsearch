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

package org.elasticsearch.action.admin.cluster.state;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.IndicesOptionsTests;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class ClusterStateRequestTest extends ElasticsearchTestCase {

    @Test
    public void testSerialization() throws Exception {
        int iterations = randomIntBetween(5, 20);
        for (int i = 0; i < iterations; i++) {

            IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
            ClusterStateRequest clusterStateRequest = new ClusterStateRequest().routingTable(randomBoolean()).metaData(randomBoolean())
                    .nodes(randomBoolean()).blocks(randomBoolean()).indices("testindex", "testindex2").indicesOptions(indicesOptions);

            BytesStreamOutput output = new BytesStreamOutput();
            Version outputVersion = randomVersion();
            output.setVersion(outputVersion);
            clusterStateRequest.writeTo(output);

            BytesStreamInput bytesStreamInput = new BytesStreamInput(output.bytes());
            bytesStreamInput.setVersion(randomVersion());
            ClusterStateRequest clusterStateRequest2 = new ClusterStateRequest();
            clusterStateRequest2.readFrom(bytesStreamInput);

            assertThat(clusterStateRequest2.routingTable(), equalTo(clusterStateRequest.routingTable()));
            assertThat(clusterStateRequest2.metaData(), equalTo(clusterStateRequest.metaData()));
            assertThat(clusterStateRequest2.nodes(), equalTo(clusterStateRequest.nodes()));
            assertThat(clusterStateRequest2.blocks(), equalTo(clusterStateRequest.blocks()));
            assertThat(clusterStateRequest2.indices(), equalTo(clusterStateRequest.indices()));

            assertThat(clusterStateRequest2.indicesOptions().ignoreUnavailable(), equalTo(clusterStateRequest.indicesOptions()
                    .ignoreUnavailable()));
            assertThat(clusterStateRequest2.indicesOptions().expandWildcardsOpen(), equalTo(clusterStateRequest.indicesOptions()
                    .expandWildcardsOpen()));
            assertThat(clusterStateRequest2.indicesOptions().expandWildcardsClosed(), equalTo(clusterStateRequest.indicesOptions()
                    .expandWildcardsClosed()));
            assertThat(clusterStateRequest2.indicesOptions().allowNoIndices(), equalTo(clusterStateRequest.indicesOptions()
                    .allowNoIndices()));

        }
    }

}
