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

package org.elasticsearch.client.eql;

import org.elasticsearch.client.AbstractRequestTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchModule;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class EqlSearchRequestTests extends AbstractRequestTestCase<EqlSearchRequest, org.elasticsearch.xpack.eql.action.EqlSearchRequest> {

    @Override
    protected EqlSearchRequest createClientTestInstance() {
        EqlSearchRequest EqlSearchRequest = new EqlSearchRequest("testindex", randomAlphaOfLength(40));
        if (randomBoolean()) {
            EqlSearchRequest.fetchSize(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            EqlSearchRequest.eventCategoryField(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            EqlSearchRequest.query(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            EqlSearchRequest.timestampField(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            EqlSearchRequest.tiebreakerField(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            if (randomBoolean()) {
                EqlSearchRequest.filter(QueryBuilders.matchAllQuery());
            } else {
                EqlSearchRequest.filter(QueryBuilders.termQuery(randomAlphaOfLength(10), randomInt(100)));
            }
        }
        return EqlSearchRequest;
    }

    @Override
    protected org.elasticsearch.xpack.eql.action.EqlSearchRequest doParseToServerInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.xpack.eql.action.EqlSearchRequest.fromXContent(parser).indices("testindex");
    }

    @Override
    protected void assertInstances(org.elasticsearch.xpack.eql.action.EqlSearchRequest serverInstance, EqlSearchRequest
        clientTestInstance) {
        assertThat(serverInstance.eventCategoryField(), equalTo(clientTestInstance.eventCategoryField()));
        assertThat(serverInstance.timestampField(), equalTo(clientTestInstance.timestampField()));
        assertThat(serverInstance.tiebreakerField(), equalTo(clientTestInstance.tiebreakerField()));
        assertThat(serverInstance.filter(), equalTo(clientTestInstance.filter()));
        assertThat(serverInstance.query(), equalTo(clientTestInstance.query()));
        assertThat(serverInstance.indicesOptions(), equalTo(clientTestInstance.indicesOptions()));
        assertThat(serverInstance.indices(), equalTo(clientTestInstance.indices()));
        assertThat(serverInstance.fetchSize(), equalTo(clientTestInstance.fetchSize()));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents());
    }
}
