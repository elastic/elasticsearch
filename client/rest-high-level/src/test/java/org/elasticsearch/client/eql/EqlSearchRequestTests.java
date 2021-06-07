/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

import static org.elasticsearch.xpack.ql.TestUtils.randomRuntimeMappings;
import static org.hamcrest.Matchers.equalTo;

public class EqlSearchRequestTests extends AbstractRequestTestCase<EqlSearchRequest, org.elasticsearch.xpack.eql.action.EqlSearchRequest> {

    @Override
    protected EqlSearchRequest createClientTestInstance() {
        EqlSearchRequest eqlSearchRequest = new EqlSearchRequest("testindex", randomAlphaOfLength(40));
        if (randomBoolean()) {
            eqlSearchRequest.fetchSize(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            eqlSearchRequest.size(randomInt(Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            eqlSearchRequest.eventCategoryField(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            eqlSearchRequest.query(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            eqlSearchRequest.timestampField(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            eqlSearchRequest.tiebreakerField(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            if (randomBoolean()) {
                eqlSearchRequest.filter(QueryBuilders.matchAllQuery());
            } else {
                eqlSearchRequest.filter(QueryBuilders.termQuery(randomAlphaOfLength(10), randomInt(100)));
            }
        }
        if (randomBoolean()) {
            eqlSearchRequest.runtimeMappings(randomRuntimeMappings());
        }
        return eqlSearchRequest;
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
        assertThat(serverInstance.size(), equalTo(clientTestInstance.size()));
        assertThat(serverInstance.runtimeMappings(), equalTo(clientTestInstance.runtimeMappings()));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents());
    }
}
