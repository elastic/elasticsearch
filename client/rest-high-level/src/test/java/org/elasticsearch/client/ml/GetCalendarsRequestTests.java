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
package org.elasticsearch.client.ml;

import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

public class GetCalendarsRequestTests extends AbstractXContentTestCase<GetCalendarsRequest> {

    @Override
    protected GetCalendarsRequest createTestInstance() {
        GetCalendarsRequest request = new GetCalendarsRequest();
        request.setCalendarId(randomAlphaOfLength(9));
        if (randomBoolean()) {
            request.setPageParams(new PageParams(1, 2));
        }
        return request;
    }

    @Override
    protected GetCalendarsRequest doParseInstance(XContentParser parser) {
        return GetCalendarsRequest.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
