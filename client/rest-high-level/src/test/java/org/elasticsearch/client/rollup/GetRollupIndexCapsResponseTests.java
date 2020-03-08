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
package org.elasticsearch.client.rollup;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;

public class GetRollupIndexCapsResponseTests extends RollupCapsResponseTestCase<GetRollupIndexCapsResponse> {

    @Override
    protected GetRollupIndexCapsResponse createTestInstance() {
        return new GetRollupIndexCapsResponse(indices);
    }

    @Override
    protected void toXContent(GetRollupIndexCapsResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        for (Map.Entry<String, RollableIndexCaps> entry : response.getJobs().entrySet()) {
            entry.getValue().toXContent(builder, null);
        }
        builder.endObject();
    }

    @Override
    protected Predicate<String> randomFieldsExcludeFilter() {
        return (field) ->
        {
            // base cannot have extra things in it
            return "".equals(field)
                // the field list expects to be a nested object of a certain type
                || field.contains("fields");
        };
    }

    @Override
    protected GetRollupIndexCapsResponse fromXContent(XContentParser parser) throws IOException {
        return GetRollupIndexCapsResponse.fromXContent(parser);
    }
}
