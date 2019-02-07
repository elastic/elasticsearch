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

package org.elasticsearch.action.admin.cluster.snapshots.status;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.test.AbstractXContentTestCase;


public class SnapshotIndexStatusTests extends AbstractXContentTestCase<SnapshotIndexStatus> {

    @Override
    protected SnapshotIndexStatus createTestInstance() {
        String index = randomAlphaOfLength(10);
        List<SnapshotIndexShardStatus> shardStatuses = new ArrayList<>();
        SnapshotIndexShardStatusTests builder = new SnapshotIndexShardStatusTests();
        for (int idx = 0; idx < randomIntBetween(0, 10); idx++) {
            shardStatuses.add(builder.createForIndex(index));
        }
        return new SnapshotIndexStatus(index, shardStatuses);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // Do not place random fields in the root object or the shards field since their fields correspond to names.
        return (s) -> s.isEmpty() || s.endsWith("shards");
    }

    @Override
    protected SnapshotIndexStatus doParseInstance(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
        SnapshotIndexStatus status = SnapshotIndexStatus.fromXContent(parser);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
        return status;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
