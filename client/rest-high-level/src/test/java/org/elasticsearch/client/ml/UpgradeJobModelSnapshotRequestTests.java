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

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class UpgradeJobModelSnapshotRequestTests extends AbstractXContentTestCase<UpgradeJobModelSnapshotRequest> {

    @Override
    protected UpgradeJobModelSnapshotRequest createTestInstance() {
        return new UpgradeJobModelSnapshotRequest(randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomTimeValue(),
            randomBoolean() ? null : randomBoolean());
    }

    @Override
    protected UpgradeJobModelSnapshotRequest doParseInstance(XContentParser parser) throws IOException {
        return UpgradeJobModelSnapshotRequest.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
