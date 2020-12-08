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

package org.elasticsearch.index.shard;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

import static org.elasticsearch.index.shard.IndexLongFieldRangeTestUtils.checkForSameInstances;
import static org.elasticsearch.index.shard.IndexLongFieldRangeTestUtils.randomRange;
import static org.hamcrest.Matchers.sameInstance;

public class IndexLongFieldRangeXContentTests extends AbstractXContentTestCase<IndexLongFieldRange> {
    @Override
    protected IndexLongFieldRange createTestInstance() {
        return randomRange();
    }

    @Override
    protected IndexLongFieldRange doParseInstance(XContentParser parser) throws IOException {
        assertThat(parser.nextToken(), sameInstance(XContentParser.Token.START_OBJECT));
        return IndexLongFieldRange.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected void assertEqualInstances(IndexLongFieldRange expectedInstance, IndexLongFieldRange newInstance) {
        if (checkForSameInstances(expectedInstance, newInstance) == false) {
            super.assertEqualInstances(expectedInstance, newInstance);
        }
    }
}
