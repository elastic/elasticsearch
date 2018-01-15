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

package org.elasticsearch.action.admin.indices.open;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.hamcrest.CoreMatchers.equalTo;

public class OpenIndexResponseTests extends ESTestCase {

    public void testFromToXContent() throws IOException {
        final OpenIndexResponse openIndexResponse = createTestItem();

        boolean humanReadable = randomBoolean();
        final XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(openIndexResponse, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        BytesReference mutated;
        if (randomBoolean()) {
            mutated = insertRandomFields(xContentType, originalBytes, null, random());
        } else {
            mutated = originalBytes;
        }
        
        OpenIndexResponse parsedOpenIndexResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            parsedOpenIndexResponse = OpenIndexResponse.fromXContent(parser);
            assertNull(parser.nextToken());
        }

        assertThat(parsedOpenIndexResponse.isShardsAcknowledged(), equalTo(openIndexResponse.isShardsAcknowledged()));
        assertThat(parsedOpenIndexResponse.isAcknowledged(), equalTo(openIndexResponse.isAcknowledged()));
    }
    
    private static OpenIndexResponse createTestItem() {
        boolean acknowledged = randomBoolean();
        boolean shardsAcknowledged = acknowledged && randomBoolean();
        return new OpenIndexResponse(acknowledged, shardsAcknowledged);
    }
}
