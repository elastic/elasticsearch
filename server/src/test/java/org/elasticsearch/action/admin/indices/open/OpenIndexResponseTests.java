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

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

public class OpenIndexResponseTests extends AbstractSerializingTestCase<OpenIndexResponse> {

    @Override
    protected OpenIndexResponse doParseInstance(XContentParser parser){
        return OpenIndexResponse.fromXContent(parser);
    }

    @Override
    protected OpenIndexResponse createTestInstance() {
        boolean acknowledged = randomBoolean();
        boolean shardsAcknowledged = acknowledged && randomBoolean();
        return new OpenIndexResponse(acknowledged, shardsAcknowledged);
    }

    @Override
    protected Writeable.Reader<OpenIndexResponse> instanceReader() {
        return OpenIndexResponse::new;
    }

    @Override
    protected OpenIndexResponse mutateInstance(OpenIndexResponse response) {
        if (randomBoolean()) {
            boolean acknowledged = response.isAcknowledged() == false;
            boolean shardsAcknowledged = acknowledged && response.isShardsAcknowledged();
            return new OpenIndexResponse(acknowledged, shardsAcknowledged);
        } else {
            boolean shardsAcknowledged = response.isShardsAcknowledged() == false;
            boolean acknowledged = shardsAcknowledged || response.isAcknowledged();
            return new OpenIndexResponse(acknowledged, shardsAcknowledged);
        }
    }
}
