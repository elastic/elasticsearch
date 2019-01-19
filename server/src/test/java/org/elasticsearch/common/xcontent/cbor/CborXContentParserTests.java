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

package org.elasticsearch.common.xcontent.cbor;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class CborXContentParserTests extends ESTestCase {
    public void testEmptyValue() throws IOException {
        BytesReference ref = BytesReference.bytes(XContentFactory.cborBuilder().startObject().field("field", "").endObject());

        for (int i = 0; i < 2; i++) {
            // Running this part twice triggers the issue.
            // See https://github.com/elastic/elasticsearch/issues/8629
            try (XContentParser parser = createParser(CborXContent.cborXContent, ref)) {
                while (parser.nextToken() != null) {
                    parser.charBuffer();
                }
            }
        }
    }
}
