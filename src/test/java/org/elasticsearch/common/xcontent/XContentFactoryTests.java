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

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class XContentFactoryTests extends ElasticsearchTestCase {


    @Test
    public void testGuessJson() throws IOException {
        testGuessType(XContentType.JSON);
    }

    @Test
    public void testGuessSmile() throws IOException {
        testGuessType(XContentType.SMILE);
    }

    @Test
    public void testGuessYaml() throws IOException {
        testGuessType(XContentType.YAML);
    }

    @Test
    public void testGuessCbor() throws IOException {
        testGuessType(XContentType.CBOR);
    }

    private void testGuessType(XContentType type) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(type);
        builder.startObject();
        builder.field("field1", "value1");
        builder.endObject();

        assertThat(XContentFactory.xContentType(builder.bytes()), equalTo(type));
        BytesArray bytesArray = builder.bytes().toBytesArray();
        assertThat(XContentFactory.xContentType(new BytesStreamInput(bytesArray.array(), bytesArray.arrayOffset(), bytesArray.length(), false)), equalTo(type));

        // CBOR is binary, cannot use String
        if (type != XContentType.CBOR) {
            assertThat(XContentFactory.xContentType(builder.string()), equalTo(type));
        }
    }
}
