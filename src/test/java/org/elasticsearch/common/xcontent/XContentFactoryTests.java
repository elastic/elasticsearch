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
 * Tests {@link XContentFactory} type generation.
 */
public class XContentFactoryTests extends ElasticsearchTestCase {


    @Test
    public void testGuessJson() throws IOException {
        assertType(XContentType.JSON);
    }

    @Test
    public void testGuessSmile() throws IOException {
        assertType(XContentType.SMILE);
    }

    @Test
    public void testGuessYaml() throws IOException {
        assertType(XContentType.YAML);
    }

    @Test
    public void testGuessCbor() throws IOException {
        assertType(XContentType.CBOR);
    }

    private void assertType(XContentType type) throws IOException {
        for (XContentBuilder builder : generateBuilders(type)) {
            assertBuilderType(builder, type);
        }
    }

    /**
     * Assert the {@code builder} maps to the appropriate {@code type}.
     *
     * @param builder Builder to check.
     * @param type Type to match.
     * @throws IOException if any error occurs while checking the builder
     */
    private void assertBuilderType(XContentBuilder builder, XContentType type) throws IOException {
        assertThat(XContentFactory.xContentType(builder.bytes()), equalTo(type));
        BytesArray bytesArray = builder.bytes().toBytesArray();
        assertThat(XContentFactory.xContentType(new BytesStreamInput(bytesArray.array(), bytesArray.arrayOffset(), bytesArray.length(), false)), equalTo(type));

        // CBOR is binary, cannot use String
        if (type != XContentType.CBOR) {
            assertThat(XContentFactory.xContentType(builder.string()), equalTo(type));
        }
    }

    /**
     * Generate builders to test various use cases to check.
     *
     * @param type The type to use.
     * @return Never {@code null} array of unique {@link XContentBuilder}s testing different edge cases.
     * @throws IOException if any error occurs while generating builders
     */
    private XContentBuilder[] generateBuilders(XContentType type) throws IOException {
        XContentBuilder[] builders = new XContentBuilder[] {
            XContentFactory.contentBuilder(type), XContentFactory.contentBuilder(type)
        };

        // simple object
        builders[0].startObject();
        builders[0].field("field1", "value1");
            builders[0].startObject("object1");
            builders[0].field("field2", "value2");
            builders[0].endObject();
        builders[0].endObject();

        // empty object
        builders[1].startObject().endObject();

        return builders;
    }
}
