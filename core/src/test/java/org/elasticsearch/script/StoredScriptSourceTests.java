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

package org.elasticsearch.script;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class StoredScriptSourceTests extends AbstractSerializingTestCase<StoredScriptSource> {

    @Override
    protected StoredScriptSource createTestInstance() {
        String lang = randomAlphaOfLengthBetween(1, 20);
        XContentType xContentType = randomFrom(XContentType.JSON, XContentType.YAML);
        try {
            XContentBuilder template = XContentBuilder.builder(xContentType.xContent());
            template.startObject();
            template.startObject("query");
            template.startObject("match");
            template.field("title", "{{query_string}}");
            template.endObject();
            template.endObject();
            template.endObject();
            Map<String, String> options = new HashMap<>();
            if (randomBoolean()) {
                options.put(Script.CONTENT_TYPE_OPTION, xContentType.mediaType());
            }
            return StoredScriptSource.parse(lang, template.bytes(), xContentType);
        } catch (IOException e) {
            throw new AssertionError("Failed to create test instance", e);
        }
    }

    @Override
    protected StoredScriptSource doParseInstance(XContentParser parser) throws IOException {
        return StoredScriptSource.fromXContent(parser);
    }

    @Override
    protected Reader<StoredScriptSource> instanceReader() {
        return StoredScriptSource::new;
    }


}
