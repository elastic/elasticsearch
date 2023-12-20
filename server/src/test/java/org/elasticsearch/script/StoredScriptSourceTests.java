/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class StoredScriptSourceTests extends AbstractXContentSerializingTestCase<StoredScriptSource> {

    @Override
    protected StoredScriptSource createTestInstance() {
        XContentType xContentType = randomFrom(XContentType.JSON, XContentType.YAML);
        try {
            XContentBuilder template = XContentBuilder.builder(xContentType.xContent());
            template.startObject();
            template.startObject("script");
            {
                template.field("lang", "mustache");
                template.startObject("source");
                template.startObject("query").startObject("match").field("title", "{{query_string}}").endObject();
                template.endObject();
                template.endObject();
            }
            template.endObject();
            template.endObject();
            Map<String, String> options = new HashMap<>();
            if (randomBoolean()) {
                options.put(Script.CONTENT_TYPE_OPTION, xContentType.mediaType());
            }
            return StoredScriptSource.parse(BytesReference.bytes(template), xContentType);
        } catch (IOException e) {
            throw new AssertionError("Failed to create test instance", e);
        }
    }

    @Override
    protected StoredScriptSource doParseInstance(XContentParser parser) {
        return StoredScriptSource.fromXContent(parser, false);
    }

    @Override
    protected Reader<StoredScriptSource> instanceReader() {
        return StoredScriptSource::new;
    }

    @Override
    protected StoredScriptSource mutateInstance(StoredScriptSource instance) throws IOException {
        String source = instance.getSource();
        String lang = instance.getLang();
        Map<String, String> options = instance.getOptions();

        XContentType newXContentType = randomFrom(XContentType.JSON, XContentType.YAML);
        XContentBuilder newTemplate = XContentBuilder.builder(newXContentType.xContent());
        newTemplate.startObject();
        newTemplate.startObject("query");
        newTemplate.startObject("match");
        newTemplate.field("body", "{{query_string}}");
        newTemplate.endObject();
        newTemplate.endObject();
        newTemplate.endObject();

        switch (between(0, 2)) {
            case 0 -> source = Strings.toString(newTemplate);
            case 1 -> lang = randomAlphaOfLengthBetween(1, 20);
            default -> {
                options = new HashMap<>(options);
                options.put(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
            }
        }
        return new StoredScriptSource(lang, source, options);
    }
}
