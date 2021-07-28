/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ScriptConfigTests extends AbstractSerializingTransformTestCase<ScriptConfig> {

    private boolean lenient;

    public static ScriptConfig randomScriptConfig() {
        ScriptType type = randomFrom(ScriptType.values());
        String lang = randomBoolean() ? Script.DEFAULT_SCRIPT_LANG : randomAlphaOfLengthBetween(1, 20);
        String idOrCode = randomAlphaOfLengthBetween(1, 20);
        Map<String, Object> params = Collections.emptyMap();

        type = ScriptType.STORED;

        Script script = new Script(type, type == ScriptType.STORED ? null : lang, idOrCode, params);
        LinkedHashMap<String, Object> source = null;

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = script.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            source = (LinkedHashMap<String, Object>) XContentHelper.convertToMap(BytesReference.bytes(content), true, XContentType.JSON)
                .v2();
        } catch (IOException e) {
            // should not happen
            fail("failed to create random script config");
        }
        return new ScriptConfig(source, script);
    }

    public static ScriptConfig randomInvalidScriptConfig() {
        // create something broken but with a source
        LinkedHashMap<String, Object> source = new LinkedHashMap<>();
        for (String key : randomUnique(() -> randomAlphaOfLengthBetween(1, 20), randomIntBetween(1, 10))) {
            source.put(key, randomAlphaOfLengthBetween(1, 20));
        }

        return new ScriptConfig(source, null);
    }

    @Before
    public void setRandomFeatures() {
        lenient = randomBoolean();
    }

    @Override
    protected ScriptConfig doParseInstance(XContentParser parser) throws IOException {
        return ScriptConfig.fromXContent(parser, lenient);
    }

    @Override
    protected Reader<ScriptConfig> instanceReader() {
        return ScriptConfig::new;
    }

    @Override
    protected ScriptConfig createTestInstance() {
        return lenient ? randomBoolean() ? randomScriptConfig() : randomInvalidScriptConfig() : randomScriptConfig();
    }

    public void testFailOnStrictPassOnLenient() throws IOException {
        // use a wrong syntax to trigger a parsing exception for strict parsing
        String source = "{\n" + "          \"source-code\": \"a=b\"" + "        }";

        // lenient, passes but reports invalid
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            ScriptConfig scriptConfig = ScriptConfig.fromXContent(parser, true);
            ValidationException validationException = scriptConfig.validate(null);
            assertThat(validationException, is(notNullValue()));
            assertThat(validationException.getMessage(), containsString("script must not be null"));
        }

        // strict throws
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            expectThrows(XContentParseException.class, () -> ScriptConfig.fromXContent(parser, false));
        }
    }
}
