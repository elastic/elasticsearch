/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.support;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class WatcherScriptTests extends ESTestCase {

    public void testParseScript() throws IOException {
        final Map<String, Object> params =
                randomFrom(Collections.<String, Object>emptyMap(), Collections.singletonMap("foo", (Object)"bar"), null);

        WatcherScript script = new WatcherScript(randomAsciiOfLengthBetween(1, 5),
                                                randomFrom(ScriptType.values()),
                                                randomFrom("custom", "mustache", null),
                                                params);

        try (XContentParser parser = createParser(script)) {
            assertThat(WatcherScript.parse(parser), equalTo(script));
        }
    }

    public void testParseScriptWithCustomLang() throws IOException {
        final String lang = randomFrom("custom", "painful");
        final WatcherScript script = new WatcherScript("my-script", randomFrom(ScriptType.values()), lang, null);

        try (XContentParser parser = createParser(script)) {
            WatcherScript result = WatcherScript.parse(parser, WatcherScript.DEFAULT_LANG);
            assertThat(result.script(), equalTo(script.script()));
            assertThat(result.type(), equalTo(script.type()));
            assertThat(result.lang(), equalTo(lang));
            assertThat(result.params(), equalTo(script.params()));
        }
    }

    public void testParseScriptWithDefaultLang() throws IOException {
        final WatcherScript script = new WatcherScript("my-script", randomFrom(ScriptType.values()), null, null);

        try (XContentParser parser = createParser(script)) {
            WatcherScript result = WatcherScript.parse(parser, WatcherScript.DEFAULT_LANG);
            assertThat(result.script(), equalTo(script.script()));
            assertThat(result.type(), equalTo(script.type()));
            assertThat(result.lang(), equalTo(WatcherScript.DEFAULT_LANG));
            assertThat(result.params(), equalTo(script.params()));
        }
    }

    private static XContentParser createParser(WatcherScript watcherScript) throws IOException {
        final XContent xContent = randomFrom(XContentType.values()).xContent();

        XContentBuilder builder = XContentBuilder.builder(xContent);
        watcherScript.toXContent(builder, ToXContent.EMPTY_PARAMS);

        XContentParser parser = XContentHelper.createParser(builder.bytes());
        assertNull(parser.currentToken());
        parser.nextToken();
        return parser;
    }
}
