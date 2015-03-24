/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform;

import org.elasticsearch.watcher.watch.WatchExecutionContext;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.support.Script;
import org.elasticsearch.watcher.support.Variables;
import org.elasticsearch.watcher.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.watcher.test.WatcherTestUtils.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class ScriptTransformTests extends ElasticsearchTestCase {

    @Test
    public void testApply() throws Exception {
        ScriptServiceProxy service = mock(ScriptServiceProxy.class);
        ScriptService.ScriptType type = randomFrom(ScriptService.ScriptType.values());
        Map<String, Object> params = Collections.emptyMap();
        Script script = new Script("_script", type, "_lang", params);
        ScriptTransform transform = new ScriptTransform(service, script);

        WatchExecutionContext ctx = mockExecutionContext("_name", EMPTY_PAYLOAD);

        Payload payload = simplePayload("key", "value");

        Map<String, Object> model = Variables.createCtxModel(ctx, payload);

        Map<String, Object> transformed = ImmutableMap.<String, Object>builder()
                .put("key", "value")
                .build();

        ExecutableScript executable = mock(ExecutableScript.class);
        when(executable.run()).thenReturn(transformed);
        when(service.executable("_lang", "_script", type, model)).thenReturn(executable);

        Transform.Result result = transform.apply(ctx, payload);
        assertThat(result, notNullValue());
        assertThat(result.type(), is(ScriptTransform.TYPE));
        assertThat(result.payload().data(), equalTo(transformed));
    }

    @Test
    public void testParser() throws Exception {
        ScriptServiceProxy service = mock(ScriptServiceProxy.class);
        ScriptService.ScriptType type = randomFrom(ScriptService.ScriptType.values());
        XContentBuilder builder = jsonBuilder().startObject()
                .field("script", "_script")
                .field("lang", "_lang")
                .field("type", type.name())
                .startObject("params").field("key", "value").endObject()
                .endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();
        ScriptTransform transform = new ScriptTransform.Parser(service).parse(parser);
        assertThat(transform.script(), equalTo(new Script("_script", type, "_lang", ImmutableMap.<String, Object>builder().put("key", "value").build())));
    }

    @Test
    public void testParser_String() throws Exception {
        ScriptServiceProxy service = mock(ScriptServiceProxy.class);
        XContentBuilder builder = jsonBuilder().value("_script");

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();
        ScriptTransform transform = new ScriptTransform.Parser(service).parse(parser);
        assertThat(transform.script(), equalTo(new Script("_script", ScriptService.ScriptType.INLINE, ScriptService.DEFAULT_LANG, ImmutableMap.<String, Object>of())));
    }
}
