/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.test.integration;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.transform.search.ExecutableSearchTransform;
import org.elasticsearch.xpack.watcher.transform.search.SearchTransform;
import org.elasticsearch.xpack.watcher.transform.search.SearchTransformFactory;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.getRandomSupportedSearchType;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

public class SearchTransformTests extends ESTestCase {

    public void testParser() throws Exception {
        String[] indices = rarely() ? null : randomBoolean() ? new String[] { "idx" } : new String[] { "idx1", "idx2" };
        SearchType searchType = getRandomSupportedSearchType();
        String templateName = randomBoolean() ? null : "template1";
        XContentBuilder builder = jsonBuilder().startObject();
        builder.startObject("request");
        if (indices != null) {
            builder.array("indices", indices);
        }
        if (searchType != null) {
            builder.field("search_type", searchType.name());
        }
        if (templateName != null) {
            TextTemplate template = new TextTemplate(templateName, null, ScriptType.INLINE, null);
            builder.field("template", template);
        }

        builder.startObject("body").startObject("query").startObject("match_all").endObject().endObject().endObject();

        builder.endObject();
        TimeValue readTimeout = randomBoolean() ? TimeValue.timeValueSeconds(randomInt(10)) : null;
        if (readTimeout != null) {
            builder.field("timeout", readTimeout);
        }
        builder.endObject();

        XContentParser parser = createParser(builder);
        parser.nextToken();

        final MockScriptEngine engine = new MockScriptEngine("mock", Collections.emptyMap(), Collections.emptyMap());
        Map<String, ScriptEngine> engines = Collections.singletonMap(engine.getType(), engine);
        ScriptService scriptService = new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS, () -> 1L);

        Client client = mock(Client.class);
        SearchTransformFactory transformFactory = new SearchTransformFactory(Settings.EMPTY, client, xContentRegistry(), scriptService);
        ExecutableSearchTransform executable = transformFactory.parseExecutable("_id", parser);

        assertThat(executable, notNullValue());
        assertThat(executable.type(), is(SearchTransform.TYPE));
        assertThat(executable.transform().getRequest(), notNullValue());
        if (indices != null) {
            assertThat(executable.transform().getRequest().getIndices(), arrayContainingInAnyOrder(indices));
        }
        if (searchType != null) {
            assertThat(executable.transform().getRequest().getSearchType(), is(searchType));
        }
        if (templateName != null) {
            assertThat(
                executable.transform().getRequest().getTemplate(),
                equalTo(new Script(ScriptType.INLINE, "mustache", "template1", Collections.emptyMap()))
            );
        }
        assertThat(executable.transform().getRequest().getSearchSource().utf8ToString(), equalTo("{\"query\":{\"match_all\":{}}}"));
        assertThat(executable.transform().getTimeout(), equalTo(readTimeout));
    }
}
