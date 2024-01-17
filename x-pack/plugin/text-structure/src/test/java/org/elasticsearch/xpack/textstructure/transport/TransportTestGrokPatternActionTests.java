/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.textstructure.transport;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.textstructure.action.TestGrokPatternAction;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;

import java.util.List;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

public class TransportTestGrokPatternActionTests extends ESTestCase {

    private XContentSource executeRequest(TestGrokPatternAction.Request request) throws Exception {
        TransportService service = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        TransportTestGrokPatternAction action = new TransportTestGrokPatternAction(
            service,
            mock(ActionFilters.class),
            service.getThreadPool()
        );

        PlainActionFuture<TestGrokPatternAction.Response> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            future.get().toXContent(builder, ToXContent.EMPTY_PARAMS);
            return new XContentSource(builder);
        }
    }

    public void test() throws Exception {
        TestGrokPatternAction.Request request = new TestGrokPatternAction.Request.Builder().grokPattern(
            "%{WORD}.*%{WORD:first_name} %{WORD:last_name}!"
        ).text(List.of("Hello   Dave Roberts!", "this does not match")).build();

        XContentSource source = executeRequest(request);
        assertThat(source.getValue(""), aMapWithSize(1));
        assertThat(source.getValue("matches"), hasSize(2));
        assertThat(source.getValue("matches.0"), aMapWithSize(2));
        assertThat(source.getValue("matches.0.matched"), equalTo(true));
        assertThat(source.getValue("matches.0.fields"), aMapWithSize(2));
        assertThat(source.getValue("matches.0.fields.first_name"), aMapWithSize(3));
        assertThat(source.getValue("matches.0.fields.first_name.match"), equalTo("Dave"));
        assertThat(source.getValue("matches.0.fields.first_name.offset"), equalTo(8));
        assertThat(source.getValue("matches.0.fields.first_name.length"), equalTo(4));
        assertThat(source.getValue("matches.0.fields.last_name"), aMapWithSize(3));
        assertThat(source.getValue("matches.0.fields.last_name.match"), equalTo("Roberts"));
        assertThat(source.getValue("matches.0.fields.last_name.offset"), equalTo(13));
        assertThat(source.getValue("matches.0.fields.last_name.length"), equalTo(7));
        assertThat(source.getValue("matches.1"), aMapWithSize(1));
        assertThat(source.getValue("matches.1.matched"), equalTo(false));
    }

    public void test_repeatedIdentifiers() throws Exception {
        TestGrokPatternAction.Request request = new TestGrokPatternAction.Request.Builder().grokPattern(
            "%{WORD}.*%{WORD:name} %{WORD:name}!"
        ).text(List.of("Hello   Dave Roberts!", "this does not match")).build();

        XContentSource source = executeRequest(request);
        assertThat(source.getValue(""), aMapWithSize(1));
        assertThat(source.getValue("matches"), hasSize(2));
        assertThat(source.getValue("matches.0"), aMapWithSize(2));
        assertThat(source.getValue("matches.0.matched"), equalTo(true));
        assertThat(source.getValue("matches.0.fields"), aMapWithSize(1));
        assertThat(source.getValue("matches.0.fields.name"), hasSize(2));
        assertThat(source.getValue("matches.0.fields.name.0"), aMapWithSize(3));
        assertThat(source.getValue("matches.0.fields.name.0.match"), equalTo("Dave"));
        assertThat(source.getValue("matches.0.fields.name.0.offset"), equalTo(8));
        assertThat(source.getValue("matches.0.fields.name.0.length"), equalTo(4));
        assertThat(source.getValue("matches.0.fields.name.1"), aMapWithSize(3));
        assertThat(source.getValue("matches.0.fields.name.1.match"), equalTo("Roberts"));
        assertThat(source.getValue("matches.0.fields.name.1.offset"), equalTo(13));
        assertThat(source.getValue("matches.0.fields.name.1.length"), equalTo(7));
        assertThat(source.getValue("matches.1"), aMapWithSize(1));
        assertThat(source.getValue("matches.1.matched"), equalTo(false));
    }
}
