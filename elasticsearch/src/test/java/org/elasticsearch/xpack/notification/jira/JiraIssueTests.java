/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.jira;

import org.apache.http.HttpStatus;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.common.http.HttpMethod;
import org.elasticsearch.xpack.common.http.HttpRequest;
import org.elasticsearch.xpack.common.http.HttpResponse;
import org.elasticsearch.xpack.common.http.auth.HttpAuthRegistry;
import org.elasticsearch.xpack.common.http.auth.basic.BasicAuth;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.cborBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.smileBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.yamlBuilder;
import static org.elasticsearch.xpack.notification.jira.JiraAccountTests.randomHttpError;
import static org.elasticsearch.xpack.notification.jira.JiraAccountTests.randomIssueDefaults;
import static org.elasticsearch.xpack.notification.jira.JiraIssue.resolveFailureReason;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class JiraIssueTests extends ESTestCase {

    public void testToXContent() throws Exception {
        final JiraIssue issue = randomJiraIssue();

        BytesReference bytes = null;
        try (XContentBuilder builder = randomFrom(jsonBuilder(), smileBuilder(), yamlBuilder(), cborBuilder())) {
            issue.toXContent(builder, ToXContent.EMPTY_PARAMS);
            bytes = builder.bytes();
        }

        Map<String, Object> parsedFields = null;
        Map<String, Object> parsedResult = null;

        HttpRequest parsedRequest = null;
        HttpResponse parsedResponse = null;
        String parsedReason = null;

        try (XContentParser parser = XContentHelper.createParser(bytes)) {
            assertNull(parser.currentToken());
            parser.nextToken();

            XContentParser.Token token = parser.currentToken();
            assertThat(token, is(XContentParser.Token.START_OBJECT));

            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if ("result".equals(currentFieldName)) {
                    parsedResult = parser.map();
                } else if ("request".equals(currentFieldName)) {
                    HttpRequest.Parser httpRequestParser = new HttpRequest.Parser(mock(HttpAuthRegistry.class));
                    parsedRequest = httpRequestParser.parse(parser);
                } else if ("response".equals(currentFieldName)) {
                    parsedResponse = HttpResponse.parse(parser);
                } else if ("fields".equals(currentFieldName)) {
                    parsedFields = parser.map();
                } else if ("reason".equals(currentFieldName)) {
                    parsedReason = parser.text();
                } else {
                    fail("unknown field [" + currentFieldName + "]");
                }
            }
        }

        assertThat(parsedFields, equalTo(issue.getFields()));
        if (issue.successful()) {
            assertThat(parsedResult, hasEntry("key", "TEST"));
            assertNull(parsedRequest);
            assertNull(parsedResponse);
        } else {
            assertThat(parsedRequest, equalTo(issue.getRequest()));
            assertThat(parsedResponse, equalTo(issue.getResponse()));
            assertThat(parsedReason, equalTo(resolveFailureReason(issue.getResponse())));
        }
    }

    public void testEquals() throws Exception {
        final JiraIssue issue1 = randomJiraIssue();
        final boolean equals = randomBoolean();

        final Map<String, Object> fields = new HashMap<>(issue1.getFields());
        if (equals == false) {
            String key = randomFrom(fields.keySet());
            fields.remove(key);
        }

        JiraIssue issue2 = new JiraIssue(fields, issue1.getRequest(), issue1.getResponse(), issue1.getFailureReason());
        assertThat(issue1.equals(issue2), is(equals));
    }

    private static JiraIssue randomJiraIssue() throws IOException {
        Map<String, Object> fields = randomIssueDefaults();
        HttpRequest request = HttpRequest.builder(randomFrom("localhost", "internal-jira.elastic.co"), randomFrom(80, 443))
                                            .method(HttpMethod.POST)
                                            .path(JiraAccount.DEFAULT_PATH)
                                            .auth(new BasicAuth(randomAsciiOfLength(5), randomAsciiOfLength(5).toCharArray()))
                                            .build();
        if (rarely()) {
            Tuple<Integer, String> error = randomHttpError();
            return JiraIssue.responded(fields, request, new HttpResponse(error.v1(), "{\"error\": \"" + error.v2() + "\"}"));
        }
        return JiraIssue.responded(fields, request, new HttpResponse(HttpStatus.SC_CREATED, "{\"key\": \"TEST\"}"));
    }
}
