/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.jira;

import org.apache.http.HttpStatus;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.execution.Wid;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpProxy;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.notification.jira.JiraAccount;
import org.elasticsearch.xpack.watcher.notification.jira.JiraAccountTests;
import org.elasticsearch.xpack.watcher.notification.jira.JiraIssue;
import org.elasticsearch.xpack.watcher.notification.jira.JiraService;
import org.elasticsearch.xpack.watcher.support.Variables;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Map.entry;
import static org.elasticsearch.common.xcontent.XContentFactory.cborBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.smileBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.yamlBuilder;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.mockExecutionContextBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JiraActionTests extends ESTestCase {
    public void testParser() throws Exception {
        final String accountName = randomAlphaOfLength(10);
        final Map<String, Object> issueDefaults = JiraAccountTests.randomIssueDefaults();

        XContentBuilder builder = jsonBuilder().startObject()
                    .field("account", accountName)
                    .field("fields", issueDefaults)
                .endObject();

        BytesReference bytes = BytesReference.bytes(builder);
        logger.info("jira action json [{}]", bytes.utf8ToString());

        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken();

        JiraAction action = JiraAction.parse("_watch", "_action", parser);

        assertThat(action, notNullValue());
        assertThat(action.account, is(accountName));
        assertThat(action.fields, notNullValue());
        assertThat(action.fields, is(issueDefaults));
    }

    public void testParserSelfGenerated() throws Exception {
        final JiraAction action = randomJiraAction();

        XContentBuilder builder = jsonBuilder();
        action.toXContent(builder, ToXContent.EMPTY_PARAMS);
        BytesReference bytes = BytesReference.bytes(builder);
        logger.info("{}", bytes.utf8ToString());
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken();

        JiraAction parsedAction = JiraAction.parse("_watch", "_action", parser);

        assertThat(parsedAction, notNullValue());
        assertThat(parsedAction.proxy, equalTo(action.proxy));
        assertThat(parsedAction.fields, equalTo(action.fields));
        assertThat(parsedAction.account, equalTo(action.account));
        assertThat(parsedAction, is(action));
    }

    public void testParserInvalid() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().field("unknown_field", "value").endObject();
        XContentParser parser = createParser(builder);
        parser.nextToken();

        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> JiraAction.parse("_w", "_a", parser));
        assertThat(e.getMessage(), is("failed to parse [jira] action [_w/_a]. unexpected token [VALUE_STRING/unknown_field]"));
    }

    public void testToXContent() throws Exception {
        final JiraAction action = randomJiraAction();

        try (XContentBuilder builder = randomFrom(jsonBuilder(), smileBuilder(), yamlBuilder(), cborBuilder())) {
            action.toXContent(builder, ToXContent.EMPTY_PARAMS);

            String parsedAccount = null;
            HttpProxy parsedProxy = null;
            Map<String, Object> parsedFields = null;

            try (XContentParser parser = createParser(builder)) {
                assertNull(parser.currentToken());
                parser.nextToken();

                XContentParser.Token token = parser.currentToken();
                assertThat(token, is(XContentParser.Token.START_OBJECT));

                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if ("account".equals(currentFieldName)) {
                        parsedAccount = parser.text();
                    } else if ("proxy".equals(currentFieldName)) {
                        parsedProxy = HttpProxy.parse(parser);
                    } else if ("fields".equals(currentFieldName)) {
                        parsedFields = parser.map();
                    } else {
                        fail("unknown field [" + currentFieldName + "]");
                    }
                }
            }

            assertThat(parsedAccount, equalTo(action.getAccount()));
            assertThat(parsedProxy, equalTo(action.proxy));
            assertThat(parsedFields, equalTo(action.fields));
        }
    }

    public void testEquals() throws Exception {
        final JiraAction action1 = randomJiraAction();

        String account = action1.account;
        Map<String, Object> fields = action1.fields;
        HttpProxy proxy = action1.proxy;

        boolean equals = randomBoolean();
        if (equals == false) {
            equals = true;
            if (rarely()) {
                equals = false;
                account = "another account";
            }
            if (rarely()) {
                equals = false;
                // cover the special case that randomIssueDefaults() left an empty map here as
                // well as in the action1, so that those would be equal - make sure they are not
                fields = JiraAccountTests.randomIssueDefaults();
                while (fields.equals(action1.fields)) {
                    fields = JiraAccountTests.randomIssueDefaults();
                }
            }
            if (rarely()) {
                equals = false;
                // another low probability case, that a random proxy is exactly the same including
                // port number
                proxy = randomHttpProxy();
                while (proxy.equals(action1.proxy)) {
                    proxy = randomHttpProxy();
                }
            }
        }

        JiraAction action2 = new JiraAction(account, fields, proxy);
        assertThat(action1.equals(action2), is(equals));
    }

    public void testExecute() throws Exception {
        final Map<String, Object> model = new HashMap<>();
        final var entries = new ArrayList<Map.Entry<String, Object>>();

        String summary = randomAlphaOfLength(15);
        entries.add(entry("summary", "{{ctx.summary}}"));
        model.put("{{ctx.summary}}", summary);

        String projectId = randomAlphaOfLength(10);
        entries.add(entry("project", singletonMap("id", "{{ctx.project_id}}")));
        model.put("{{ctx.project_id}}", projectId);

        String description = null;
        if (randomBoolean()) {
            description = randomAlphaOfLength(50);
            entries.add(entry("description", description));
        }

        String issueType = null;
        if (randomBoolean()) {
            issueType = randomFrom("Bug", "Test", "Task", "Epic");
            entries.add(entry("issuetype", Map.of("name", issueType)));
        }

        String watchId = null;
        if (randomBoolean()) {
            watchId = "jira_watch_" + randomInt();
            model.put("{{" + Variables.WATCH_ID + "}}", watchId);
            entries.add(entry("customfield_0", "{{watch_id}}"));
        }

        HttpClient httpClient = mock(HttpClient.class);
        when(httpClient.execute(any(HttpRequest.class))).thenReturn(new HttpResponse(HttpStatus.SC_CREATED));

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("secure_url", "https://internal-jira.elastic.co:443");
        secureSettings.setString("secure_user", "elastic");
        secureSettings.setString("secure_password", "secret");

        Settings.Builder settings = Settings.builder()
                .setSecureSettings(secureSettings)
                .put("issue_defaults.customfield_000", "foo")
                .put("issue_defaults.customfield_001", "bar");

        JiraAccount account = new JiraAccount("account", settings.build(), httpClient);

        JiraService service = mock(JiraService.class);
        when(service.getAccount(eq("account"))).thenReturn(account);

        JiraAction action = new JiraAction("account", Maps.ofEntries(entries), null);
        ExecutableJiraAction executable = new ExecutableJiraAction(action, logger, service, new ModelTextTemplateEngine(model));

        Map<String, Object> data = new HashMap<>();
        Payload payload = new Payload.Simple(data);

        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);

        Wid wid = new Wid(randomAlphaOfLength(5),  now);
        WatchExecutionContext context = mockExecutionContextBuilder(wid.watchId())
                .wid(wid)
                .payload(payload)
                .time(wid.watchId(), now)
                .buildMock();
        when(context.simulateAction("test")).thenReturn(false);

        Action.Result result = executable.execute("test", context, new Payload.Simple());
        assertThat(result, instanceOf(JiraAction.Result.class));
        assertThat(result, instanceOf(JiraAction.Executed.class));

        JiraIssue issue = ((JiraAction.Executed) result).getResult();
        assertThat(issue.getFields().get("summary"), equalTo(summary));
        assertThat(issue.getFields().get("customfield_000"), equalTo("foo"));
        assertThat(issue.getFields().get("customfield_001"), equalTo("bar"));
        assertThat(((Map) issue.getFields().get("project")).get("id"), equalTo(projectId));
        if (issueType != null) {
            assertThat(((Map) issue.getFields().get("issuetype")).get("name"), equalTo(issueType));
        }
        if (description != null) {
            assertThat(issue.getFields().get("description"), equalTo(description));
        }
        if (watchId != null) {
            assertThat(issue.getFields().get("customfield_0"), equalTo(watchId));
        }
    }

    private static JiraAction randomJiraAction() {
        String account = null;
        if (randomBoolean()) {
            account = randomAlphaOfLength(randomIntBetween(5, 10));
        }
        Map<String, Object> fields = emptyMap();
        if (frequently()) {
            fields = JiraAccountTests.randomIssueDefaults();
        }
        HttpProxy proxy = null;
        if (randomBoolean()) {
            proxy = randomHttpProxy();
        }
        return new JiraAction(account, fields, proxy);
    }

    private static HttpProxy randomHttpProxy() {
        return new HttpProxy(randomFrom("localhost", "www.elastic.co", "198.18.0.0"), randomIntBetween(8000, 10000));
    }

    /**
     * TextTemplateEngine that picks up templates from the model if exist,
     * otherwise returns the template as it is.
     */
    class ModelTextTemplateEngine extends TextTemplateEngine {

        private final Map<String, Object> model;

        ModelTextTemplateEngine(Map<String, Object> model) {
            super(mock(ScriptService.class));
            this.model = model;
        }

        @Override
        public String render(TextTemplate textTemplate, Map<String, Object> ignoredModel) {
            String template = textTemplate.getTemplate();
            if (model.containsKey(template)) {
                return (String) model.get(template);
            }
            return template;
        }
    }
}
