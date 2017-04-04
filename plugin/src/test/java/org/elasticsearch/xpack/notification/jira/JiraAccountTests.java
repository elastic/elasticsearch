/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.jira;

import org.apache.http.HttpStatus;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.common.http.HttpClient;
import org.elasticsearch.xpack.common.http.HttpRequest;
import org.elasticsearch.xpack.common.http.HttpResponse;
import org.elasticsearch.xpack.common.http.Scheme;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.collect.Tuple.tuple;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JiraAccountTests extends ESTestCase {

    private HttpClient httpClient;
    private ClusterSettings clusterSettings;

    @Before
    public void init() throws Exception {
        httpClient = mock(HttpClient.class);
        clusterSettings = new ClusterSettings(Settings.EMPTY, Collections.singleton(JiraService.JIRA_ACCOUNT_SETTING));
    }

    public void testJiraAccountSettings() {
        final String url = "https://internal-jira.elastic.co:443";

        SettingsException e = expectThrows(SettingsException.class, () -> new JiraAccount(null, Settings.EMPTY, null));
        assertThat(e.getMessage(), containsString("invalid jira [null] account settings. missing required [url] setting"));

        Settings settings1 = Settings.builder().put("url", url).build();
        e = expectThrows(SettingsException.class, () -> new JiraAccount("test", settings1, null));
        assertThat(e.getMessage(), containsString("invalid jira [test] account settings. missing required [user] setting"));

        Settings settings2 = Settings.builder().put("url", url).put("user", "").build();
        e = expectThrows(SettingsException.class, () -> new JiraAccount("test", settings2, null));
        assertThat(e.getMessage(), containsString("invalid jira [test] account settings. missing required [user] setting"));

        Settings settings3 = Settings.builder().put("url", url).put("user", "foo").build();
        e = expectThrows(SettingsException.class, () -> new JiraAccount("test", settings3, null));
        assertThat(e.getMessage(), containsString("invalid jira [test] account settings. missing required [password] setting"));

        Settings settings4 = Settings.builder().put("url", url).put("user", "foo").put("password", "").build();
        e = expectThrows(SettingsException.class, () -> new JiraAccount("test", settings4, null));
        assertThat(e.getMessage(), containsString("invalid jira [test] account settings. missing required [password] setting"));
    }

    public void testSingleAccount() throws Exception {
        Settings.Builder builder = Settings.builder().put("xpack.notification.jira.default_account", "account1");
        addAccountSettings("account1", builder);

        JiraService service = new JiraService(builder.build(), httpClient, clusterSettings);
        JiraAccount account = service.getAccount("account1");
        assertThat(account, notNullValue());
        assertThat(account.getName(), equalTo("account1"));
        account = service.getAccount(null); // falling back on the default
        assertThat(account, notNullValue());
        assertThat(account.getName(), equalTo("account1"));
    }

    public void testSingleAccountNoExplicitDefault() throws Exception {
        Settings.Builder builder = Settings.builder();
        addAccountSettings("account1", builder);

        JiraService service = new JiraService(builder.build(), httpClient, clusterSettings);
        JiraAccount account = service.getAccount("account1");
        assertThat(account, notNullValue());
        assertThat(account.getName(), equalTo("account1"));
        account = service.getAccount(null); // falling back on the default
        assertThat(account, notNullValue());
        assertThat(account.getName(), equalTo("account1"));
    }

    public void testMultipleAccounts() throws Exception {
        Settings.Builder builder = Settings.builder().put("xpack.notification.jira.default_account", "account1");
        addAccountSettings("account1", builder);
        addAccountSettings("account2", builder);

        JiraService service = new JiraService(builder.build(), httpClient, clusterSettings);
        JiraAccount account = service.getAccount("account1");
        assertThat(account, notNullValue());
        assertThat(account.getName(), equalTo("account1"));
        account = service.getAccount("account2");
        assertThat(account, notNullValue());
        assertThat(account.getName(), equalTo("account2"));
        account = service.getAccount(null); // falling back on the default
        assertThat(account, notNullValue());
        assertThat(account.getName(), equalTo("account1"));
    }

    public void testMultipleAccountsNoExplicitDefault() throws Exception {
        Settings.Builder builder = Settings.builder().put("xpack.notification.jira.default_account", "account1");
        addAccountSettings("account1", builder);
        addAccountSettings("account2", builder);

        JiraService service = new JiraService(builder.build(), httpClient, clusterSettings);
        JiraAccount account = service.getAccount("account1");
        assertThat(account, notNullValue());
        assertThat(account.getName(), equalTo("account1"));
        account = service.getAccount("account2");
        assertThat(account, notNullValue());
        assertThat(account.getName(), equalTo("account2"));
        account = service.getAccount(null);
        assertThat(account, notNullValue());
        assertThat(account.getName(), isOneOf("account1", "account2"));
    }

    public void testMultipleAccountsUnknownDefault() throws Exception {
        Settings.Builder builder = Settings.builder().put("xpack.notification.jira.default_account", "unknown");
        addAccountSettings("account1", builder);
        addAccountSettings("account2", builder);
        SettingsException e = expectThrows(SettingsException.class, () -> new JiraService(builder.build(), httpClient, clusterSettings)
        );
        assertThat(e.getMessage(), is("could not find default account [unknown]"));
    }

    public void testNoAccount() throws Exception {
        Settings.Builder builder = Settings.builder();
        JiraService service = new JiraService(builder.build(), httpClient, clusterSettings);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> service.getAccount(null));
        assertThat(e.getMessage(), is("no account found for name: [null]"));
    }

    public void testNoAccountWithDefaultAccount() throws Exception {
        Settings.Builder builder = Settings.builder().put("xpack.notification.jira.default_account", "unknown");

        SettingsException e = expectThrows(SettingsException.class, () -> new JiraService(builder.build(), httpClient, clusterSettings)
        );
        assertThat(e.getMessage(), is("could not find default account [unknown]"));
    }

    public void testUnsecureAccountUrl() throws Exception {
        Settings settings = Settings.builder().put("url", "http://localhost").put("user", "foo").put("password", "bar").build();
        SettingsException e = expectThrows(SettingsException.class, () -> new JiraAccount("test", settings, null));
        assertThat(e.getMessage(), containsString("invalid jira [test] account settings. unsecure scheme [HTTP]"));

        Settings disallowHttp = Settings.builder().put(settings).put("allow_http", false).build();
        e = expectThrows(SettingsException.class, () -> new JiraAccount("test", disallowHttp, null));
        assertThat(e.getMessage(), containsString("invalid jira [test] account settings. unsecure scheme [HTTP]"));

        Settings allowHttp = Settings.builder().put(settings).put("allow_http", true).build();
        assertNotNull(new JiraAccount("test", allowHttp, null));
    }

    public void testCreateIssueWithError() throws Exception {
        Settings.Builder builder = Settings.builder();
        addAccountSettings("account1", builder);

        JiraService service = new JiraService(builder.build(), httpClient, clusterSettings);
        JiraAccount account = service.getAccount("account1");

        Tuple<Integer, String> error = randomHttpError();

        when(httpClient.execute(any(HttpRequest.class))).thenReturn(new HttpResponse(error.v1()));
        JiraIssue issue = account.createIssue(emptyMap(), null);
        assertFalse(issue.successful());
        assertThat(issue.getFailureReason(), equalTo(error.v2()));
    }

    public void testCreateIssue() throws Exception {
        Settings.Builder builder = Settings.builder();
        addAccountSettings("account1", builder);

        JiraService service = new JiraService(builder.build(), httpClient, clusterSettings);
        JiraAccount account = service.getAccount("account1");

        ArgumentCaptor<HttpRequest> argumentCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        when(httpClient.execute(argumentCaptor.capture())).thenReturn(new HttpResponse(HttpStatus.SC_CREATED));

        Map<String, Object> fields = singletonMap("key", "value");

        JiraIssue issue = account.createIssue(fields, null);
        assertTrue(issue.successful());
        assertNull(issue.getFailureReason());

        HttpRequest sentRequest = argumentCaptor.getValue();
        assertThat(sentRequest.host(), equalTo("internal-jira.elastic.co"));
        assertThat(sentRequest.port(), equalTo(443));
        assertThat(sentRequest.scheme(), equalTo(Scheme.HTTPS));
        assertThat(sentRequest.path(), equalTo(JiraAccount.DEFAULT_PATH));
        assertThat(sentRequest.auth(), notNullValue());
        assertThat(sentRequest.body(), notNullValue());
    }

    private void addAccountSettings(String name, Settings.Builder builder) {
        builder.put("xpack.notification.jira.account." + name + "." + JiraAccount.URL_SETTING, "https://internal-jira.elastic.co:443");
        builder.put("xpack.notification.jira.account." + name + "." + JiraAccount.USER_SETTING, randomAlphaOfLength(10));
        builder.put("xpack.notification.jira.account." + name + "." + JiraAccount.PASSWORD_SETTING, randomAlphaOfLength(10));

        Map<String, Object> defaults = randomIssueDefaults();
        for (Map.Entry<String, Object> setting : defaults.entrySet()) {
            String key = "xpack.notification.jira.account." + name + "." + JiraAccount.ISSUE_DEFAULTS_SETTING + "." + setting.getKey();
            if (setting.getValue() instanceof String) {
                builder.put(key, setting.getValue());
            } else if (setting.getValue() instanceof Map) {
                builder.putProperties((Map) setting.getValue(), s -> true, s -> key + "." + s);
            }
        }
    }

    public static Map<String, Object> randomIssueDefaults() {
        MapBuilder<String, Object> builder = MapBuilder.newMapBuilder();
        if (randomBoolean()) {
            Map<String, Object> project = new HashMap<>();
            project.put("project", singletonMap("id", randomAlphaOfLength(10)));
            builder.putAll(project);
        }
        if (randomBoolean()) {
            Map<String, Object> project = new HashMap<>();
            project.put("issuetype", singletonMap("name", randomAlphaOfLength(5)));
            builder.putAll(project);
        }
        if (randomBoolean()) {
            builder.put("summary", randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            builder.put("description", randomAlphaOfLength(50));
        }
        if (randomBoolean()) {
            int count = randomIntBetween(0, 5);
            for (int i = 0; i < count; i++) {
                builder.put("customfield_" + i, randomAlphaOfLengthBetween(5, 10));
            }
        }
        return builder.immutableMap();
    }

    static Tuple<Integer, String> randomHttpError() {
        Tuple<Integer, String> error = randomFrom(
                tuple(400, "Bad Request"),
                tuple(401, "Unauthorized (authentication credentials are invalid)"),
                tuple(403, "Forbidden (account doesn't have permission to create this issue)"),
                tuple(404, "Not Found (account uses invalid JIRA REST APIs)"),
                tuple(408, "Request Timeout (request took too long to process)"),
                tuple(500, "JIRA Server Error (internal error occurred while processing request)"),
                tuple(666, "Unknown Error")
        );
        return error;
    }
}
