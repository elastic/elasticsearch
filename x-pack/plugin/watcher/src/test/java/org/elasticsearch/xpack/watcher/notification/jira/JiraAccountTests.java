/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.jira;

import org.apache.http.HttpStatus;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpProxy;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.common.http.Scheme;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.core.Tuple.tuple;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JiraAccountTests extends ESTestCase {

    private HttpClient httpClient;
    private ClusterSettings clusterSettings;

    @Before
    public void init() throws Exception {
        httpClient = mock(HttpClient.class);
        clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(JiraService.getSettings()));
    }

    public void testJiraAccountSettings() {
        final String url = "https://internal-jira.elastic.co:443";
        final MockSecureSettings secureSettings = new MockSecureSettings();

        SettingsException e = expectThrows(SettingsException.class, () -> new JiraAccount(null, Settings.EMPTY, null));
        assertThat(e.getMessage(), containsString("invalid jira [null] account settings. missing required [secure_url] setting"));

        secureSettings.setString("secure_url", url);
        Settings settings1 = Settings.builder().setSecureSettings(secureSettings).build();
        e = expectThrows(SettingsException.class, () -> new JiraAccount("test", settings1, null));
        assertThat(e.getMessage(), containsString("invalid jira [test] account settings. missing required [secure_user] setting"));

        secureSettings.setString("secure_user", "");
        Settings settings2 = Settings.builder().setSecureSettings(secureSettings).build();
        e = expectThrows(SettingsException.class, () -> new JiraAccount("test", settings2, null));
        assertThat(e.getMessage(), containsString("invalid jira [test] account settings. missing required [secure_user] setting"));

        secureSettings.setString("secure_user", "foo");
        Settings settings3 = Settings.builder().setSecureSettings(secureSettings).build();
        e = expectThrows(SettingsException.class, () -> new JiraAccount("test", settings3, null));
        assertThat(e.getMessage(), containsString("invalid jira [test] account settings. missing required [secure_password] setting"));

        secureSettings.setString("secure_password", "");
        Settings settings4 = Settings.builder().setSecureSettings(secureSettings).build();
        e = expectThrows(SettingsException.class, () -> new JiraAccount("test", settings4, null));
        assertThat(e.getMessage(), containsString("invalid jira [test] account settings. missing required [secure_password] setting"));
    }

    public void testInvalidSchemeUrl() throws Exception {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(JiraAccount.SECURE_URL_SETTING.getKey(), "test"); // Setting test as invalid scheme url
        secureSettings.setString(JiraAccount.SECURE_USER_SETTING.getKey(), "foo");
        secureSettings.setString(JiraAccount.SECURE_PASSWORD_SETTING.getKey(), "password");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        SettingsException e = expectThrows(SettingsException.class, () -> new JiraAccount("test", settings, null));
        assertThat(e.getMessage(), containsString("invalid jira [test] account settings. invalid [secure_url] setting"));
    }

    public void testUnsecureAccountUrl() throws Exception {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(JiraAccount.SECURE_USER_SETTING.getKey(), "foo");
        secureSettings.setString(JiraAccount.SECURE_PASSWORD_SETTING.getKey(), "password");
        secureSettings.setString(JiraAccount.SECURE_URL_SETTING.getKey(), "http://localhost");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
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

    public void testCustomUrls() throws Exception {
        assertCustomUrl("https://localhost/foo", "/foo");
        assertCustomUrl("https://localhost/foo/", "/foo/");
        // this ensures we retain backwards compatibility
        assertCustomUrl("https://localhost/", JiraAccount.DEFAULT_PATH);
        assertCustomUrl("https://localhost", JiraAccount.DEFAULT_PATH);
    }

    private void assertCustomUrl(String urlSettings, String expectedPath) throws IOException {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("secure_url", urlSettings);
        secureSettings.setString("secure_user", "foo");
        secureSettings.setString("secure_password", "bar");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        HttpClient client = mock(HttpClient.class);

        HttpResponse response = new HttpResponse(200);
        when(client.execute(any())).thenReturn(response);

        JiraAccount jiraAccount = new JiraAccount("test", settings, client);
        jiraAccount.createIssue(Collections.emptyMap(), HttpProxy.NO_PROXY);

        ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(client, times(1)).execute(captor.capture());
        assertThat(captor.getAllValues(), hasSize(1));
        HttpRequest request = captor.getValue();
        assertThat(request.path(), is(expectedPath));
    }

    @SuppressWarnings("unchecked")
    private void addAccountSettings(String name, Settings.Builder builder) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(
            "xpack.notification.jira.account." + name + "." + JiraAccount.SECURE_URL_SETTING.getKey(),
            "https://internal-jira.elastic.co:443"
        );
        secureSettings.setString(
            "xpack.notification.jira.account." + name + "." + JiraAccount.SECURE_USER_SETTING.getKey(),
            randomAlphaOfLength(10)
        );
        secureSettings.setString(
            "xpack.notification.jira.account." + name + "." + JiraAccount.SECURE_PASSWORD_SETTING.getKey(),
            randomAlphaOfLength(10)
        );
        builder.setSecureSettings(secureSettings);

        Map<String, Object> defaults = randomIssueDefaults();
        for (Map.Entry<String, Object> setting : defaults.entrySet()) {
            String key = "xpack.notification.jira.account." + name + "." + JiraAccount.ISSUE_DEFAULTS_SETTING + "." + setting.getKey();
            if (setting.getValue() instanceof String) {
                builder.put(key, setting.getValue().toString());
            } else if (setting.getValue() instanceof Map) {
                builder.putProperties((Map) setting.getValue(), s -> key + "." + s);
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
