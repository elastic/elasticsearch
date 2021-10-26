/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.jira;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.execution.Wid;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.http.BasicAuth;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpProxy;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.notification.jira.JiraAccount;
import org.elasticsearch.xpack.watcher.notification.jira.JiraService;
import org.mockito.ArgumentCaptor;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.mockExecutionContextBuilder;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExecutableJiraActionTests extends ESTestCase {

    public void testProxy() throws Exception {
        HttpProxy proxy = new HttpProxy("localhost", 8080);
        Map<String, Object> issueDefaults = Collections.singletonMap("customfield_0001", "test");
        JiraAction action = new JiraAction("account1", issueDefaults, proxy);

        HttpClient httpClient = mock(HttpClient.class);
        ArgumentCaptor<HttpRequest> argumentCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        when(httpClient.execute(argumentCaptor.capture())).thenReturn(new HttpResponse(200));

        final String host = randomFrom("localhost", "internal-jira.elastic.co");
        final int port = randomFrom(80, 8080, 449, 9443);
        final String url = "https://" + host + ":" + port;
        final String user = randomAlphaOfLength(10);
        final String password = randomAlphaOfLength(10);

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(JiraAccount.SECURE_URL_SETTING.getKey(), url);
        secureSettings.setString(JiraAccount.SECURE_USER_SETTING.getKey(), user);
        secureSettings.setString(JiraAccount.SECURE_PASSWORD_SETTING.getKey(), password);
        Settings accountSettings = Settings.builder().setSecureSettings(secureSettings).build();

        JiraAccount account = new JiraAccount("account1", accountSettings, httpClient);

        JiraService service = mock(JiraService.class);
        when(service.getAccount(eq("account1"))).thenReturn(account);

        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);

        Wid wid = new Wid(randomAlphaOfLength(5), now);
        WatchExecutionContext ctx = mockExecutionContextBuilder(wid.watchId())
                .wid(wid)
                .payload(new Payload.Simple())
                .time(wid.watchId(), now)
                .buildMock();

        ExecutableJiraAction executable = new ExecutableJiraAction(action, logger, service, new UpperCaseTextTemplateEngine());
        executable.execute("foo", ctx, new Payload.Simple());

        HttpRequest request = argumentCaptor.getValue();
        assertThat(request.proxy(), is(proxy));
        assertThat(request.host(), is(host));
        assertThat(request.port(), is(port));
        assertThat(request.path(), is(JiraAccount.DEFAULT_PATH));

        BasicAuth httpAuth = request.auth();

        assertThat(httpAuth.getUsername(), is(user));
    }

    public void testExecutionWithNoDefaults() throws Exception {
        JiraAction.Simulated result = simulateExecution(singletonMap("key", "value"), emptyMap());
        assertEquals(result.getFields().size(), 1);
        assertThat(result.getFields(), hasEntry("KEY", "VALUE"));
    }

    public void testExecutionNoFieldsWithDefaults() throws Exception {
        Map<String, String> defaults = new HashMap<>();
        defaults.put("k0", "v0");

        JiraAction.Simulated result = simulateExecution(new HashMap<>(), defaults);
        assertEquals(result.getFields().size(), 1);
        assertThat(result.getFields(), hasEntry("K0", "V0"));

        defaults.put("k1", "v1");

        result = simulateExecution(new HashMap<>(), defaults);
        assertEquals(result.getFields().size(), 2);
        assertThat(result.getFields(), allOf(hasEntry("K0", "V0"), hasEntry("K1", "V1")));
    }

    public void testExecutionFields() throws Exception {
        Map<String, String> defaults = new HashMap<>();
        defaults.put("k0", "v0");
        defaults.put("k1", "v1");

        Map<String, Object> fields = new HashMap<>();
        fields.put("k1", "new_v1"); // overridden
        fields.put("k2", "v2");
        fields.put("k3", "v3");

        JiraAction.Simulated result = simulateExecution(fields, defaults);
        assertEquals(result.getFields().size(), 4);
        assertThat(result.getFields(), allOf(hasEntry("K0", "V0"), hasEntry("K1", "NEW_V1"), hasEntry("K2", "V2"), hasEntry("K3", "V3")));
    }

    public void testExecutionFieldsMaps() throws Exception {
        Map<String, String> defaults = new HashMap<>();
        defaults.put("k0.a", "b");
        defaults.put("k1.c", "d");
        defaults.put("k1.e", "f");
        defaults.put("k1.g.a", "b");

        Map<String, Object> fields = new HashMap<>();
        fields.put("k2", "v2");
        fields.put("k3", "v3");

        JiraAction.Simulated result = simulateExecution(fields, defaults);

        final Map<String, Object> expected = new HashMap<>();
        expected.put("K0", singletonMap("A", "B"));
        expected.put("K2", "V2");
        expected.put("K3", "V3");

        final Map<String, Object> expectedK1 = new HashMap<>();
        expectedK1.put("C", "D");
        expectedK1.put("E", "F");
        expectedK1.put("G", singletonMap("A", "B"));
        expected.put("K1", expectedK1);

        assertThat(result.getFields(), equalTo(expected));
    }

    public void testExecutionFieldsMapsAreOverridden() throws Exception {
        Map<String, String> defaults = new HashMap<>();
        defaults.put("k0", "v0");
        defaults.put("k1.a", "b");
        defaults.put("k1.c", "d");

        Map<String, Object> fields = new HashMap<>();
        fields.put("k1", singletonMap("c", "e")); // will overrides the defaults
        fields.put("k2", "v2");

        JiraAction.Simulated result = simulateExecution(fields, defaults);

        final Map<String, Object> expected = new HashMap<>();
        expected.put("K0", "V0");
        expected.put("K1", singletonMap("C", "E"));
        expected.put("K2", "V2");

        assertThat(result.getFields(), equalTo(expected));
    }

    public void testExecutionFieldsLists() throws Exception {
        Map<String, String> defaults = new HashMap<>();
        defaults.put("k0.0", "a");
        defaults.put("k0.1", "b");
        defaults.put("k0.2", "c");
        defaults.put("k1", "v1");

        Map<String, Object> fields = new HashMap<>();
        fields.put("k2", "v2");
        fields.put("k3", Arrays.asList("d", "e", "f"));

        JiraAction.Simulated result = simulateExecution(fields, defaults);

        final Map<String, Object> expected = new HashMap<>();
        expected.put("K0", Arrays.asList("A", "B", "C"));
        expected.put("K1", "V1");
        expected.put("K2", "V2");
        expected.put("K3", Arrays.asList("D", "E", "F"));

        assertThat(result.getFields(), equalTo(expected));
    }

    public void testExecutionFieldsListsNotOverridden() throws Exception {
        Map<String, String> defaults = new HashMap<>();
        defaults.put("k0.0", "a");
        defaults.put("k0.1", "b");
        defaults.put("k0.2", "c");

        Map<String, Object> fields = new HashMap<>();
        fields.put("k1", "v1");
        fields.put("k0", Arrays.asList("d", "e", "f")); // should not be overridden byt the defaults

        JiraAction.Simulated result = simulateExecution(fields, defaults);

        final Map<String, Object> expected = new HashMap<>();
        expected.put("K0", Arrays.asList("D", "E", "F"));
        expected.put("K1", "V1");

        assertThat(result.getFields(), equalTo(expected));
    }

    public void testExecutionFieldsStringArrays() throws Exception {
        Settings build = Settings.builder()
                .putList("k0", "a", "b", "c")
                .put("k1", "v1")
                .build();
        Map<String, String> defaults = build.keySet().stream().collect(Collectors.toMap(Function.identity(), k -> build.get(k)));

        Map<String, Object> fields = new HashMap<>();
        fields.put("k2", "v2");
        fields.put("k3", new String[]{"d", "e", "f"});

        JiraAction.Simulated result = simulateExecution(fields, defaults);

        assertThat(result.getFields().get("K1"), equalTo("V1"));
        assertThat(result.getFields().get("K2"), equalTo("V2"));
        assertArrayEquals((Object[]) result.getFields().get("K3"), new Object[]{"D", "E", "F"});
    }

    public void testExecutionFieldsStringArraysNotOverridden() throws Exception {
        Settings build = Settings.builder()
                .putList("k0", "a", "b", "c")
                .build();
        Map<String, String> defaults = build.keySet().stream().collect(Collectors.toMap(Function.identity(), k -> build.get(k)));
        Map<String, Object> fields = new HashMap<>();
        fields.put("k1", "v1");
        fields.put("k0", new String[]{"d", "e", "f"}); // should not be overridden byt the defaults

        JiraAction.Simulated result = simulateExecution(fields, defaults);

        final Map<String, Object> expected = new HashMap<>();
        expected.put("K0", new String[]{"D", "E", "F"});
        expected.put("K1", "V1");

        assertArrayEquals((Object[]) result.getFields().get("K0"), new Object[]{"D", "E", "F"});
        assertThat(result.getFields().get("K1"), equalTo("V1"));
    }

    private JiraAction.Simulated simulateExecution(Map<String, Object> actionFields, Map<String, String> accountFields) throws Exception {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(JiraAccount.SECURE_URL_SETTING.getKey(), "https://internal-jira.elastic.co:443");
        secureSettings.setString(JiraAccount.SECURE_USER_SETTING.getKey(), "elastic");
        secureSettings.setString(JiraAccount.SECURE_PASSWORD_SETTING.getKey(), "secret");
        Settings.Builder settings = Settings.builder()
                .setSecureSettings(secureSettings)
                .putProperties(accountFields, s -> "issue_defaults." + s);

        JiraAccount account = new JiraAccount("account", settings.build(), mock(HttpClient.class));

        JiraService service = mock(JiraService.class);
        when(service.getAccount(eq("account"))).thenReturn(account);

        JiraAction action = new JiraAction("account", actionFields, null);
        ExecutableJiraAction executable = new ExecutableJiraAction(action, null, service, new UpperCaseTextTemplateEngine());

        WatchExecutionContext context = createWatchExecutionContext();
        when(context.simulateAction("test")).thenReturn(true);

        Action.Result result = executable.execute("test", context, new Payload.Simple());
        assertThat(result, instanceOf(JiraAction.Result.class));
        assertThat(result, instanceOf(JiraAction.Simulated.class));
        return (JiraAction.Simulated) result;
    }

    private WatchExecutionContext createWatchExecutionContext() {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        Wid wid = new Wid(randomAlphaOfLength(5), now);
        Map<String, Object> metadata = MapBuilder.<String, Object>newMapBuilder().put("_key", "_val").map();
        return mockExecutionContextBuilder("watch1")
                .wid(wid)
                .payload(new Payload.Simple())
                .time("watch1", now)
                .metadata(metadata)
                .buildMock();
    }

    /**
     * TextTemplateEngine that convert templates to uppercase
     */
    class UpperCaseTextTemplateEngine extends TextTemplateEngine {

        UpperCaseTextTemplateEngine() {
            super(mock(ScriptService.class));
        }

        @Override
        public String render(TextTemplate textTemplate, Map<String, Object> model) {
            return textTemplate.getTemplate().toUpperCase(Locale.ROOT);
        }
    }

    @SuppressWarnings("unchecked")
    public void testMerge() {
        Map<String, Object> writeableMap = new HashMap<>();
        Map<String, Object> mergeNull = ExecutableJiraAction.merge(writeableMap, null, s -> s);
        assertTrue(mergeNull.isEmpty());
        Map<String, Object> map = new HashMap<>();
        map.put("foo", "bar");
        map.put("list", Arrays.asList("test1", "test2"));
        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put("var", "abc");
        map.put("map", valueMap);
        Map<String, Object> componentMap = new HashMap<>();
        componentMap.put("name", "value");
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(componentMap);
        map.put("components", list);
        Map<String, Object> result = ExecutableJiraAction.merge(writeableMap, map, s -> s.toUpperCase(Locale.ROOT));
        assertThat(result, hasEntry("FOO", "BAR"));
        assertThat(result.get("LIST"), instanceOf(List.class));
        List<String> mergedList = (List<String>) result.get("LIST");
        assertEquals(2, mergedList.size());
        assertEquals("TEST1", mergedList.get(0));
        assertEquals("TEST2", mergedList.get(1));
        Map<String, Object> mergedMap = (Map<String, Object>) result.get("MAP");
        assertEquals(1, mergedMap.size());
        assertEquals("ABC", mergedMap.get("VAR"));
        assertThat(result.get("COMPONENTS"), instanceOf(List.class));
        List<Map<String, Object>> components = (List<Map<String, Object>>) result.get("COMPONENTS");
        assertThat(components.get(0), hasEntry("NAME", "VALUE"));

        // test the fields is not overwritten
        Map<String, Object> fields = new HashMap<>();
        fields.put("FOO", "bob");
        fields.put("LIST", Arrays.asList("test3"));
        fields.put("MAP", new HashMap<>());
        fields.put("COMPONENTS", new ArrayList<>());

        result = ExecutableJiraAction.merge(fields, map, s -> s.toUpperCase(Locale.ROOT));
        assertThat(result, hasEntry("FOO", "bob"));
        assertThat(result.get("LIST"), instanceOf(List.class));
        mergedList = (List<String>) result.get("LIST");
        assertEquals(1, mergedList.size());
        assertEquals("test3", mergedList.get(0));
        mergedMap = (Map<String, Object>) result.get("MAP");
        assertTrue(mergedMap.isEmpty());
        assertThat(result.get("COMPONENTS"), instanceOf(List.class));
        components = (List<Map<String, Object>>) result.get("COMPONENTS");
        assertTrue(components.isEmpty());
    }

}
