/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.slack;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.execution.Wid;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpProxy;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.notification.slack.SlackAccount;
import org.elasticsearch.xpack.watcher.notification.slack.SlackService;
import org.elasticsearch.xpack.watcher.notification.slack.message.SlackMessage;
import org.elasticsearch.xpack.watcher.test.MockTextTemplateEngine;
import org.mockito.ArgumentCaptor;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.mockExecutionContextBuilder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExecutableSlackActionTests extends ESTestCase {

    public void testProxy() throws Exception {
        HttpProxy proxy = new HttpProxy("localhost", 8080);
        SlackMessage.Template messageTemplate = SlackMessage.Template.builder().addTo("to").setText(new TextTemplate("content")).build();
        SlackAction action = new SlackAction("account1", messageTemplate, proxy);

        HttpClient httpClient = mock(HttpClient.class);
        ArgumentCaptor<HttpRequest> argumentCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        when(httpClient.execute(argumentCaptor.capture())).thenReturn(new HttpResponse(200));

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("secure_url", "http://example.org");
        Settings accountSettings = Settings.builder().setSecureSettings(secureSettings).build();
        SlackAccount account = new SlackAccount("account1", accountSettings, httpClient, logger);

        SlackService service = mock(SlackService.class);
        when(service.getAccount(eq("account1"))).thenReturn(account);

        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);

        Wid wid = new Wid(randomAlphaOfLength(5), now);
        WatchExecutionContext ctx = mockExecutionContextBuilder(wid.watchId())
                .wid(wid)
                .payload(new Payload.Simple())
                .time(wid.watchId(), now)
                .buildMock();

        ExecutableSlackAction executable = new ExecutableSlackAction(action, logger, service, new MockTextTemplateEngine());
        executable.execute("foo", ctx, new Payload.Simple());

        HttpRequest request = argumentCaptor.getValue();
        assertThat(request.proxy(), is(proxy));
    }

}
