/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.hipchat;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpProxy;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.HashSet;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HipChatAccountsTests extends ESTestCase {
    private HttpClient httpClient;

    @Before
    public void init() throws Exception {
        httpClient = mock(HttpClient.class);
    }

    public void testProxy() throws Exception {
        Settings.Builder builder = Settings.builder()
                .put("xpack.notification.hipchat.default_account", "account1");
        addAccountSettings("account1", builder);
        HipChatService service = new HipChatService(builder.build(), httpClient, new ClusterSettings(Settings.EMPTY,
                new HashSet<>(HipChatService.getSettings())));
        HipChatAccount account = service.getAccount("account1");

        HipChatMessage hipChatMessage = new HipChatMessage("body", new String[]{"rooms"}, null, "from", null, null, null);

        ArgumentCaptor<HttpRequest> argumentCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        when(httpClient.execute(argumentCaptor.capture())).thenReturn(new HttpResponse(200));

        HttpProxy proxy = new HttpProxy("localhost", 8080);
        account.send(hipChatMessage, proxy);

        HttpRequest request = argumentCaptor.getValue();
        assertThat(request.proxy(), is(proxy));
    }

    private void addAccountSettings(String name, Settings.Builder builder) {
        HipChatAccount.Profile profile = randomFrom(HipChatAccount.Profile.values());
        builder.put("xpack.notification.hipchat.account." + name + ".profile", profile.value());
        builder.put("xpack.notification.hipchat.account." + name + ".auth_token", randomAlphaOfLength(50));
        if (profile == HipChatAccount.Profile.INTEGRATION) {
            builder.put("xpack.notification.hipchat.account." + name + ".room", randomAlphaOfLength(10));
        }
    }
}
