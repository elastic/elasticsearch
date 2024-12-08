/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher.notification;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpMethod;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.common.http.Scheme;

import java.io.IOException;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WebhookServiceTests extends ESTestCase {
    public void testModifyRequest() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put(WebhookService.SETTING_WEBHOOK_TOKEN_ENABLED.getKey(), true);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(
            WebhookService.SETTING_WEBHOOK_HOST_TOKEN_PAIRS.getKey(),
            "host1.com:1234=token1234,host2.org:2345=token2345"
        );
        builder.setSecureSettings(secureSettings);
        Settings finalSettings = builder.build();

        HttpClient mockClient = mock(HttpClient.class);
        WebhookService service = new WebhookService(
            finalSettings,
            mockClient,
            new ClusterSettings(
                finalSettings,
                Set.of(WebhookService.SETTING_WEBHOOK_TOKEN_ENABLED, WebhookService.SETTING_WEBHOOK_HOST_TOKEN_PAIRS)
            )
        );

        HttpRequest nonTokenReq = new HttpRequest(
            "example.com",
            80,
            Scheme.HTTP,
            HttpMethod.GET,
            "/",
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        HttpRequest host1Req = new HttpRequest(
            "host1.com",
            1234,
            Scheme.HTTP,
            HttpMethod.GET,
            "/",
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        HttpRequest host1WrongPortReq = new HttpRequest(
            "host1.com",
            3456,
            Scheme.HTTP,
            HttpMethod.GET,
            "/",
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        HttpRequest host2Req = new HttpRequest(
            "host2.org",
            2345,
            Scheme.HTTP,
            HttpMethod.GET,
            "/",
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        assertSame(service.maybeModifyHttpRequest(nonTokenReq), nonTokenReq);
        assertSame(service.maybeModifyHttpRequest(host1WrongPortReq), host1WrongPortReq);
        assertThat(service.maybeModifyHttpRequest(host2Req).headers().get(WebhookService.TOKEN_HEADER_NAME), equalTo("token2345"));

        HttpRequest host1ModReq = service.maybeModifyHttpRequest(host1Req);
        assertThat(host1ModReq.headers().get(WebhookService.TOKEN_HEADER_NAME), equalTo("token1234"));
        when(mockClient.execute(any(HttpRequest.class))).thenReturn(new HttpResponse(200, "{}"));
        service.modifyAndExecuteHttpRequest(host1Req);
        verify(mockClient).execute(host1ModReq);
    }
}
