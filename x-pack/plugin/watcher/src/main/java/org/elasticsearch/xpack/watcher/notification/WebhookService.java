/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher.notification;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherXContentParser;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.actions.webhook.WebhookAction;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.support.Variables;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * The WebhookService class handles executing webhook requests for Watcher actions. These can be
 * regular "webhook" actions as well as parts of an "email" action with attachments that make HTTP
 * requests.
 */
public class WebhookService extends NotificationService<WebhookService.WebhookAccount> {
    public static final String NAME = "webhook";

    private static final Logger logger = LogManager.getLogger(WebhookService.class);

    public static final String TOKEN_HEADER_NAME = "X-Elastic-App-Auth";
    public static final Setting<SecureString> SETTING_WEBHOOK_ADDITIONAL_TOKEN = SecureSetting.secureString(
        "xpack.notification.webhook.additional_token",
        null
    );

    // Token URLs should be in the form of "example.com:8200,other.com:80",
    // i.e., a list of comma-separated hosts and ports
    public static final Setting<SecureString> SETTING_WEBHOOK_TOKEN_HOSTS = SecureSetting.secureString(
        "xpack.notification.webhook.token_hosts",
        null
    );

    private final HttpClient httpClient;

    public WebhookService(Settings settings, HttpClient httpClient, ClusterSettings clusterSettings) {
        super(NAME, settings, clusterSettings, List.of(), getSecureSettings());
        this.httpClient = httpClient;
        // do an initial load of all settings
        reload(settings);
    }

    public static List<Setting<?>> getSettings() {
        return getSecureSettings();
    }

    private static List<Setting<?>> getSecureSettings() {
        return List.of(SETTING_WEBHOOK_ADDITIONAL_TOKEN, SETTING_WEBHOOK_TOKEN_HOSTS);
    }

    @Override
    protected String getDefaultAccountName(Settings settings) {
        // There are no accounts for webhooks, but we still want a default
        // account because Watcher's notification services infrastructure
        // expects one, so use "webhook" as the name.
        return NAME;
    }

    @Override
    protected WebhookAccount createAccount(String name, Settings accountSettings) {
        throw new UnsupportedOperationException("this should never be called");
    }

    @Override
    protected Map<String, LazyInitializable<WebhookAccount, SettingsException>> createAccounts(
        Settings settings,
        Set<String> accountNames,
        BiFunction<String, Settings, WebhookAccount> accountFactory
    ) {
        // We override the createAccounts here because there are no real "accounts" for webhooks.
        // Instead, we create the single account. This simplifies a great deal around dynamic
        // setting keys for accounts
        final WebhookAccount defaultAccount = new WebhookAccount(settings);
        return Map.of(NAME, new LazyInitializable<>(() -> defaultAccount));
    }

    public Action.Result execute(
        String actionId,
        WebhookAction action,
        TextTemplateEngine templateEngine,
        WatchExecutionContext ctx,
        Payload payload
    ) throws IOException {
        Map<String, Object> model = Variables.createCtxParamsMap(ctx, payload);

        // Render the original request
        HttpRequest request = action.getRequest().render(templateEngine, model);

        // If applicable, add the extra token to the headers
        boolean tokenAdded = false;
        WebhookAccount account = getAccount(NAME);
        if (account.validTokenHosts.size() > 0 && Strings.hasText(account.token)) {
            // Generate a string like example.com:9200 to match against the list of hosts where the
            // additional token should be provided. The token will only be added to the headers if
            // the request matches the list.
            String reqHostAndPort = request.host() + ":" + request.port();
            if (account.validTokenHosts.contains(reqHostAndPort)) {
                // Add the additional token
                tokenAdded = true;
                request = request.copy().setHeader(TOKEN_HEADER_NAME, account.token).build();
            }
        }

        final Function<HttpRequest, HttpRequest> redactToken = tokenAdded
            ? req -> req.copy().setHeader(TOKEN_HEADER_NAME, WatcherXContentParser.REDACTED_PASSWORD).build()
            : Function.identity();

        if (ctx.simulateAction(actionId)) {
            // Skip execution, return only the simulated (and redacted if necessary) response
            return new WebhookAction.Result.Simulated(redactToken.apply(request));
        }

        HttpResponse response = httpClient.execute(request);

        if (response.status() >= 400) {
            return new WebhookAction.Result.Failure(redactToken.apply(request), response);
        } else {
            return new WebhookAction.Result.Success(redactToken.apply(request), response);
        }
    }

    public static final class WebhookAccount {
        @Nullable
        private final String token;
        private final Set<String> validTokenHosts;

        public WebhookAccount(Settings settings) {
            SecureString validTokenHosts = SETTING_WEBHOOK_TOKEN_HOSTS.get(settings);
            if (Strings.hasText(validTokenHosts)) {
                SecureString tokenText = SETTING_WEBHOOK_ADDITIONAL_TOKEN.get(settings);
                this.validTokenHosts = Strings.commaDelimitedListToSet(validTokenHosts.toString());
                this.token = Strings.hasText(tokenText) ? tokenText.toString() : "";
            } else {
                this.validTokenHosts = Set.of();
                this.token = null;
            }
        }

        @Override
        public String toString() {
            return "WebhookAccount[token="
                + (token == null ? "<missing>" : "********")
                + ",hosts="
                + Strings.collectionToCommaDelimitedString(validTokenHosts)
                + "]";
        }
    }
}
