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
import org.elasticsearch.core.Tuple;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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

    // Token URLs should be in the form of "example.com:8200=<token>,other.com:80=<token>",
    // i.e., a list of comma-separated <host>:<port>=<token> pairs
    public static final Setting<SecureString> SETTING_WEBHOOK_HOST_TOKEN_PAIRS = SecureSetting.secureString(
        "xpack.notification.webhook.host_token_pairs",
        null
    );

    // Boolean setting for whether token should be sent if present.
    public static final Setting<Boolean> SETTING_WEBHOOK_TOKEN_ENABLED = Setting.boolSetting(
        "xpack.notification.webhook.additional_token_enabled",
        false,
        Setting.Property.NodeScope
    );

    private final HttpClient httpClient;
    private final boolean additionalTokenEnabled;

    public WebhookService(Settings settings, HttpClient httpClient, ClusterSettings clusterSettings) {
        super(NAME, settings, clusterSettings, List.of(), getSecureSettings());
        this.httpClient = httpClient;
        // do an initial load of all settings
        reload(settings);
        this.additionalTokenEnabled = SETTING_WEBHOOK_TOKEN_ENABLED.get(settings);
    }

    public static List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>(getSecureSettings());
        settings.add(SETTING_WEBHOOK_TOKEN_ENABLED);
        return settings;
    }

    private static List<Setting<?>> getSecureSettings() {
        return List.of(SETTING_WEBHOOK_HOST_TOKEN_PAIRS);
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
        HttpRequest originalRequest = action.getRequest().render(templateEngine, model);

        if (ctx.simulateAction(actionId)) {
            HttpRequest request = maybeModifyHttpRequest(originalRequest);
            // If the request was modified, then the request has had the token added
            boolean tokenAdded = originalRequest != request;
            // Skip execution, return only the simulated (and redacted if necessary) response
            return new WebhookAction.Result.Simulated(
                tokenAdded ? request.copy().setHeader(TOKEN_HEADER_NAME, WatcherXContentParser.REDACTED_PASSWORD).build() : request
            );
        }

        final Tuple<HttpRequest, HttpResponse> respTup = modifyAndExecuteHttpRequest(originalRequest);
        final HttpRequest request = respTup.v1();
        final HttpResponse response = respTup.v2();
        // If the request was modified, then the request has had the token added
        final boolean tokenAdded = originalRequest != request;

        final Function<HttpRequest, HttpRequest> redactToken = tokenAdded
            ? req -> req.copy().setHeader(TOKEN_HEADER_NAME, WatcherXContentParser.REDACTED_PASSWORD).build()
            : Function.identity();

        if (response.status() >= 400) {
            return new WebhookAction.Result.Failure(redactToken.apply(request), response);
        } else {
            return new WebhookAction.Result.Success(redactToken.apply(request), response);
        }
    }

    /**
     * Makes any additional modifications to the {@link HttpRequest} if necessary.
     * If no modifications are made the same instance is returned, otherwise a new
     * HttpRequest is returned.
     */
    HttpRequest maybeModifyHttpRequest(HttpRequest request) {
        WebhookAccount account = getAccount(NAME);
        if (this.additionalTokenEnabled && account.hostTokenMap.size() > 0) {
            // Generate a string like example.com:9200 to match against the list of hosts where the
            // additional token should be provided. The token will only be added to the headers if
            // the request matches the list.
            String reqHostAndPort = request.host() + ":" + request.port();
            if (Strings.hasText(account.hostTokenMap.get(reqHostAndPort))) {
                // Add the additional token
                logger.debug(
                    "additional [{}] header token added to watcher webhook request for {}://{}:{}",
                    TOKEN_HEADER_NAME,
                    request.scheme().scheme(),
                    request.host(),
                    request.port()
                );
                return request.copy().setHeader(TOKEN_HEADER_NAME, account.hostTokenMap.get(reqHostAndPort)).build();
            }
        }
        return request;
    }

    /**
     * Executes the given {@link HttpRequest} after any necessary modifications.
     * A tuple of the modified (or unmodified) {@link HttpRequest} and
     * {@link HttpResponse} is returned.
     */
    public Tuple<HttpRequest, HttpResponse> modifyAndExecuteHttpRequest(HttpRequest request) throws IOException {
        final HttpRequest modifiedRequest = maybeModifyHttpRequest(request);
        final HttpResponse response = httpClient.execute(modifiedRequest);
        logger.debug(
            "executed watcher webhook request for {}://{}:{}, response code: {}",
            modifiedRequest.scheme().scheme(),
            modifiedRequest.host(),
            modifiedRequest.port(),
            response.status()
        );
        return Tuple.tuple(modifiedRequest, response);
    }

    public static final class WebhookAccount {
        private final Map<String, String> hostTokenMap;

        public WebhookAccount(Settings settings) {
            SecureString validTokenHosts = SETTING_WEBHOOK_HOST_TOKEN_PAIRS.get(settings);
            if (Strings.hasText(validTokenHosts)) {
                Set<String> hostAndTokens = Strings.commaDelimitedListToSet(validTokenHosts.toString());
                Map<String, String> hostAndPortToToken = new HashMap<>(hostAndTokens.size());
                for (String hostPortToken : hostAndTokens) {
                    int equalsIndex = hostPortToken.indexOf('=');
                    if (equalsIndex == -1) {
                        // This is an invalid format, and we can skip this token
                        break;
                    }
                    if (equalsIndex + 1 == hostPortToken.length()) {
                        // This is also invalid, because it ends in a trailing =
                        break;
                    }
                    // The first part becomes the <host>:<port> pair
                    String hostAndPort = hostPortToken.substring(0, equalsIndex);
                    // The second part after the '=' is the <token>
                    String token = hostPortToken.substring(equalsIndex + 1);
                    hostAndPortToToken.put(hostAndPort, token);
                }
                this.hostTokenMap = Collections.unmodifiableMap(hostAndPortToToken);
            } else {
                this.hostTokenMap = Map.of();
            }
        }

        @Override
        public String toString() {
            return "WebhookAccount[" + this.hostTokenMap.keySet().stream().map(s -> s + "=********") + "]";
        }
    }
}
