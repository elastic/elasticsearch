/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.jira;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.watcher.common.http.BasicAuth;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpMethod;
import org.elasticsearch.xpack.watcher.common.http.HttpProxy;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.common.http.Scheme;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;

public class JiraAccount {

    /**
     * Default JIRA REST API path for create issues
     **/
    public static final String DEFAULT_PATH = "/rest/api/2/issue";

    static final String ISSUE_DEFAULTS_SETTING = "issue_defaults";
    static final String ALLOW_HTTP_SETTING = "allow_http";

    public static final Setting<SecureString> SECURE_USER_SETTING = SecureSetting.secureString("secure_user", null);
    public static final Setting<SecureString> SECURE_PASSWORD_SETTING = SecureSetting.secureString("secure_password", null);
    public static final Setting<SecureString> SECURE_URL_SETTING = SecureSetting.secureString("secure_url", null);

    private final HttpClient httpClient;
    private final String name;
    private final String user;
    private final String password;
    private final URI url;
    private final Map<String, Object> issueDefaults;

    public JiraAccount(String name, Settings settings, HttpClient httpClient) {
        this.httpClient = httpClient;
        this.name = name;
        String url = getSetting(name, settings, SECURE_URL_SETTING);
        try {
            URI uri = new URI(url);
            if (uri.getScheme() == null) {
                throw new URISyntaxException("null", "No scheme defined in url");
            }
            Scheme protocol = Scheme.parse(uri.getScheme());
            if ((protocol == Scheme.HTTP) && (Booleans.isTrue(settings.get(ALLOW_HTTP_SETTING)) == false)) {
                throw new SettingsException("invalid jira [" + name + "] account settings. unsecure scheme [" + protocol + "]");
            }
            this.url = uri;
        } catch (URISyntaxException | IllegalArgumentException e) {
            throw new SettingsException(
                "invalid jira [" + name + "] account settings. invalid [" + SECURE_URL_SETTING.getKey() + "] setting",
                e
            );
        }
        this.user = getSetting(name, settings, SECURE_USER_SETTING);
        this.password = getSetting(name, settings, SECURE_PASSWORD_SETTING);
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.startObject();
            settings.getAsSettings(ISSUE_DEFAULTS_SETTING).toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            try (
                InputStream stream = BytesReference.bytes(builder).streamInput();
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(new NamedXContentRegistry(Collections.emptyList()), LoggingDeprecationHandler.INSTANCE, stream)
            ) {
                this.issueDefaults = Collections.unmodifiableMap(parser.map());
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static String getSetting(String accountName, Settings settings, Setting<SecureString> secureSetting) {
        SecureString secureString = secureSetting.get(settings);
        if (secureString == null || secureString.length() < 1) {
            throw requiredSettingException(accountName, secureSetting.getKey());
        }
        return secureString.toString();
    }

    public String getName() {
        return name;
    }

    public Map<String, Object> getDefaults() {
        return issueDefaults;
    }

    public JiraIssue createIssue(final Map<String, Object> fields, final HttpProxy proxy) throws IOException {
        HttpRequest request = HttpRequest.builder(url.getHost(), url.getPort())
            .scheme(Scheme.parse(url.getScheme()))
            .method(HttpMethod.POST)
            .path(url.getPath().isEmpty() || url.getPath().equals("/") ? DEFAULT_PATH : url.getPath())
            .jsonBody((builder, params) -> builder.field("fields", fields))
            .auth(new BasicAuth(user, password.toCharArray()))
            .proxy(proxy)
            .build();

        HttpResponse response = httpClient.execute(request);
        return JiraIssue.responded(name, fields, request, response);
    }

    private static SettingsException requiredSettingException(String account, String setting) {
        return new SettingsException("invalid jira [" + account + "] account settings. missing required [" + setting + "] setting");
    }
}
