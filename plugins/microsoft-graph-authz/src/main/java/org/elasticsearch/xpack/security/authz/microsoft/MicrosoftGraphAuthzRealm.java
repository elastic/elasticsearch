/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.security.authz.microsoft;

import com.nimbusds.jose.util.JSONObjectUtils;

import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.nimbusds.jose.util.JSONObjectUtils.getJSONObjectArray;
import static com.nimbusds.jose.util.JSONObjectUtils.getString;

public class MicrosoftGraphAuthzRealm extends Realm {

    private static final Logger logger = LogManager.getLogger(MicrosoftGraphAuthzRealm.class);

    private final HttpClient httpClient;
    private final RealmConfig config;
    private final UserRoleMapper roleMapper;
    private final SecureString clientSecret;

    public MicrosoftGraphAuthzRealm(UserRoleMapper roleMapper, RealmConfig config) {
        super(config);

        this.roleMapper = roleMapper;
        this.config = config;
        this.httpClient = HttpClients.createDefault();
        this.clientSecret = config.getSetting(MicrosoftGraphAuthzRealmSettings.CLIENT_SECRET);
    }

    @Override
    public boolean supports(AuthenticationToken token) {
        return false;
    }

    @Override
    public AuthenticationToken token(ThreadContext context) {
        return null;
    }

    @Override
    public void authenticate(AuthenticationToken token, ActionListener<AuthenticationResult<User>> listener) {
        listener.onResponse(AuthenticationResult.notHandled());
    }

    @Override
    public void lookupUser(String principal, ActionListener<User> listener) {
        try {
            final var token = fetchAccessToken();
            final var userProperties = fetchUserProperties(principal, token);
            final var groups = fetchGroupMembership(principal, token);

            final var userData = new UserRoleMapper.UserData(principal, null, groups, Map.of(), config);

            roleMapper.resolveRoles(userData, listener.delegateFailureAndWrap((l, roles) -> {
                final var user = new User(
                    principal,
                    roles.toArray(Strings.EMPTY_ARRAY),
                    userProperties.v1(),
                    userProperties.v2(),
                    Map.of(),
                    true
                );
                logger.debug("Entra ID user {}", user);
                l.onResponse(user);
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private String fetchAccessToken() throws IOException, ParseException {
        var request = new HttpPost(
            Strings.format(
                "%s/%s/oauth2/v2.0/token",
                config.getSetting(MicrosoftGraphAuthzRealmSettings.ACCESS_TOKEN_HOST),
                config.getSetting(MicrosoftGraphAuthzRealmSettings.TENANT_ID)
            )
        );
        request.setEntity(
            new UrlEncodedFormEntity(
                List.of(
                    new BasicNameValuePair("grant_type", "client_credentials"),
                    new BasicNameValuePair("scope", "https://graph.microsoft.com/.default"),
                    new BasicNameValuePair("client_id", config.getSetting(MicrosoftGraphAuthzRealmSettings.CLIENT_ID)),
                    new BasicNameValuePair("client_secret", clientSecret.toString())
                )
            )
        );
        logger.trace("getting bearer token from {}", request.getURI());
        final var response = httpClient.execute(request, new BasicResponseHandler());

        final var json = JSONObjectUtils.parse(response);
        final var token = getString(json, "access_token");
        logger.trace("Azure access token [{}]", token);

        return token;
    }

    private Tuple<String, String> fetchUserProperties(String userId, String token) throws IOException, ParseException {
        var request = new HttpGet(
            Strings.format(
                "%s/v1.0/users/%s?$select=displayName,mail",
                config.getSetting(MicrosoftGraphAuthzRealmSettings.API_HOST),
                userId
            )
        );
        request.addHeader("Authorization", "Bearer " + token);
        logger.trace("getting user info from {}", request.getURI());
        final var response = httpClient.execute(request, new BasicResponseHandler());

        final var json = JSONObjectUtils.parse(response);
        final var email = getString(json, "email");
        final var name = getString(json, "displayName");

        logger.trace("User [{}] has email [{}]", name, email);

        return Tuple.tuple(name, email);
    }

    private List<String> fetchGroupMembership(String userId, String token) throws IOException, ParseException, URISyntaxException {
        var request = new HttpGet();
        request.addHeader("Authorization", "Bearer " + token);

        var nextPage = Strings.format(
            "%s/v1.0/users/%s/memberOf/microsoft.graph.group?$select=id&$top=999",
            config.getSetting(MicrosoftGraphAuthzRealmSettings.API_HOST),
            userId
        );
        var groups = new ArrayList<String>();

        while (nextPage != null) {
            request.setURI(new URI(nextPage));
            logger.trace("getting group membership from {}", request.getURI());
            final var response = httpClient.execute(request, new BasicResponseHandler());

            var json = JSONObjectUtils.parse(response);
            nextPage = getString(json, "@odata.nextLink");
            for (var groupData : getJSONObjectArray(json, "groups")) {
                groups.add(getString(groupData, "id"));
            }
        }

        logger.trace("Got {} groups from Graph {}", groups.size(), String.join(", ", groups));

        return groups;
    }

}
