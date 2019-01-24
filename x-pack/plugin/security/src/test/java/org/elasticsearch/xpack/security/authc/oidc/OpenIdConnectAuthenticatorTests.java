package org.elasticsearch.xpack.security.authc.oidc;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.oauth2.sdk.ResponseType;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.id.Issuer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings;
import org.junit.Before;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.hamcrest.Matchers.containsString;

public class OpenIdConnectAuthenticatorTests extends ESTestCase {

    private OpenIdConnectAuthenticator authenticator;
    private static String REALM_NAME = "oidc-realm";
    private Settings globalSettings;
    private Environment env;
    private ThreadContext threadContext;

    @Before
    public void setup() throws Exception {
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        env = TestEnvironment.newEnvironment(globalSettings);
        threadContext = new ThreadContext(globalSettings);
        authenticator = buildAuthenticator();
    }


    private OpenIdConnectAuthenticator buildAuthenticator() throws MalformedURLException, URISyntaxException {
        final RealmConfig config = buildConfig(getBasicRealmSettings().build());
        return new OpenIdConnectAuthenticator(config, getOpConfig(), getRpConfig(), null);
    }

    public void testEmptyRedirectUrlIsRejected() {
        OpenIdConnectToken token = new OpenIdConnectToken(null, randomAlphaOfLength(8), randomAlphaOfLength(8));
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> {
            authenticator.authenticate(token);
        });
        assertThat(e.getMessage(), containsString("Failed to consume the OpenID connect response"));
    }

    public void testInvalidStateIsRejected() {
        final String code = randomAlphaOfLengthBetween(8, 12);
        final String state = randomAlphaOfLengthBetween(8, 12);
        final String invalidState = state.concat(randomAlphaOfLength(2));
        final String redirectUrl = "https://rp.elastic.co/cb?code=" + code + "&state=" + state;
        OpenIdConnectToken token = new OpenIdConnectToken(redirectUrl, invalidState, randomAlphaOfLength(10));
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> {
            authenticator.authenticate(token);
        });
        assertThat(e.getMessage(), containsString("Received a response with an invalid state parameter"));
    }

    public void testInvalidNonceIsRejected() {
        //TODO
    }

    private Settings.Builder getBasicRealmSettings() {
        return Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.org/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.org/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_NAME), "the op")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_URL), "https://op.example.org/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.elastic.co/cb")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), randomFrom("code", "id_token"))
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.GROUPS_CLAIM.getClaim()), "groups")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.MAIL_CLAIM.getClaim()), "mail")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.NAME_CLAIM.getClaim()), "name");
    }

    private OpenIdConnectProviderConfiguration getOpConfig() throws MalformedURLException, URISyntaxException {
        return new OpenIdConnectProviderConfiguration("op_name",
            new Issuer("https://op.example.com"),
            new URL("https://op.example.org/jwks.json"),
            new URI("https://op.example.org/login"),
            new URI("https://op.example.org/token"),
            new URI("https://op.example.org/userinfo"));
    }

    private RelyingPartyConfiguration getRpConfig() throws URISyntaxException {
        return new RelyingPartyConfiguration(
            new ClientID("rp-my"),
            new SecureString("mysecret".toCharArray()),
            new URI("https://rp.elastic.co/cb"),
            new ResponseType("code"),
            new Scope("openid"),
            JWSAlgorithm.RS384);
    }

    private RealmConfig buildConfig(Settings realmSettings) {
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put(realmSettings).build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        return new RealmConfig(new RealmConfig.RealmIdentifier("oidc", REALM_NAME), settings, env, threadContext);
    }
}
