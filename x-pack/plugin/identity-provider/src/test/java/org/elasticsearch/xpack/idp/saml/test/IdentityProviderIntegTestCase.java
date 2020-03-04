/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.test;

import io.netty.util.ThreadDeathWatcher;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.idp.IdentityProviderPlugin;
import org.elasticsearch.xpack.idp.LocalStateIdentityProvider;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_CONTACT_EMAIL;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_CONTACT_GIVEN_NAME;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_CONTACT_SURNAME;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_ENTITY_ID;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_ORGANIZATION_DISPLAY_NAME;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_ORGANIZATION_NAME;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_ORGANIZATION_URL;
import static org.elasticsearch.xpack.idp.IdentityProviderPlugin.IDP_SSO_REDIRECT_ENDPOINT;
import static org.elasticsearch.xpack.security.test.SecurityTestUtils.writeFile;

public class IdentityProviderIntegTestCase extends ESIntegTestCase {

    public static final String TEST_USER_NAME = "idp_user";
    public static final String TEST_PASSWORD = "idp_user_password";
    public static final String TEST_PASSWORD_HASHED =
        new String(Hasher.resolve("bcrypt9").hash(new SecureString(TEST_PASSWORD.toCharArray())));
    public static final String TEST_ROLE = "idp_user_role";
    public static final String TEST_SUPERUSER = "test_superuser";
    public static final RequestOptions IDP_REQUEST_WITH_SECONDARY_AUTH_OPTIONS = RequestOptions.DEFAULT.toBuilder()
        .addHeader("Authorization", basicAuthHeaderValue(TEST_SUPERUSER,
            new SecureString(TEST_PASSWORD.toCharArray())))
        .addHeader("es-secondary-authorization", basicAuthHeaderValue(TEST_USER_NAME,
            new SecureString(TEST_PASSWORD.toCharArray())))
        .build();
    public static final RequestOptions IDP_REQUEST_OPTIONS = RequestOptions.DEFAULT.toBuilder()
        .addHeader("Authorization", basicAuthHeaderValue(TEST_SUPERUSER,
            new SecureString(TEST_PASSWORD.toCharArray())))
        .build();
    private static Path PARENT_DIR;

    @BeforeClass
    public static void setup(){
        PARENT_DIR = createTempDir();
    }

    /**
     * A JUnit class level rule that runs after the AfterClass method in {@link ESIntegTestCase},
     * which stops the cluster. After the cluster is stopped, there are a few netty threads that
     * can linger, so we wait for them to finish otherwise these lingering threads can intermittently
     * trigger the thread leak detector
     */
    @ClassRule
    public static final ExternalResource STOP_NETTY_RESOURCE = new ExternalResource() {
        @Override
        protected void after() {
            try {
                GlobalEventExecutor.INSTANCE.awaitInactivity(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IllegalStateException e) {
                if (e.getMessage().equals("thread was not started") == false) {
                    throw e;
                }
                // ignore since the thread was never started
            }

            try {
                ThreadDeathWatcher.awaitInactivity(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    };

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final Path home = nodePath(PARENT_DIR, nodeOrdinal);
        final Path xpackConf = home.resolve("config");
        try {
            Files.createDirectories(xpackConf);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        writeFile(xpackConf, "roles.yml", configRoles());
        writeFile(xpackConf, "users", configUsers());
        writeFile(xpackConf, "users_roles", configUsersRoles());
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(XPackSettings.SECURITY_ENABLED.getKey(), true)
            .put(NetworkModule.TRANSPORT_TYPE_KEY, randomBoolean() ? SecurityField.NAME4 : SecurityField.NIO)
            .put(NetworkModule.HTTP_TYPE_KEY, randomBoolean() ? SecurityField.NAME4 : SecurityField.NIO)
            .put("xpack.idp.enabled", true)
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/sso/redirect")
            .put(IDP_ORGANIZATION_NAME.getKey(), "Identity Provider")
            .put(IDP_ORGANIZATION_DISPLAY_NAME.getKey(), "Identity Provider")
            .put(IDP_ORGANIZATION_URL.getKey(), "https://idp.org")
            .put(IDP_CONTACT_GIVEN_NAME.getKey(), "Tony")
            .put(IDP_CONTACT_SURNAME.getKey(), "Stark")
            .put(IDP_CONTACT_EMAIL.getKey(), "tony@starkindustries.com")
            .put("xpack.security.authc.realms." + FileRealmSettings.TYPE + ".file.order", 0)
            .put("xpack.security.authc.realms." + NativeRealmSettings.TYPE + ".index.order", "1")
            .put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true)
            .put("xpack.license.self_generated.type", "trial");
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateIdentityProvider.class, IdentityProviderPlugin.class, Netty4Plugin.class, CommonAnalysisPlugin.class);
    }

    @Override
    protected boolean addMockTransportService() {
        return false; // security has its own transport service
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Function<Client, Client> getClientWrapper() {
        Map<String, String> headers = Collections.singletonMap("Authorization",
            basicAuthHeaderValue(TEST_USER_NAME, new SecureString(TEST_PASSWORD.toCharArray())));
        // we need to wrap node clients because we do not specify a user for nodes and all requests will use the system
        // user. This is ok for internal n2n stuff but the test framework does other things like wiping indices, repositories, etc
        // that the system user cannot do. so we wrap the node client with a user that can do these things since the client() calls
        // return a node client
        return client -> (client instanceof NodeClient) ? client.filterWithHeader(headers) : client;
    }

    @Override
    protected Path nodeConfigPath(int nodeOrdinal) {
        return nodePath(PARENT_DIR, nodeOrdinal).resolve("config");
    }

    private String configRoles() {
        // test role allows for everything
        return TEST_ROLE + ":\n" +
            "  cluster: [ ALL ]\n" +
            "  indices:\n" +
            "    - names: '*'\n" +
            "      allow_restricted_indices: true\n" +
            "      privileges: [ ALL ]\n";
    }

    private String configUsers() {
        return TEST_USER_NAME + ":" + TEST_PASSWORD_HASHED + "\n" +
            TEST_SUPERUSER + ":" + TEST_PASSWORD_HASHED + "\n";
    }

    private String configUsersRoles() {
        return TEST_ROLE + ":" + TEST_USER_NAME + "\n" +
            "superuser:" + TEST_SUPERUSER + "\n";
    }

    Path nodePath(Path confDir, final int nodeOrdinal) {
        return confDir.resolve(getCurrentClusterScope() + "-" + nodeOrdinal);
    }

    protected Scope getCurrentClusterScope() {
        return getCurrentClusterScope(this.getClass());
    }

    private static Scope getCurrentClusterScope(Class<?> clazz) {
        ClusterScope annotation = getAnnotation(clazz);
        return annotation == null ? Scope.SUITE : annotation.scope();
    }

    private static ClusterScope getAnnotation(Class<?> clazz) {
        if (clazz == Object.class || clazz == SecurityIntegTestCase.class) {
            return null;
        }
        ClusterScope annotation = clazz.getAnnotation(ClusterScope.class);
        if (annotation != null) {
            return annotation;
        }
        return getAnnotation(clazz.getSuperclass());
    }
}
