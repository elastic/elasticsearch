/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.test;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.idp.LocalStateIdentityProviderPlugin;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_CONTACT_EMAIL;
import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_CONTACT_GIVEN_NAME;
import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_CONTACT_SURNAME;
import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_ENTITY_ID;
import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_ORGANIZATION_DISPLAY_NAME;
import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_ORGANIZATION_NAME;
import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_ORGANIZATION_URL;
import static org.elasticsearch.xpack.idp.saml.idp.SamlIdentityProviderBuilder.IDP_SSO_REDIRECT_ENDPOINT;
import static org.elasticsearch.xpack.idp.saml.sp.ServiceProviderDefaults.APPLICATION_NAME_SETTING;
import static org.elasticsearch.xpack.idp.saml.sp.ServiceProviderDefaults.NAMEID_FORMAT_SETTING;
import static org.opensaml.saml.saml2.core.NameIDType.TRANSIENT;

public abstract class IdentityProviderIntegTestCase extends ESIntegTestCase {

    // Local Security Cluster user
    public static final String SAMPLE_USER_NAME = "es_user";
    public static final String SAMPLE_USER_PASSWORD = "es_user_password";
    public static final String SAMPLE_USER_PASSWORD_HASHED = new String(
        Hasher.resolve("bcrypt9").hash(new SecureString(SAMPLE_USER_PASSWORD.toCharArray()))
    );
    public static final String SAMPLE_USER_ROLE = "es_user_role";
    // User that is authenticated to the Security Cluster in order to perform SSO to cloud resources
    public static final String SAMPLE_IDPUSER_NAME = "idp_user";
    public static final String SAMPLE_IDPUSER_PASSWORD = "idp_user_password";
    public static final String SAMPLE_IDPUSER_PASSWORD_HASHED = new String(
        Hasher.resolve("bcrypt9").hash(new SecureString(SAMPLE_IDPUSER_PASSWORD.toCharArray()))
    );
    public static final String SAMPLE_IDPUSER_ROLE = "idp_user_role";
    // Cloud console user that calls all IDP related APIs
    public static final String CONSOLE_USER_NAME = "console_user";
    public static final String CONSOLE_USER_PASSWORD = "console_user_password";
    public static final String CONSOLE_USER_PASSWORD_HASHED = new String(
        Hasher.resolve("bcrypt9").hash(new SecureString(CONSOLE_USER_PASSWORD.toCharArray()))
    );
    public static final String CONSOLE_USER_ROLE = "console_user_role";
    public static final String SP_ENTITY_ID = "ec:abcdef:123456";
    public static final RequestOptions REQUEST_OPTIONS_AS_CONSOLE_USER = RequestOptions.DEFAULT.toBuilder()
        .addHeader("Authorization", basicAuthHeaderValue(CONSOLE_USER_NAME, new SecureString(CONSOLE_USER_PASSWORD.toCharArray())))
        .build();
    private static Path PARENT_DIR;

    @BeforeClass
    public static void setup() {
        PARENT_DIR = createTempDir();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Path home = dataPath(PARENT_DIR, nodeOrdinal);
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
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.SECURITY_ENABLED.getKey(), true)
            .put(NetworkModule.TRANSPORT_TYPE_KEY, SecurityField.NAME4)
            .put(NetworkModule.HTTP_TYPE_KEY, SecurityField.NAME4)
            .put("xpack.idp.enabled", true)
            .put(IDP_ENTITY_ID.getKey(), "urn:elastic:cloud:idp")
            .put(IDP_SSO_REDIRECT_ENDPOINT.getKey(), "https://idp.org/sso/redirect")
            .put(IDP_ORGANIZATION_NAME.getKey(), "Identity Provider")
            .put(IDP_ORGANIZATION_DISPLAY_NAME.getKey(), "Identity Provider")
            .put(IDP_ORGANIZATION_URL.getKey(), "https://idp.org")
            .put(IDP_CONTACT_GIVEN_NAME.getKey(), "Tony")
            .put(IDP_CONTACT_SURNAME.getKey(), "Stark")
            .put(IDP_CONTACT_EMAIL.getKey(), "tony@starkindustries.com")
            .put(APPLICATION_NAME_SETTING.getKey(), "elastic-cloud")
            .put(NAMEID_FORMAT_SETTING.getKey(), TRANSIENT)
            .put("xpack.idp.signing.key", resolveResourcePath("/keypair/keypair_RSA_2048.key"))
            .put("xpack.idp.signing.certificate", resolveResourcePath("/keypair/keypair_RSA_2048.crt"))
            .put("xpack.security.authc.realms." + FileRealmSettings.TYPE + ".file.order", 0)
            .put("xpack.security.authc.realms." + NativeRealmSettings.TYPE + ".index.order", "1")
            .put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true)
            .put("xpack.license.self_generated.type", "trial");
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateIdentityProviderPlugin.class, Netty4Plugin.class, CommonAnalysisPlugin.class);
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
        Map<String, String> headers = Collections.singletonMap(
            "Authorization",
            basicAuthHeaderValue(SAMPLE_USER_NAME, new SecureString(SAMPLE_USER_PASSWORD.toCharArray()))
        );
        // we need to wrap node clients because we do not specify a user for nodes and all requests will use the system
        // user. This is ok for internal n2n stuff but the test framework does other things like wiping indices, repositories, etc
        // that the system user cannot do. so we wrap the node client with a user that can do these things since the client() calls
        // return a node client
        return client -> asInstanceOf(NodeClient.class, client).filterWithHeader(headers);
    }

    @Override
    protected Path nodeConfigPath(int nodeOrdinal) {
        return dataPath(PARENT_DIR, nodeOrdinal).resolve("config");
    }

    private String configRoles() {
        // test role allows for everything
        // IDP end user doesn't need any privileges on the security cluster
        // Could switch to grant apikey for user and call this as console_user
        // Console user should be able to call all IDP related endpoints and register application privileges
        return Strings.format("""
            %s:
              cluster: [ ALL ]
              indices:
                - names: '*'
                  allow_restricted_indices: true
                  privileges: [ ALL ]

            %s:
              cluster: ['cluster:admin/xpack/security/api_key/create']
              indices: []
              applications:
                 - application: elastic-cloud
                   resources: [ '%s' ]
                   privileges: [ 'sso:superuser' ]

            %s:
              cluster: ['cluster:admin/idp/*', 'cluster:admin/xpack/security/privilege/*' ]
              indices: []
            """, SAMPLE_USER_ROLE, SAMPLE_IDPUSER_ROLE, SP_ENTITY_ID, CONSOLE_USER_ROLE);
    }

    private String configUsers() {
        return SAMPLE_USER_NAME
            + ":"
            + SAMPLE_USER_PASSWORD_HASHED
            + "\n"
            + SAMPLE_IDPUSER_NAME
            + ":"
            + SAMPLE_IDPUSER_PASSWORD_HASHED
            + "\n"
            + CONSOLE_USER_NAME
            + ":"
            + CONSOLE_USER_PASSWORD_HASHED
            + "\n";
    }

    private String configUsersRoles() {
        return SAMPLE_USER_ROLE
            + ":"
            + SAMPLE_USER_NAME
            + "\n"
            + SAMPLE_IDPUSER_ROLE
            + ":"
            + SAMPLE_IDPUSER_NAME
            + "\n"
            + CONSOLE_USER_ROLE
            + ":"
            + CONSOLE_USER_NAME
            + "\n";
    }

    Path dataPath(Path confDir, final int nodeOrdinal) {
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
        if (clazz == Object.class || clazz == IdentityProviderIntegTestCase.class) {
            return null;
        }
        ClusterScope annotation = clazz.getAnnotation(ClusterScope.class);
        if (annotation != null) {
            return annotation;
        }
        return getAnnotation(clazz.getSuperclass());
    }

    private static String writeFile(Path folder, String name, byte[] content) {
        final Path path = folder.resolve(name);
        Path tempFile = null;
        try {
            tempFile = Files.createTempFile(path.getParent(), path.getFileName().toString(), "tmp");
            try (OutputStream os = Files.newOutputStream(tempFile, CREATE, TRUNCATE_EXISTING, WRITE)) {
                Streams.copy(content, os);
            }

            try {
                Files.move(tempFile, path, REPLACE_EXISTING, ATOMIC_MOVE);
            } catch (final AtomicMoveNotSupportedException e) {
                Files.move(tempFile, path, REPLACE_EXISTING);
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(Strings.format("could not write file [%s]", path.toAbsolutePath()), e);
        } finally {
            // we are ignoring exceptions here, so we do not need handle whether or not tempFile was initialized nor if the file exists
            IOUtils.deleteFilesIgnoringExceptions(tempFile);
        }
        return path.toAbsolutePath().toString();
    }

    private static String writeFile(Path folder, String name, String content) {
        return writeFile(folder, name, content.getBytes(StandardCharsets.UTF_8));
    }

    private Path resolveResourcePath(String resourcePathToFile) {
        try {
            Path path = createTempFile();
            try (InputStream resourceInput = IdentityProviderIntegTestCase.class.getResourceAsStream(resourcePathToFile)) {
                Files.copy(resourceInput, path, StandardCopyOption.REPLACE_EXISTING);
            }
            return path;
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to resolve resource (Path=[{}])", e, resourcePathToFile);
        }
    }
}
