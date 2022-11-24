/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.junit.BeforeClass;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.security.transport.RcsClusterCredentials.RCS_CLUSTER_CREDENTIAL_HEADER_KEY;
import static org.elasticsearch.xpack.security.transport.RcsClusterCredentials.RCS_SUPPORTED_AUTHENTICATION_SCHEMES;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RcsClusterCredentialsTests extends ESTestCase {
    final List<String> UNSUPPORTED_SCHEMES = List.of(UsernamePasswordToken.BASIC_AUTH_HEADER, "Bearer");

    private ThreadPool threadPool;
    private ClusterService clusterService;

    @BeforeClass
    public static void checkFeatureFlag() {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.threadPool = new TestThreadPool(getTestName());
        this.clusterService = ClusterServiceUtils.createClusterService(this.threadPool);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        this.clusterService.close();
        terminate(this.threadPool);
    }

    public void testRemoteClusterSupportedSchemes() {
        assertThat(RCS_SUPPORTED_AUTHENTICATION_SCHEMES, is(equalTo(List.of("ApiKey"))));
    }

    public void testRemoteClusterHeaderName() {
        assertThat(RCS_CLUSTER_CREDENTIAL_HEADER_KEY, is(equalTo("_remote_access_cluster_credential")));
    }

    public void testRemoteClusterConstructor() {
        final String apiKeyId = randomAlphaOfLength(10);
        final String apiKeySecret = randomAlphaOfLength(44);
        final String base64Credentials = Base64.getEncoder()
            .encodeToString((apiKeyId + ":" + apiKeySecret).getBytes(StandardCharsets.UTF_8));
        final String schemeAndBase64Credentials = ApiKeyService.API_KEY_HEADER + base64Credentials;
        final SecureString secureSchemeAndBase64Credentials = new SecureString(schemeAndBase64Credentials.toCharArray());
        final RcsClusterCredentials rcsClusterCredentials = new RcsClusterCredentials("ApiKey", secureSchemeAndBase64Credentials);
        assertThat(rcsClusterCredentials.scheme(), is(equalTo("ApiKey")));
        assertThat(rcsClusterCredentials.value(), is(equalTo(secureSchemeAndBase64Credentials)));
    }

    public void testRemoteClusterEncodeDecode() {
        final String apiKeyId = randomAlphaOfLength(10);
        final String apiKeySecret = randomAlphaOfLength(44);
        final String base64Credentials = Base64.getEncoder()
            .encodeToString((apiKeyId + ":" + apiKeySecret).getBytes(StandardCharsets.UTF_8));
        final String schemeAndBase64Credentials = ApiKeyService.API_KEY_HEADER + base64Credentials;
        final SecureString secureSchemeAndBase64Credentials = new SecureString(schemeAndBase64Credentials.toCharArray());

        // Decode SecureString into RcsClusterCredentials(String, SecureString), verify re-encode works
        final RcsClusterCredentials decoded = RcsClusterCredentials.decode(secureSchemeAndBase64Credentials);
        assertThat(decoded.scheme(), is(equalTo(ApiKeyService.API_KEY_SCHEME)));
        assertThat(decoded.value(), is(equalTo(base64Credentials)));
        final SecureString encoded = RcsClusterCredentials.encode(decoded);
        assertThat(encoded, is(equalTo(secureSchemeAndBase64Credentials)));

        // Encode SecureString to ThreadContext, verify decode works
        final ThreadContext threadContext1 = new ThreadContext(Settings.EMPTY);
        RcsClusterCredentials.writeToContextHeader(threadContext1, secureSchemeAndBase64Credentials);
        final RcsClusterCredentials read1 = RcsClusterCredentials.readFromContextHeader(threadContext1);
        assertThat(read1, is(equalTo(decoded)));

        // Encode RcsClusterCredentials to ThreadContext, verify decode works
        final ThreadContext threadContext2 = new ThreadContext(Settings.EMPTY);
        RcsClusterCredentials.writeToContextHeader(threadContext2, decoded);
        final RcsClusterCredentials read2 = RcsClusterCredentials.readFromContextHeader(threadContext2);
        assertThat(read2, is(equalTo(decoded)));
    }

    public void testRemoteClusterOverrides() {
        final String scheme1 = "ApiKey";
        final String value1 = randomAlphaOfLength(10);
        // different instance, backed by different char[]
        final String scheme2 = (scheme1 + randomAlphaOfLength(1)).substring(0, scheme1.length());
        final String value2 = (value1 + randomAlphaOfLength(1)).substring(0, value1.length());

        final RcsClusterCredentials rcsClusterCredentials1 = new RcsClusterCredentials(scheme1, new SecureString(value1.toCharArray()));
        final RcsClusterCredentials rcsClusterCredentials2 = new RcsClusterCredentials(scheme2, new SecureString(value2.toCharArray()));
        assertThat(rcsClusterCredentials1, is(equalTo(rcsClusterCredentials2)));
        assertThat(rcsClusterCredentials1.hashCode(), is(equalTo(rcsClusterCredentials2.hashCode())));
        assertThat(rcsClusterCredentials1.toString(), is(equalTo(rcsClusterCredentials2.toString())));
        assertThat(rcsClusterCredentials1.toString(), is(equalTo("ApiKey REDACTED")));
    }

    public void testRemoteClusterConstructorValidationExceptions() {
        for (final String emptyScheme : new String[] { null, "" }) {
            final IllegalArgumentException emptySchemeException = expectThrows(
                IllegalArgumentException.class,
                () -> new RcsClusterCredentials(emptyScheme, new SecureString(randomAlphaOfLength(8).toCharArray()))
            );
            assertThat(emptySchemeException.getMessage(), is(equalTo("Empty scheme not supported")));
        }

        for (final SecureString emptyValue : new SecureString[] { null, new SecureString("".toCharArray()) }) {
            for (final String supportedScheme : RCS_SUPPORTED_AUTHENTICATION_SCHEMES) {
                final IllegalArgumentException emptyValueException = expectThrows(
                    IllegalArgumentException.class,
                    () -> new RcsClusterCredentials(supportedScheme, emptyValue)
                );
                assertThat(emptyValueException.getMessage(), is(equalTo("Empty value not supported")));
            }
        }

        for (final String unsupportedScheme : UNSUPPORTED_SCHEMES) {
            final IllegalArgumentException emptyValueException = expectThrows(
                IllegalArgumentException.class,
                () -> new RcsClusterCredentials(unsupportedScheme, new SecureString(randomAlphaOfLength(1).toCharArray()))
            );
            assertThat(
                emptyValueException.getMessage(),
                is(equalTo("Unsupported scheme [" + unsupportedScheme + "], supported schemes are " + RCS_SUPPORTED_AUTHENTICATION_SCHEMES))
            );
        }
    }

    public void testRemoteClusterDecodeValidationExceptions() {
        for (final CharSequence emptyValue : new String[] { null, "" }) {
            final IllegalArgumentException emptyValueException = expectThrows(
                IllegalArgumentException.class,
                () -> RcsClusterCredentials.decode(emptyValue)
            );
            assertThat(emptyValueException.getMessage(), is(equalTo("Empty value not supported")));
        }

        for (final CharSequence supportedScheme : RCS_SUPPORTED_AUTHENTICATION_SCHEMES) {
            final IllegalArgumentException missingSpaceAndValueAfterScheme = expectThrows(
                IllegalArgumentException.class,
                () -> RcsClusterCredentials.decode(supportedScheme)
            );
            assertThat(
                missingSpaceAndValueAfterScheme.getMessage(),
                is(equalTo("Missing space and value after scheme [" + supportedScheme + "]"))
            );

            final IllegalArgumentException missingSpaceAfterScheme = expectThrows(
                IllegalArgumentException.class,
                () -> RcsClusterCredentials.decode(supportedScheme + "_")
            );
            assertThat(missingSpaceAfterScheme.getMessage(), is(equalTo("Missing space after scheme [" + supportedScheme + "]")));

            final IllegalArgumentException missingValueAfterSchemeAndSpace = expectThrows(
                IllegalArgumentException.class,
                () -> RcsClusterCredentials.decode(supportedScheme + " ")
            );
            assertThat(
                missingValueAfterSchemeAndSpace.getMessage(),
                is(equalTo("Missing value after scheme [" + supportedScheme + "] and space"))
            );
        }

        for (final String unsupportedScheme : UNSUPPORTED_SCHEMES) {
            final String basicUsername = randomAlphaOfLength(8);
            final String basicPassword = randomAlphaOfLength(44);
            final String base64Credentials = Base64.getEncoder()
                .encodeToString((basicUsername + ":" + basicPassword).getBytes(StandardCharsets.UTF_8));
            final String schemeAndBase64Credentials = unsupportedScheme + base64Credentials;
            final SecureString secureSchemeAndBase64Credentials = new SecureString(schemeAndBase64Credentials.toCharArray());
            for (final CharSequence badVal : Set.of(
                unsupportedScheme,
                unsupportedScheme + "_",
                unsupportedScheme + " ",
                schemeAndBase64Credentials,
                secureSchemeAndBase64Credentials
            )) {
                final IllegalArgumentException invalidSchemeException = expectThrows(
                    IllegalArgumentException.class,
                    () -> RcsClusterCredentials.decode(badVal)
                );
                assertThat(
                    invalidSchemeException.getMessage(),
                    is(equalTo("No supported scheme found, supported schemes are " + RCS_SUPPORTED_AUTHENTICATION_SCHEMES))
                );
            }
        }
    }
}
