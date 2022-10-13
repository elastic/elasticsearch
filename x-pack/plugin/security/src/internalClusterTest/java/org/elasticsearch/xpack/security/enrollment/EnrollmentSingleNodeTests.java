/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.xpack.core.security.EnrollmentToken;
import org.elasticsearch.xpack.core.security.action.enrollment.KibanaEnrollmentAction;
import org.elasticsearch.xpack.core.security.action.enrollment.KibanaEnrollmentRequest;
import org.elasticsearch.xpack.core.security.action.enrollment.KibanaEnrollmentResponse;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.SecuritySettingsSource.addSSLSettingsForStore;
import static org.elasticsearch.xpack.core.XPackSettings.ENROLLMENT_ENABLED;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.spy;

public class EnrollmentSingleNodeTests extends SecuritySingleNodeTestCase {

    @BeforeClass
    public static void muteInFips() {
        assumeFalse("Enrollment is not supported in FIPS 140-2 as we are using PKCS#12 keystores", inFipsJvm());
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings());
        addSSLSettingsForStore(
            builder,
            "xpack.security.http.",
            "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/httpCa.p12",
            "password",
            false
        );
        builder.put("xpack.security.http.ssl.enabled", true).put(ENROLLMENT_ENABLED.getKey(), "true");
        // Need at least 2 threads because enrollment token creator internally uses a client
        builder.put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), 2);
        return builder.build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected boolean transportSSLEnabled() {
        return true;
    }

    public void testKibanaEnrollmentTokenCreation() throws Exception {
        final SSLService sslService = getInstanceFromNode(SSLService.class);

        final InternalEnrollmentTokenGenerator internalEnrollmentTokenGenerator = spy(
            new InternalEnrollmentTokenGenerator(
                newEnvironment(Settings.builder().put(ENROLLMENT_ENABLED.getKey(), "true").build()),
                sslService,
                node().client()
            )
        );
        // Mock the getHttpsCaFingerprint method because the real method requires createClassLoader permission
        Mockito.doReturn("fingerprint").when(internalEnrollmentTokenGenerator).getHttpsCaFingerprint();

        final SetOnce<EnrollmentToken> enrollmentTokenSetOnce = new SetOnce<>();
        final CountDownLatch latch = new CountDownLatch(1);

        // Create the kibana enrollment token and wait for the process to complete
        internalEnrollmentTokenGenerator.createKibanaEnrollmentToken(enrollmentToken -> {
            enrollmentTokenSetOnce.set(enrollmentToken);
            latch.countDown();
        }, List.of(TimeValue.timeValueMillis(500)).iterator());
        latch.await(20, TimeUnit.SECONDS);

        // The API key is created by the right user and should work
        final Client apiKeyClient = client().filterWithHeader(
            Map.of(
                "Authorization",
                "ApiKey " + Base64.getEncoder().encodeToString(enrollmentTokenSetOnce.get().getApiKey().getBytes(StandardCharsets.UTF_8))
            )
        );
        final AuthenticateResponse authenticateResponse1 = apiKeyClient.execute(AuthenticateAction.INSTANCE, AuthenticateRequest.INSTANCE)
            .actionGet();
        assertThat(authenticateResponse1.authentication().getUser().principal(), equalTo("_xpack_security"));

        final KibanaEnrollmentResponse kibanaEnrollmentResponse = apiKeyClient.execute(
            KibanaEnrollmentAction.INSTANCE,
            new KibanaEnrollmentRequest()
        ).actionGet();

        // The service token should work
        final Client kibanaClient = client().filterWithHeader(
            Map.of("Authorization", "Bearer " + kibanaEnrollmentResponse.getTokenValue())
        );

        final AuthenticateResponse authenticateResponse2 = kibanaClient.execute(AuthenticateAction.INSTANCE, AuthenticateRequest.INSTANCE)
            .actionGet();
        assertThat(authenticateResponse2.authentication().getUser().principal(), equalTo("elastic/kibana"));
    }
}
