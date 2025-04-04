/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.s3;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.retry.RetryPolicyContext;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.endpoints.S3EndpointParams;
import software.amazon.awssdk.services.s3.endpoints.internal.DefaultS3EndpointProvider;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.mock;

public class S3ServiceTests extends ESTestCase {

    public void testCachedClientsAreReleased() throws IOException {
        final S3Service s3Service = new S3Service(
            mock(Environment.class),
            Settings.EMPTY,
            mock(ResourceWatcherService.class),
            () -> Region.of("es-test-region")
        );
        s3Service.start();
        final String endpointOverride = "http://first";
        final Settings settings = Settings.builder().put("endpoint", endpointOverride).build();
        final RepositoryMetadata metadata1 = new RepositoryMetadata("first", "s3", settings);
        final RepositoryMetadata metadata2 = new RepositoryMetadata("second", "s3", settings);
        final S3ClientSettings clientSettings = s3Service.settings(metadata2);
        final S3ClientSettings otherClientSettings = s3Service.settings(metadata2);
        assertSame(clientSettings, otherClientSettings);
        final AmazonS3Reference reference = s3Service.client(metadata1);

        // TODO NOMERGE: move to its own test.
        assertEquals(endpointOverride, reference.client().serviceClientConfiguration().endpointOverride().get().toString());
        assertEquals("es-test-region", reference.client().serviceClientConfiguration().region().toString());

        reference.close();
        s3Service.doClose();
        final AmazonS3Reference referenceReloaded = s3Service.client(metadata1);
        assertNotSame(referenceReloaded, reference);
        referenceReloaded.close();
        s3Service.doClose();
        final S3ClientSettings clientSettingsReloaded = s3Service.settings(metadata1);
        assertNotSame(clientSettings, clientSettingsReloaded);
        s3Service.close();
    }

    public void testRetryOn403RetryPolicy() {
        AwsErrorDetails awsErrorDetails = AwsErrorDetails.builder().errorCode("InvalidAccessKeyId").build();
        AwsServiceException s3Exception = S3Exception.builder()
            .awsErrorDetails(awsErrorDetails)
            .statusCode(RestStatus.FORBIDDEN.getStatus())
            .build();

        // AWS default retry condition does not retry on 403
        assertFalse(
            RetryCondition.defaultRetryCondition()
                .shouldRetry(RetryPolicyContext.builder().retriesAttempted(between(0, 9)).exception(s3Exception).build())
        );

        // The retryable 403 condition retries on 403 invalid access key id
        assertTrue(
            S3Service.RETRYABLE_403_RETRY_PREDICATE(
                RetryPolicyContext.builder().retriesAttempted(between(0, 9)).exception(s3Exception).build().exception()
            )
        );

        if (randomBoolean()) {
            // Random for another error status that is not 403
            var non403StatusCode = randomValueOtherThan(403, () -> between(0, 600));
            var non403Exception = S3Exception.builder().statusCode(non403StatusCode).awsErrorDetails(awsErrorDetails).build();
            var retryPolicyContext = RetryPolicyContext.builder().retriesAttempted(between(0, 9)).exception(non403Exception).build();
            // Retryable 403 condition delegates to the AWS default retry condition. Its result must be consistent with the decision
            // by the AWS default, e.g. some error status like 429 is retryable by default, the retryable 403 condition respects it.
            boolean actual = S3Service.RETRYABLE_403_RETRY_PREDICATE(retryPolicyContext.exception());
            boolean expected = RetryCondition.defaultRetryCondition().shouldRetry(retryPolicyContext);
            assertThat(actual, equalTo(expected));
        } else {
            // Not retry for 403 with error code that is not invalid access key id
            String errorCode = randomAlphaOfLength(10);
            var exception = S3Exception.builder().awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).build()).build();
            var retryPolicyContext = RetryPolicyContext.builder().retriesAttempted(between(0, 9)).exception(exception).build();
            assertFalse(S3Service.RETRYABLE_403_RETRY_PREDICATE(retryPolicyContext.exception()));
        }
    }

    @TestLogging(reason = "testing WARN log output", value = "org.elasticsearch.repositories.s3.S3Service:WARN")
    public void testGetClientRegionFromSetting() {
        final var regionRequested = new AtomicBoolean();
        try (var s3Service = new S3Service(mock(Environment.class), Settings.EMPTY, mock(ResourceWatcherService.class), () -> {
            assertTrue(regionRequested.compareAndSet(false, true));
            return randomFrom(randomFrom(Region.regions()), Region.of(randomIdentifier()), null);
        })) {
            s3Service.start();
            assertTrue(regionRequested.get());

            final var clientName = randomBoolean() ? "default" : randomIdentifier();

            final var region = randomBoolean() ? randomFrom(Region.regions()) : Region.of(randomIdentifier());
            MockLog.assertThatLogger(
                () -> assertSame(
                    region,
                    s3Service.getClientRegion(
                        S3ClientSettings.getClientSettings(
                            Settings.builder().put("s3.client." + clientName + ".region", region.id()).build(),
                            clientName
                        )
                    )
                ),
                S3Service.class,
                new MockLog.UnseenEventExpectation("no warning", S3Service.class.getCanonicalName(), Level.WARN, "*"),
                new MockLog.UnseenEventExpectation("no debug", S3Service.class.getCanonicalName(), Level.DEBUG, "*")
            );
        }
    }

    @TestLogging(reason = "testing WARN log output", value = "org.elasticsearch.repositories.s3.S3Service:WARN")
    public void testGetClientRegionFromEndpointSettingGuess() {
        final var regionRequested = new AtomicBoolean();
        try (var s3Service = new S3Service(mock(Environment.class), Settings.EMPTY, mock(ResourceWatcherService.class), () -> {
            assertTrue(regionRequested.compareAndSet(false, true));
            return randomFrom(randomFrom(Region.regions()), Region.of(randomIdentifier()), null);
        })) {
            s3Service.start();
            assertTrue(regionRequested.get());

            final var clientName = randomBoolean() ? "default" : randomIdentifier();

            final var guessedRegion = randomValueOtherThanMany(
                r -> r.isGlobalRegion() || r.id().contains("-gov-"),
                () -> randomFrom(Region.regions())
            );
            final var endpointUrl = safeGet(
                new DefaultS3EndpointProvider().resolveEndpoint(S3EndpointParams.builder().region(guessedRegion).build())
            ).url();
            final var endpoint = randomFrom(endpointUrl.toString(), endpointUrl.getHost());

            MockLog.assertThatLogger(
                () -> assertEquals(
                    endpoint,
                    guessedRegion,
                    s3Service.getClientRegion(
                        S3ClientSettings.getClientSettings(
                            Settings.builder().put("s3.client." + clientName + ".endpoint", endpoint).build(),
                            clientName
                        )
                    )
                ),
                S3Service.class,
                new MockLog.SeenEventExpectation(
                    endpoint + " -> " + guessedRegion,
                    S3Service.class.getCanonicalName(),
                    Level.WARN,
                    Strings.format(
                        """
                            found S3 client with endpoint [%s] but no configured region, guessing it should use [%s]; \
                            to suppress this warning, configure the [s3.client.CLIENT_NAME.region] setting on this node""",
                        endpoint,
                        guessedRegion.id()
                    )
                )
            );
        }
    }

    @TestLogging(reason = "testing DEBUG log output", value = "org.elasticsearch.repositories.s3.S3Service:DEBUG")
    public void testGetClientRegionFromDefault() {
        final var regionRequested = new AtomicBoolean();
        final var defaultRegion = randomBoolean() ? randomFrom(Region.regions()) : Region.of(randomIdentifier());
        try (var s3Service = new S3Service(mock(Environment.class), Settings.EMPTY, mock(ResourceWatcherService.class), () -> {
            assertTrue(regionRequested.compareAndSet(false, true));
            return defaultRegion;
        })) {
            s3Service.start();
            assertTrue(regionRequested.get());

            final var clientName = randomBoolean() ? "default" : randomIdentifier();

            MockLog.assertThatLogger(
                () -> assertSame(defaultRegion, s3Service.getClientRegion(S3ClientSettings.getClientSettings(Settings.EMPTY, clientName))),
                S3Service.class,
                new MockLog.SeenEventExpectation(
                    "warning",
                    S3Service.class.getCanonicalName(),
                    Level.DEBUG,
                    "found S3 client with no configured region, using region [" + defaultRegion.id() + "] from SDK"
                )
            );
        }
    }

    @TestLogging(reason = "testing WARN log output", value = "org.elasticsearch.repositories.s3.S3Service:WARN")
    public void testGetClientRegionFallbackToUsEast1() {
        final var regionRequested = new AtomicBoolean();
        try (var s3Service = new S3Service(mock(Environment.class), Settings.EMPTY, mock(ResourceWatcherService.class), () -> {
            assertTrue(regionRequested.compareAndSet(false, true));
            return null;
        })) {
            s3Service.start();
            assertTrue(regionRequested.get());

            final var clientName = randomBoolean() ? "default" : randomIdentifier();

            MockLog.assertThatLogger(
                () -> assertSame(
                    Region.US_EAST_1,
                    s3Service.getClientRegion(S3ClientSettings.getClientSettings(Settings.EMPTY, clientName))
                ),
                S3Service.class,
                new MockLog.SeenEventExpectation("warning", S3Service.class.getCanonicalName(), Level.WARN, """
                    found S3 client with no configured region, falling back to [us-east-1]; \
                    to suppress this warning, configure the [s3.client.CLIENT_NAME.region] setting on this node""")
            );
        }
    }
}
