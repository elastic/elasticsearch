/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.s3;

import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.endpoints.S3EndpointParams;
import software.amazon.awssdk.services.s3.endpoints.internal.DefaultS3EndpointProvider;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
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
        final Settings settings = Settings.builder().put("endpoint", "http://first").build();
        final RepositoryMetadata metadata1 = new RepositoryMetadata("first", "s3", settings);
        final RepositoryMetadata metadata2 = new RepositoryMetadata("second", "s3", settings);
        final S3ClientSettings clientSettings = s3Service.settings(metadata2);
        final S3ClientSettings otherClientSettings = s3Service.settings(metadata2);
        assertSame(clientSettings, otherClientSettings);
        final AmazonS3Reference reference = s3Service.client(metadata1);
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
                    "found S3 client with no configured region and no configured endpoint, using region ["
                        + defaultRegion.id()
                        + "] from SDK"
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
                () -> assertNull(s3Service.getClientRegion(S3ClientSettings.getClientSettings(Settings.EMPTY, clientName))),
                S3Service.class,
                new MockLog.SeenEventExpectation("warning", S3Service.class.getCanonicalName(), Level.WARN, """
                    found S3 client with no configured region and no configured endpoint, \
                    falling back to [us-east-1] and enabling cross-region access; \
                    to suppress this warning, configure the [s3.client.CLIENT_NAME.region] setting on this node""")
            );
        }
    }

    public void testEndpointOverrideSchemeDefaultsToHttpsWhenNotSpecified() {
        final var endpointWithoutScheme = randomIdentifier() + ".ignore";
        final var clientName = randomIdentifier();
        assertThat(
            getEndpointUri(Settings.builder().put("s3.client." + clientName + ".endpoint", endpointWithoutScheme), clientName),
            equalTo(URI.create("https://" + endpointWithoutScheme))
        );
    }

    public void testEndpointOverrideSchemeUsesHttpsIfHttpsProtocolSpecified() {
        final var endpointWithoutScheme = randomIdentifier() + ".ignore";
        final var clientName = randomIdentifier();
        assertThat(
            getEndpointUri(
                Settings.builder()
                    .put("s3.client." + clientName + ".endpoint", endpointWithoutScheme)
                    .put("s3.client." + clientName + ".protocol", "https"),
                clientName
            ),
            equalTo(URI.create("https://" + endpointWithoutScheme))
        );
        assertCriticalWarnings(Strings.format("""
            [s3.client.%s.protocol] setting was deprecated in Elasticsearch and will be removed in a future release.""", clientName));
    }

    public void testEndpointOverrideSchemeUsesHttpIfHttpProtocolSpecified() {
        final var endpointWithoutScheme = randomIdentifier() + ".ignore";
        final var clientName = randomIdentifier();
        assertThat(
            getEndpointUri(
                Settings.builder()
                    .put("s3.client." + clientName + ".endpoint", endpointWithoutScheme)
                    .put("s3.client." + clientName + ".protocol", "http"),
                clientName
            ),
            equalTo(URI.create("http://" + endpointWithoutScheme))
        );
        assertCriticalWarnings(Strings.format("""
            [s3.client.%s.protocol] setting was deprecated in Elasticsearch and will be removed in a future release.""", clientName));
    }

    private static URI getEndpointUri(Settings.Builder settings, String clientName) {
        return new S3Service(
            mock(Environment.class),
            Settings.EMPTY,
            mock(ResourceWatcherService.class),
            () -> Region.of(randomIdentifier())
        ).buildClient(S3ClientSettings.getClientSettings(settings.build(), clientName), mock(SdkHttpClient.class))
            .serviceClientConfiguration()
            .endpointOverride()
            .get();
    }
}
