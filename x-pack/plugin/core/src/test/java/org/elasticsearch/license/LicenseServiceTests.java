/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.licensor.LicenseSigner;
import org.elasticsearch.protocol.xpack.license.LicensesStatus;
import org.elasticsearch.protocol.xpack.license.PutLicenseResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestMatchers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_TYPE_SETTING;
import static org.elasticsearch.discovery.DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE;
import static org.elasticsearch.license.LicenseService.LICENSE_EXPIRATION_WARNING_PERIOD;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Due to changes in JDK9 where locale data is used from CLDR, the licence message will differ in jdk 8 and jdk9+
 * https://openjdk.java.net/jeps/252
 * We run ES with -Djava.locale.providers=SPI,COMPAT and same option has to be applied when running this test from IDE
 */
public class LicenseServiceTests extends ESTestCase {

    public void testLogExpirationWarning() {
        long time = LocalDate.of(2018, 11, 15).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        final boolean expired = randomBoolean();
        final String message = LicenseService.buildExpirationMessage(time, expired).toString();
        if (expired) {
            assertThat(message, startsWith("LICENSE [EXPIRED] ON [THURSDAY, NOVEMBER 15, 2018].\n"));
        } else {
            assertThat(message, startsWith("License [will expire] on [Thursday, November 15, 2018].\n"));
        }
    }

    /**
     * Tests loading a license when {@link LicenseService#ALLOWED_LICENSE_TYPES_SETTING} is on its default value (all license types)
     */
    public void testRegisterLicenseWithoutTypeRestrictions() throws Exception {
        assertRegisterValidLicense(
            Settings.EMPTY,
            randomValueOtherThan(License.LicenseType.BASIC, () -> randomFrom(License.LicenseType.values()))
        );
    }

    /**
     * Tests loading a license when {@link LicenseService#ALLOWED_LICENSE_TYPES_SETTING} is set,
     * and the uploaded license type matches
     */
    public void testSuccessfullyRegisterLicenseMatchingTypeRestrictions() throws Exception {
        final List<License.LicenseType> allowed = randomSubsetOf(
            randomIntBetween(1, LicenseService.ALLOWABLE_UPLOAD_TYPES.size() - 1),
            LicenseService.ALLOWABLE_UPLOAD_TYPES
        );
        final List<String> allowedNames = allowed.stream().map(License.LicenseType::getTypeName).toList();
        final Settings settings = Settings.builder().putList("xpack.license.upload.types", allowedNames).build();
        assertRegisterValidLicense(settings, randomFrom(allowed));
    }

    /**
     * Tests loading a license when {@link LicenseService#ALLOWED_LICENSE_TYPES_SETTING} is set,
     * and the uploaded license type does not match
     */
    public void testFailToRegisterLicenseNotMatchingTypeRestrictions() throws Exception {
        final List<License.LicenseType> allowed = randomSubsetOf(
            randomIntBetween(1, LicenseService.ALLOWABLE_UPLOAD_TYPES.size() - 2),
            LicenseService.ALLOWABLE_UPLOAD_TYPES
        );
        final List<String> allowedNames = allowed.stream().map(License.LicenseType::getTypeName).toList();
        final Settings settings = Settings.builder().putList("xpack.license.upload.types", allowedNames).build();
        final License.LicenseType notAllowed = randomValueOtherThanMany(
            test -> allowed.contains(test),
            () -> randomFrom(LicenseService.ALLOWABLE_UPLOAD_TYPES)
        );
        assertRegisterDisallowedLicenseType(settings, notAllowed);
    }

    /**
     * Tests that the license overrides from {@link LicenseOverrides} are applied when an override is present for a license's ID.
     */
    public void testLicenseExpiryDateOverride() throws IOException {
        UUID licenseId = UUID.fromString("12345678-abcd-0000-0000-000000000000"); // Special test UUID
        License.LicenseType type = randomFrom(License.LicenseType.values());
        License testLicense = buildLicense(licenseId, type, TimeValue.timeValueDays(randomIntBetween(1, 100)).millis());

        assertThat(LicenseService.getExpiryDate(testLicense), equalTo(new Date(42000L).getTime()));
    }

    /**
     * Tests that a license with an overridden expiry date that's in the past is expired.
     */
    public void testLicenseWithOverridenExpiryInPastIsExpired() throws IOException {
        UUID licenseId = UUID.fromString("12345678-abcd-0000-0000-000000000000"); // Special test UUID
        License.LicenseType type = randomFrom(LicenseService.ALLOWABLE_UPLOAD_TYPES);
        License testLicense = sign(buildLicense(licenseId, type, TimeValue.timeValueDays(randomIntBetween(1, 100)).millis()));

        tryRegisterLicense(Settings.EMPTY, testLicense, future -> {
            PutLicenseResponse response = future.actionGet();
            assertThat(response.status(), equalTo(LicensesStatus.EXPIRED));
        });
    }

    private void assertRegisterValidLicense(Settings baseSettings, License.LicenseType licenseType) throws IOException {
        tryRegisterLicense(baseSettings, licenseType, future -> assertThat(future.actionGet().status(), equalTo(LicensesStatus.VALID)));
    }

    private void assertRegisterDisallowedLicenseType(Settings baseSettings, License.LicenseType licenseType) throws IOException {
        tryRegisterLicense(baseSettings, licenseType, future -> {
            final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, future::actionGet);
            assertThat(
                exception,
                TestMatchers.throwableWithMessage(
                    "Registering [" + licenseType.getTypeName() + "] licenses is not allowed on " + "this cluster"
                )
            );
        });
    }

    private void tryRegisterLicense(
        Settings baseSettings,
        License.LicenseType licenseType,
        Consumer<PlainActionFuture<PutLicenseResponse>> assertion
    ) throws IOException {
        tryRegisterLicense(baseSettings, sign(buildLicense(licenseType, TimeValue.timeValueDays(randomLongBetween(1, 1000)))), assertion);
    }

    private void tryRegisterLicense(Settings baseSettings, License license, Consumer<PlainActionFuture<PutLicenseResponse>> assertion)
        throws IOException {
        final Settings settings = Settings.builder()
            .put(baseSettings)
            .put("path.home", createTempDir())
            .put(DISCOVERY_TYPE_SETTING.getKey(), SINGLE_NODE_DISCOVERY_TYPE) // So we skip TLS checks
            .build();

        final ClusterState clusterState = mock(ClusterState.class);
        Mockito.when(clusterState.metadata()).thenReturn(Metadata.EMPTY_METADATA);

        final ClusterService clusterService = mock(ClusterService.class);
        Mockito.when(clusterService.state()).thenReturn(clusterState);

        final Clock clock = randomBoolean() ? Clock.systemUTC() : Clock.systemDefaultZone();
        final Environment env = TestEnvironment.newEnvironment(settings);
        final ResourceWatcherService resourceWatcherService = mock(ResourceWatcherService.class);
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        final ThreadPool threadPool = mock(ThreadPool.class);
        final LicenseService service = new LicenseService(
            settings,
            threadPool,
            clusterService,
            clock,
            env,
            resourceWatcherService,
            licenseState
        );

        final PutLicenseRequest request = new PutLicenseRequest();
        request.license(toSpec(license), XContentType.JSON);
        final PlainActionFuture<PutLicenseResponse> future = new PlainActionFuture<>();
        service.registerLicense(request, future);

        if (future.isDone()) {
            // If validation failed, the future might be done without calling the updater task.
            assertion.accept(future);
        } else {
            ArgumentCaptor<ClusterStateUpdateTask> taskCaptor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
            verify(clusterService, times(1)).submitStateUpdateTask(any(), taskCaptor.capture(), any());

            final ClusterStateUpdateTask task = taskCaptor.getValue();
            assertThat(task, instanceOf(AckedClusterStateUpdateTask.class));
            ((AckedClusterStateUpdateTask) task).onAllNodesAcked();

            assertion.accept(future);
        }
    }

    private BytesReference toSpec(License license) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        builder.startObject("license");
        license.toInnerXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        builder.endObject();
        builder.flush();
        return BytesReference.bytes(builder);
    }

    private License sign(License license) throws IOException {
        final Path publicKey = getDataPath("/public.key");
        final Path privateKey = getDataPath("/private.key");
        final LicenseSigner signer = new LicenseSigner(privateKey, publicKey);

        return signer.sign(license);
    }

    private License buildLicense(License.LicenseType type, TimeValue expires) {
        return buildLicense(new UUID(randomLong(), randomLong()), type, expires.millis());
    }

    private License buildLicense(UUID licenseId, License.LicenseType type, long expires) {
        return License.builder()
            .uid(licenseId.toString())
            .type(type)
            .expiryDate(System.currentTimeMillis() + expires)
            .issuer(randomAlphaOfLengthBetween(5, 60))
            .issuedTo(randomAlphaOfLengthBetween(5, 60))
            .issueDate(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(randomLongBetween(1, 5000)))
            .maxNodes(type == License.LicenseType.ENTERPRISE ? -1 : randomIntBetween(1, 500))
            .maxResourceUnits(type == License.LicenseType.ENTERPRISE ? randomIntBetween(10, 500) : -1)
            .signature(null)
            .build();
    }

    private void assertExpiryWarning(long adjustment, String msg) {
        long now = System.currentTimeMillis();
        long expiration = now + adjustment;
        String warning = LicenseService.getExpiryWarning(expiration, now);
        if (msg == null) {
            assertThat(warning, is(nullValue()));
        } else {
            assertThat(warning, Matchers.containsString(msg));
        }
    }

    public void testNoExpiryWarning() {
        assertExpiryWarning(LICENSE_EXPIRATION_WARNING_PERIOD.getMillis(), null);
    }

    public void testExpiryWarningSoon() {
        assertExpiryWarning(LICENSE_EXPIRATION_WARNING_PERIOD.getMillis() - 1, "Your license will expire in [6] days");
    }

    public void testExpiryWarningToday() {
        assertExpiryWarning(1, "Your license expires today");
    }

    public void testExpiryWarningExpired() {
        assertExpiryWarning(0, "Your license expired on");
    }

}
