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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.licensor.LicenseSigner;
import org.elasticsearch.protocol.xpack.license.LicensesStatus;
import org.elasticsearch.protocol.xpack.license.PutLicenseResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestMatchers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.hamcrest.Matchers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.license.LicenseService.LICENSE_EXPIRATION_WARNING_PERIOD;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Matchers.any;
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
        assertRegisterValidLicense(Settings.EMPTY,
            randomValueOtherThan(License.LicenseType.BASIC, () -> randomFrom(License.LicenseType.values())));
    }

    /**
     * Tests loading a license when {@link LicenseService#ALLOWED_LICENSE_TYPES_SETTING} is set,
     * and the uploaded license type matches
     */
    public void testSuccessfullyRegisterLicenseMatchingTypeRestrictions() throws Exception {
        final List<License.LicenseType> allowed = randomSubsetOf(
            randomIntBetween(1, LicenseService.ALLOWABLE_UPLOAD_TYPES.size() - 1), LicenseService.ALLOWABLE_UPLOAD_TYPES);
        final List<String> allowedNames = allowed.stream().map(License.LicenseType::getTypeName).collect(Collectors.toUnmodifiableList());
        final Settings settings = Settings.builder()
            .putList("xpack.license.upload.types", allowedNames)
            .build();
        assertRegisterValidLicense(settings, randomFrom(allowed));
    }

    /**
     * Tests loading a license when {@link LicenseService#ALLOWED_LICENSE_TYPES_SETTING} is set,
     * and the uploaded license type does not match
     */
    public void testFailToRegisterLicenseNotMatchingTypeRestrictions() throws Exception {
        final List<License.LicenseType> allowed = randomSubsetOf(
            randomIntBetween(1, LicenseService.ALLOWABLE_UPLOAD_TYPES.size() - 2), LicenseService.ALLOWABLE_UPLOAD_TYPES);
        final List<String> allowedNames = allowed.stream().map(License.LicenseType::getTypeName).collect(Collectors.toUnmodifiableList());
        final Settings settings = Settings.builder()
            .putList("xpack.license.upload.types", allowedNames)
            .build();
        final License.LicenseType notAllowed = randomValueOtherThanMany(
            test -> allowed.contains(test),
            () -> randomFrom(LicenseService.ALLOWABLE_UPLOAD_TYPES));
        assertRegisterDisallowedLicenseType(settings, notAllowed);
    }

    private void assertRegisterValidLicense(Settings baseSettings, License.LicenseType licenseType) throws IOException {
        tryRegisterLicense(baseSettings, licenseType,
            future -> assertThat(future.actionGet().status(), equalTo(LicensesStatus.VALID)));
    }

    private void assertRegisterDisallowedLicenseType(Settings baseSettings, License.LicenseType licenseType) throws IOException {
        tryRegisterLicense(baseSettings, licenseType, future -> {
            final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, future::actionGet);
            assertThat(exception, TestMatchers.throwableWithMessage(
                "Registering [" + licenseType.getTypeName() + "] licenses is not allowed on " + "this cluster"));
        });
    }

    private void tryRegisterLicense(Settings baseSettings, License.LicenseType licenseType,
                                    Consumer<PlainActionFuture<PutLicenseResponse>> assertion) throws IOException {
        final Settings settings = Settings.builder()
            .put(baseSettings)
            .put("path.home", createTempDir())
            .put("discovery.type", "single-node") // So we skip TLS checks
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
        final LicenseService service =
            new LicenseService(settings, threadPool, clusterService, clock, env, resourceWatcherService, licenseState);

        final PutLicenseRequest request = new PutLicenseRequest();
        request.license(spec(licenseType, TimeValue.timeValueDays(randomLongBetween(1, 1000))), XContentType.JSON);
        final PlainActionFuture<PutLicenseResponse> future = new PlainActionFuture<>();
        service.registerLicense(request, future);

        if (future.isDone()) {
            // If validation failed, the future might be done without calling the updater task.
            assertion.accept(future);
        } else {
            ArgumentCaptor<ClusterStateUpdateTask> taskCaptor = ArgumentCaptor.forClass(ClusterStateUpdateTask.class);
            verify(clusterService, times(1)).submitStateUpdateTask(any(), taskCaptor.capture());

            final ClusterStateUpdateTask task = taskCaptor.getValue();
            assertThat(task, instanceOf(AckedClusterStateUpdateTask.class));
            ((AckedClusterStateUpdateTask) task).onAllNodesAcked(null);

            assertion.accept(future);
        }
    }

    private BytesReference spec(License.LicenseType type, TimeValue expires) throws IOException {
        final License signed = sign(buildLicense(type, expires));
        return toSpec(signed);
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
        return License.builder()
            .uid(new UUID(randomLong(), randomLong()).toString())
            .type(type)
            .expiryDate(System.currentTimeMillis() + expires.millis())
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
