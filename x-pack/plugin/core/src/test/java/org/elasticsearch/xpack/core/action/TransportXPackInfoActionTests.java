/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo.FeatureSet;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureSet;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportXPackInfoActionTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testDoExecute() throws Exception {
        EnumSet<XPackInfoRequest.Category> categories = EnumSet.noneOf(XPackInfoRequest.Category.class);
        int maxCategoryCount = randomIntBetween(0, XPackInfoRequest.Category.values().length);
        for (int i = 0; i < maxCategoryCount; i++) {
            categories.add(randomFrom(XPackInfoRequest.Category.values()));
        }

        License license = mock(License.class);
        long expiryDate = randomLong();
        when(license.expiryDate()).thenReturn(expiryDate);
        String licenseType = randomAlphaOfLength(10);
        when(license.type()).thenReturn(licenseType);
        License.OperationMode licenseMode = randomFrom(License.OperationMode.values());
        when(license.operationMode()).thenReturn(licenseMode);
        String uid = randomAlphaOfLength(30);
        when(license.uid()).thenReturn(uid);

        checkAction(categories, -1, license, (XPackInfoResponse.LicenseInfo licenseInfo) -> {
            assertThat(licenseInfo.getExpiryDate(), is(expiryDate));
            assertThat(licenseInfo.getType(), is(licenseType));
            assertThat(licenseInfo.getMode(), is(licenseMode.name().toLowerCase(Locale.ROOT)));
            assertThat(licenseInfo.getUid(), is(uid));
        });
    }

    public void testDoExecuteWithEnterpriseLicenseWithoutBackwardsCompat() throws Exception {
        EnumSet<XPackInfoRequest.Category> categories = EnumSet.allOf(XPackInfoRequest.Category.class);

        License license = mock(License.class);
        when(license.expiryDate()).thenReturn(randomLong());
        when(license.type()).thenReturn("enterprise");
        when(license.operationMode()).thenReturn(License.OperationMode.ENTERPRISE);
        when(license.uid()).thenReturn(randomAlphaOfLength(30));

        checkAction(categories, randomFrom(License.VERSION_ENTERPRISE, License.VERSION_CURRENT, -1), license, licenseInfo -> {
            assertThat(licenseInfo.getType(), is("enterprise"));
            assertThat(licenseInfo.getMode(), is("enterprise"));
        });
    }

    public void testDoExecuteWithEnterpriseLicenseWithBackwardsCompat() throws Exception {
        EnumSet<XPackInfoRequest.Category> categories = EnumSet.allOf(XPackInfoRequest.Category.class);

        License license = mock(License.class);
        when(license.expiryDate()).thenReturn(randomLong());
        when(license.type()).thenReturn("enterprise");
        when(license.operationMode()).thenReturn(License.OperationMode.ENTERPRISE);
        when(license.uid()).thenReturn(randomAlphaOfLength(30));

        checkAction(categories, randomFrom(License.VERSION_START_DATE, License.VERSION_CRYPTO_ALGORITHMS), license, licenseInfo -> {
            assertThat(licenseInfo.getType(), is("platinum"));
            assertThat(licenseInfo.getMode(), is("platinum"));
        });
    }

    private void checkAction(
        EnumSet<XPackInfoRequest.Category> categories,
        int licenseVersion,
        License license,
        Consumer<XPackInfoResponse.LicenseInfo> licenseVerifier
    ) throws InterruptedException {
        LicenseService licenseService = mock(LicenseService.class);

        final Set<XPackFeatureSet> featureSets = new HashSet<>();
        int featureSetCount = randomIntBetween(0, 5);
        for (int i = 0; i < featureSetCount; i++) {
            XPackFeatureSet fs = mock(XPackFeatureSet.class);
            when(fs.name()).thenReturn(randomAlphaOfLength(5));
            when(fs.available()).thenReturn(randomBoolean());
            when(fs.enabled()).thenReturn(randomBoolean());
            featureSets.add(fs);
        }

        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        TransportXPackInfoAction action = new TransportXPackInfoAction(
            transportService,
            mock(ActionFilters.class),
            licenseService,
            featureSets
        );

        when(licenseService.getLicense()).thenReturn(license);

        XPackInfoRequest request = new XPackInfoRequest();
        request.setVerbose(randomBoolean());
        request.setCategories(categories);
        if (licenseVersion != -1) {
            request.setLicenseVersion(licenseVersion);
        }

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<XPackInfoResponse> response = new AtomicReference<>();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<XPackInfoResponse>() {
            @Override
            public void onResponse(XPackInfoResponse infoResponse) {
                response.set(infoResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                error.set(e);
                latch.countDown();
            }
        });

        if (latch.await(5, TimeUnit.SECONDS) == false) {
            fail("waiting too long for ");
        }

        assertThat(error.get(), nullValue());
        assertThat(response.get(), notNullValue());

        if (request.getCategories().contains(XPackInfoRequest.Category.BUILD)) {
            assertThat(response.get().getBuildInfo(), notNullValue());
        } else {
            assertThat(response.get().getBuildInfo(), nullValue());
        }

        if (request.getCategories().contains(XPackInfoRequest.Category.LICENSE)) {
            assertThat(response.get().getLicenseInfo(), notNullValue());
            licenseVerifier.accept(response.get().getLicenseInfo());
        } else {
            assertThat(response.get().getLicenseInfo(), nullValue());
        }

        if (request.getCategories().contains(XPackInfoRequest.Category.FEATURES)) {
            assertThat(response.get().getFeatureSetsInfo(), notNullValue());
            Map<String, FeatureSet> features = response.get().getFeatureSetsInfo().getFeatureSets();
            assertThat(features.size(), is(featureSets.size()));
            for (XPackFeatureSet fs : featureSets) {
                assertThat(features, hasKey(fs.name()));
                assertThat(features.get(fs.name()).name(), equalTo(fs.name()));
                assertThat(features.get(fs.name()).available(), equalTo(fs.available()));
                assertThat(features.get(fs.name()).enabled(), equalTo(fs.enabled()));
            }
        } else {
            assertThat(response.get().getFeatureSetsInfo(), nullValue());
        }
    }

}
