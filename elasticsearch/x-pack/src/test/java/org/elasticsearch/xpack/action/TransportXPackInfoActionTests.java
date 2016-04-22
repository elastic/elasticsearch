/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.LicensesService;
import org.elasticsearch.shield.user.AnonymousUser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.action.XPackInfoResponse.FeatureSetsInfo.FeatureSet;
import org.junit.After;
import org.junit.Before;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportXPackInfoActionTests extends ESTestCase {

    private boolean anonymousEnabled;

    @Before
    public void maybeEnableAnonymous() {
        anonymousEnabled = randomBoolean();
        if (anonymousEnabled) {
            Settings settings = Settings.builder().put(AnonymousUser.ROLES_SETTING.getKey(), "superuser").build();
            AnonymousUser.initialize(settings);
        }
    }

    @After
    public void resetAnonymous() {
        AnonymousUser.initialize(Settings.EMPTY);
    }

    public void testDoExecute() throws Exception {

        LicensesService licensesService = mock(LicensesService.class);

        final Set<XPackFeatureSet> featureSets = new HashSet<>();
        int featureSetCount = randomIntBetween(0, 5);
        for (int i = 0; i < featureSetCount; i++) {
            XPackFeatureSet fs = mock(XPackFeatureSet.class);
            when(fs.name()).thenReturn(randomAsciiOfLength(5));
            when(fs.description()).thenReturn(randomAsciiOfLength(10));
            when(fs.available()).thenReturn(randomBoolean());
            when(fs.enabled()).thenReturn(randomBoolean());
            featureSets.add(fs);
        }

        TransportXPackInfoAction action = new TransportXPackInfoAction(Settings.EMPTY, mock(ThreadPool.class),
                mock(TransportService.class), mock(ActionFilters.class), mock(IndexNameExpressionResolver.class),
                licensesService, featureSets);

        License license = mock(License.class);
        long expiryDate = randomLong();
        when(license.expiryDate()).thenReturn(expiryDate);
        License.Status status = randomFrom(License.Status.values());
        when(license.status()).thenReturn(status);
        String type = randomAsciiOfLength(10);
        when(license.type()).thenReturn(type);
        String uid = randomAsciiOfLength(30);
        when(license.uid()).thenReturn(uid);
        when(licensesService.getLicense()).thenReturn(license);

        XPackInfoRequest request = new XPackInfoRequest();
        request.setVerbose(randomBoolean());

        EnumSet<XPackInfoRequest.Category> categories = EnumSet.noneOf(XPackInfoRequest.Category.class);
        int maxCategoryCount = randomIntBetween(0, XPackInfoRequest.Category.values().length);
        for (int i = 0; i < maxCategoryCount; i++) {
            categories.add(randomFrom(XPackInfoRequest.Category.values()));
        }
        request.setCategories(categories);

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<XPackInfoResponse> response = new AtomicReference<>();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        action.doExecute(request, new ActionListener<XPackInfoResponse>() {
            @Override
            public void onResponse(XPackInfoResponse infoResponse) {
                response.set(infoResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable e) {
                error.set(e);
                latch.countDown();
            }
        });

        if (!latch.await(5, TimeUnit.SECONDS)) {
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
            assertThat(response.get().getLicenseInfo().getExpiryDate(), is(expiryDate));
            assertThat(response.get().getLicenseInfo().getStatus(), is(status));
            assertThat(response.get().getLicenseInfo().getType(), is(type));
            assertThat(response.get().getLicenseInfo().getUid(), is(uid));
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
                if (!request.isVerbose()) {
                    assertThat(features.get(fs.name()).description(), is(nullValue()));
                } else {
                    assertThat(features.get(fs.name()).description(), is(fs.description()));
                }
                assertThat(features.get(fs.name()).available(), equalTo(fs.available()));
                assertThat(features.get(fs.name()).enabled(), equalTo(fs.enabled()));
            }
        } else {
            assertThat(response.get().getFeatureSetsInfo(), nullValue());
        }

    }

}
