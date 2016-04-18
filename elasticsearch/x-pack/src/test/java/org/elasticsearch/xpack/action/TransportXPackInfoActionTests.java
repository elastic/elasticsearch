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
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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

        TransportXPackInfoAction action = new TransportXPackInfoAction(Settings.EMPTY, mock(ThreadPool.class),
                mock(TransportService.class), mock(ActionFilters.class), mock(IndexNameExpressionResolver.class), licensesService);

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

        assertThat(response.get().getBuildInfo(), notNullValue());
        assertThat(response.get().getLicenseInfo(), notNullValue());
        assertThat(response.get().getLicenseInfo().getExpiryDate(), is(expiryDate));
        assertThat(response.get().getLicenseInfo().getStatus(), is(status));
        assertThat(response.get().getLicenseInfo().getType(), is(type));
        assertThat(response.get().getLicenseInfo().getUid(), is(uid));
    }

}
