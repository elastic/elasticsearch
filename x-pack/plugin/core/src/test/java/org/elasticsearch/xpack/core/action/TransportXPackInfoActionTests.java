/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo.FeatureSet;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportXPackInfoActionTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testDoExecute() throws Exception {

        LicenseService licenseService = mock(LicenseService.class);

        NodeClient client = mock(NodeClient.class);
        when(client.getActionNames()).thenReturn(XPackInfoFeatureAction.ALL.stream().map(ActionType::name).toList());
        Map<XPackInfoFeatureAction, FeatureSet> featureSets = new HashMap<>();
        int featureSetCount = randomIntBetween(0, 5);
        for (XPackInfoFeatureAction infoAction : randomSubsetOf(featureSetCount, XPackInfoFeatureAction.ALL)) {
            FeatureSet featureSet = randomFeatureSet();
            featureSets.put(infoAction, featureSet);
            when(client.executeLocally(eq(infoAction), any(ActionRequest.class), any(ActionListener.class))).thenAnswer(answer -> {
                var listener = (ActionListener<XPackInfoFeatureResponse>) answer.getArguments()[2];
                listener.onResponse(new XPackInfoFeatureResponse(featureSet));
                return null;
            });
        }

        License license = mock(License.class);
        long expiryDate = randomLong();
        when(license.expiryDate()).thenReturn(expiryDate);
        String type = randomAlphaOfLength(10);
        when(license.type()).thenReturn(type);
        License.OperationMode mode = randomFrom(License.OperationMode.values());
        when(license.operationMode()).thenReturn(mode);
        String uid = randomAlphaOfLength(30);
        when(license.uid()).thenReturn(uid);
        when(licenseService.getLicense()).thenReturn(license);

        XPackInfoRequest request = new XPackInfoRequest();
        request.setVerbose(randomBoolean());

        EnumSet<XPackInfoRequest.Category> categories = EnumSet.noneOf(XPackInfoRequest.Category.class);
        int maxCategoryCount = randomIntBetween(0, XPackInfoRequest.Category.values().length);
        for (int i = 0; i < maxCategoryCount; i++) {
            categories.add(randomFrom(XPackInfoRequest.Category.values()));
        }
        categories.add(XPackInfoRequest.Category.FEATURES);
        request.setCategories(categories);

        XPackInfoResponse response = getResponse(request, client, licenseService, new ArrayList<>(featureSets.keySet()));

        if (request.getCategories().contains(XPackInfoRequest.Category.BUILD)) {
            assertThat(response.getBuildInfo(), notNullValue());
        } else {
            assertThat(response.getBuildInfo(), nullValue());
        }

        if (request.getCategories().contains(XPackInfoRequest.Category.LICENSE)) {
            assertThat(response.getLicenseInfo(), notNullValue());
            assertThat(response.getLicenseInfo().getExpiryDate(), is(expiryDate));
            assertThat(response.getLicenseInfo().getType(), is(type));
            assertThat(response.getLicenseInfo().getMode(), is(mode.name().toLowerCase(Locale.ROOT)));
            assertThat(response.getLicenseInfo().getUid(), is(uid));
        } else {
            assertThat(response.getLicenseInfo(), nullValue());
        }

        if (request.getCategories().contains(XPackInfoRequest.Category.FEATURES)) {
            assertThat(response.getFeatureSetsInfo(), notNullValue());
            Map<String, FeatureSet> features = response.getFeatureSetsInfo().getFeatureSets();
            assertThat(features.size(), is(featureSets.size()));
            for (FeatureSet fs : featureSets.values()) {
                assertThat(features, hasKey(fs.name()));
                assertThat(features.get(fs.name()).name(), equalTo(fs.name()));
                assertThat(features.get(fs.name()).available(), equalTo(fs.available()));
                assertThat(features.get(fs.name()).enabled(), equalTo(fs.enabled()));
            }
        } else {
            assertThat(response.getFeatureSetsInfo(), nullValue());
        }

    }

    // Tests where a particular feature is not present.
    public void testFeatureNotPresent() throws Exception {
        final int featureSetCount = randomIntBetween(2, 7);
        List<XPackInfoFeatureAction> totalFeatureSet = randomSubsetOf(featureSetCount, XPackInfoFeatureAction.ALL);
        List<XPackInfoFeatureAction> totalFeatureSetMinusOne = totalFeatureSet.subList(1, totalFeatureSet.size());
        assert totalFeatureSet.size() - 1 == totalFeatureSetMinusOne.size();

        NodeClient client = mock(NodeClient.class);
        when(client.getActionNames()).thenReturn(totalFeatureSetMinusOne.stream().map(ActionType::name).toList());

        // Mocks all present features
        Map<XPackInfoFeatureAction, FeatureSet> featureSets = new HashMap<>();
        for (XPackInfoFeatureAction infoAction : totalFeatureSetMinusOne) {
            FeatureSet featureSet = randomFeatureSet();
            featureSets.put(infoAction, featureSet);
            when(client.executeLocally(eq(infoAction), any(ActionRequest.class), any(ActionListener.class))).thenAnswer(answer -> {
                var listener = (ActionListener<XPackInfoFeatureResponse>) answer.getArguments()[2];
                listener.onResponse(new XPackInfoFeatureResponse(featureSet));
                return null;
            });
        }
        // Mock missing features
        XPackInfoFeatureAction missingAction = totalFeatureSet.get(0);
        when(client.executeLocally(eq(missingAction), any(ActionRequest.class), any(ActionListener.class))).thenAnswer(answer -> {
            throw new IllegalStateException("failed to find action [ActionType [" + missingAction + "]");
        });

        XPackInfoRequest request = new XPackInfoRequest();
        request.setCategories(EnumSet.of(XPackInfoRequest.Category.FEATURES));
        XPackInfoResponse response = getResponse(request, client, mock(LicenseService.class), totalFeatureSet);

        assertThat(response.getFeatureSetsInfo(), notNullValue());
        Map<String, FeatureSet> features = response.getFeatureSetsInfo().getFeatureSets();
        assertThat(features.size(), is(totalFeatureSetMinusOne.size()));
        for (FeatureSet fs : featureSets.values()) {
            assertThat(features, hasKey(fs.name()));
            assertThat(features.get(fs.name()).name(), equalTo(fs.name()));
            assertThat(features.get(fs.name()).available(), equalTo(fs.available()));
            assertThat(features.get(fs.name()).enabled(), equalTo(fs.enabled()));
        }
    }

    static XPackInfoResponse getResponse(
        XPackInfoRequest request,
        NodeClient client,
        LicenseService licenseService,
        List<XPackInfoFeatureAction> totalFeatureSet
    ) throws Exception {
        TransportXPackInfoAction action = new TransportXPackInfoAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            licenseService,
            client
        ) {
            @Override
            protected List<XPackInfoFeatureAction> infoActions() {
                return totalFeatureSet;
            }
        };

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
        return response.get();
    }

    static FeatureSet randomFeatureSet() {
        return new FeatureSet(randomAlphaOfLength(5), randomBoolean(), randomBoolean());
    }
}
