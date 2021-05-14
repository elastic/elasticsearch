/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.vectors;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.vectors.VectorsFeatureSetUsage;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class VectorsInfoTransportActionTests extends ESTestCase {

    public void testAvailable() throws Exception {
        VectorsInfoTransportAction featureSet = new VectorsInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class));
        assertThat(featureSet.available(), is(true));

        var usageAction = new VectorsUsageTransportAction(mock(TransportService.class), null, null,
            mock(ActionFilters.class), null);
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, null, future);
        XPackFeatureSet.Usage usage = future.get().getUsage();
        assertThat(usage.available(), is(true));

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new VectorsFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.available(), is(true));
    }

    public void testAlwaysEnabled() throws Exception {
        VectorsInfoTransportAction featureSet = new VectorsInfoTransportAction(
mock(TransportService.class), mock(ActionFilters.class));
        assertThat(featureSet.enabled(), is(true));

        VectorsUsageTransportAction usageAction = new VectorsUsageTransportAction(mock(TransportService.class),
            null, null, mock(ActionFilters.class), null);
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, null, future);
        XPackFeatureSet.Usage usage = future.get().getUsage();
        assertThat(usage.enabled(), is(true));

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new VectorsFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.enabled(), is(true));
    }

}
