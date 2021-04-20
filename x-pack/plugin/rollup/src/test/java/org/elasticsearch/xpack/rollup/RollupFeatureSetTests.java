/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.rollup.RollupFeatureSetUsage;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.core.Is.is;

public class RollupFeatureSetTests extends ESTestCase {

    public void testAvailable() {
        RollupFeatureSet featureSet = new RollupFeatureSet();
        assertThat(featureSet.available(), is(true));
    }

    public void testEnabledDefault() {
        RollupFeatureSet featureSet = new RollupFeatureSet();
        assertThat(featureSet.enabled(), is(true));
    }

    public void testUsage() throws ExecutionException, InterruptedException, IOException {
        RollupFeatureSet featureSet = new RollupFeatureSet();
        PlainActionFuture<XPackFeatureSet.Usage> future = new PlainActionFuture<>();
        featureSet.usage(future);
        XPackFeatureSet.Usage rollupUsage = future.get();
        BytesStreamOutput out = new BytesStreamOutput();
        rollupUsage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new RollupFeatureSetUsage(out.bytes().streamInput());
        for (XPackFeatureSet.Usage usage : Arrays.asList(rollupUsage, serializedUsage)) {
            assertThat(usage.name(), is(featureSet.name()));
            assertThat(usage.enabled(), is(featureSet.enabled()));
        }
    }

}
