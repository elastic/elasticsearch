/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionTests.randomGlobalRetention;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DataStreamGlobalRetentionResolverTests extends ESTestCase {

    public void testOnlyGlobalRetentionMetadata() {
        DataStreamFactoryRetention factoryRetention = randomBoolean()
            ? DataStreamFactoryRetention.emptyFactoryRetention()
            : randomNonEmptyFactoryRetention();
        DataStreamGlobalRetentionResolver resolver = new DataStreamGlobalRetentionResolver(factoryRetention);
        DataStreamGlobalRetention expectedGlobalRetention = randomGlobalRetention();
        DataStreamGlobalRetention globalRetention = resolver.resolve(
            ClusterState.builder(ClusterName.DEFAULT).customs(Map.of(DataStreamGlobalRetention.TYPE, expectedGlobalRetention)).build()
        );
        assertThat(globalRetention, notNullValue());
        assertThat(globalRetention.getDefaultRetention(), equalTo(expectedGlobalRetention.getDefaultRetention()));
        assertThat(globalRetention.getMaxRetention(), equalTo(expectedGlobalRetention.getMaxRetention()));
    }

    public void testOnlyFactoryRetentionFallback() {
        DataStreamFactoryRetention factoryRetention = randomNonEmptyFactoryRetention();
        DataStreamGlobalRetentionResolver resolver = new DataStreamGlobalRetentionResolver(factoryRetention);
        DataStreamGlobalRetention globalRetention = resolver.resolve(ClusterState.EMPTY_STATE);
        assertThat(globalRetention, notNullValue());
        assertThat(globalRetention.getDefaultRetention(), equalTo(factoryRetention.getDefaultRetention()));
        assertThat(globalRetention.getMaxRetention(), equalTo(factoryRetention.getMaxRetention()));
    }

    private static DataStreamFactoryRetention randomNonEmptyFactoryRetention() {
        boolean withDefault = randomBoolean();
        TimeValue defaultRetention = withDefault ? TimeValue.timeValueDays(randomIntBetween(10, 20)) : null;
        TimeValue maxRetention = withDefault && randomBoolean() ? null : TimeValue.timeValueDays(randomIntBetween(50, 200));
        return new DataStreamFactoryRetention() {
            @Override
            public TimeValue getMaxRetention() {
                return maxRetention;
            }

            @Override
            public TimeValue getDefaultRetention() {
                return defaultRetention;
            }

            @Override
            public void init(ClusterSettings clusterSettings) {

            }
        };
    }

    public void testNoRetentionConfiguration() {
        DataStreamGlobalRetentionResolver resolver = new DataStreamGlobalRetentionResolver(
            DataStreamFactoryRetention.emptyFactoryRetention()
        );
        assertThat(resolver.resolve(ClusterState.EMPTY_STATE), nullValue());
    }
}
