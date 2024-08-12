/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DataStreamGlobalRetentionProviderTests extends ESTestCase {

    public void testOnlyFactoryRetentionFallback() {
        DataStreamFactoryRetention factoryRetention = randomNonEmptyFactoryRetention();
        DataStreamGlobalRetentionProvider resolver = new DataStreamGlobalRetentionProvider(factoryRetention);
        DataStreamGlobalRetention globalRetention = resolver.provide();
        assertThat(globalRetention, notNullValue());
        assertThat(globalRetention.defaultRetention(), equalTo(factoryRetention.getDefaultRetention()));
        assertThat(globalRetention.maxRetention(), equalTo(factoryRetention.getMaxRetention()));
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
        DataStreamGlobalRetentionProvider resolver = new DataStreamGlobalRetentionProvider(
            DataStreamFactoryRetention.emptyFactoryRetention()
        );
        assertThat(resolver.provide(), nullValue());
    }
}
