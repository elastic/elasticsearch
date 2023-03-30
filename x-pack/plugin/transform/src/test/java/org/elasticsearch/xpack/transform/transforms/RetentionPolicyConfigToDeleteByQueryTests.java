/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.DestConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.RetentionPolicyConfig;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;

public class RetentionPolicyConfigToDeleteByQueryTests extends ESTestCase {

    public void testRetentionPolicyConfigBasics() {
        // null checkpoint
        assertNull(
            RetentionPolicyToDeleteByQueryRequestConverter.buildDeleteByQueryRequest(
                randomRetentionPolicyConfig(),
                SettingsConfigTests.randomSettingsConfig(),
                DestConfigTests.randomDestConfig(),
                null
            )
        );

        // empty checkpoint
        assertNull(
            RetentionPolicyToDeleteByQueryRequestConverter.buildDeleteByQueryRequest(
                randomRetentionPolicyConfig(),
                SettingsConfigTests.randomSettingsConfig(),
                DestConfigTests.randomDestConfig(),
                TransformCheckpoint.EMPTY
            )
        );
    }

    public void testTimeBasedRetentionPolicyConfig() {
        SettingsConfig settingsConfig = SettingsConfigTests.randomSettingsConfig();
        DestConfig destConfig = DestConfigTests.randomDestConfig();

        RetentionPolicyConfig retentionPolicyConfig = new TimeRetentionPolicyConfig(
            randomAlphaOfLengthBetween(3, 5),
            new TimeValue(60_000L)
        );
        TransformCheckpoint checkpoint = new TransformCheckpoint(
            randomAlphaOfLengthBetween(3, 5),
            10_000_000L,
            1,
            Collections.emptyMap(),
            8_000_000L
        );
        DeleteByQueryRequest deleteByQueryRequest = RetentionPolicyToDeleteByQueryRequestConverter.buildDeleteByQueryRequest(
            retentionPolicyConfig,
            settingsConfig,
            destConfig,
            checkpoint
        );

        assertNotNull(deleteByQueryRequest);
        assertNotNull(deleteByQueryRequest.getSearchRequest().source());
        assertNotNull(deleteByQueryRequest.getSearchRequest().source().query());

        assertFalse(deleteByQueryRequest.isRefresh());
        assertEquals(0, deleteByQueryRequest.getMaxRetries());
        assertEquals(1, deleteByQueryRequest.getSearchRequest().indices().length);
        assertEquals(destConfig.getIndex(), deleteByQueryRequest.getSearchRequest().indices()[0]);

        if (settingsConfig.getDocsPerSecond() != null) {
            assertEquals(settingsConfig.getDocsPerSecond().floatValue(), deleteByQueryRequest.getRequestsPerSecond(), 1E-15);
        }

        QueryBuilder query = deleteByQueryRequest.getSearchRequest().source().query();
        assertThat(query, instanceOf(RangeQueryBuilder.class));
        RangeQueryBuilder rangeQuery = (RangeQueryBuilder) query;
        assertThat(rangeQuery.to(), equalTo(DateFormatter.forPattern("strict_date_optional_time").formatMillis(9_940_000L)));
        assertTrue(rangeQuery.includeLower());
    }

    private static RetentionPolicyConfig randomRetentionPolicyConfig() {
        return TimeRetentionPolicyConfigTests.randomTimeRetentionPolicyConfig();
    }
}
