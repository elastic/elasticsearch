/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.AggregationConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.telemetry.TransformMeterRegistry;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.ActionTestUtils.assertNoFailureListener;
import static org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests.randomTransformConfigWithSettings;
import static org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests.randomPivotConfig;
import static org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests.randomPivotConfigWithDeprecatedFields;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TransformConfigAutoMigrationTests extends ESTestCase {
    private TransformConfigManager transformConfigManager;
    private TransformAuditor auditor;
    private TransformConfigAutoMigration autoMigration;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        transformConfigManager = mock();
        auditor = mock();
        ThreadPool threadPool = mock();
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);

        autoMigration = new TransformConfigAutoMigration(transformConfigManager, auditor, TransformMeterRegistry.noOp(), threadPool);
    }

    protected boolean enableWarningsCheck() {
        return false;
    }

    public void testMigrateWithNoDeprecatedSettings() {
        var config = randomTransformConfigWithNoDeprecatedSettings();

        var updatedConfig = autoMigration.migrate(config);

        assertThat(updatedConfig, sameInstance(config));
        verifyNoInteractions(auditor);
    }

    private static TransformConfig randomTransformConfigWithNoDeprecatedSettings() {
        return randomTransformConfigWithSettings(SettingsConfig.EMPTY, randomPivotConfig(), null);
    }

    public void testMigrateWithMaxPageSearchSize() {
        var config = randomTransformConfigWithDeprecatedSettings();

        var updatedConfig = autoMigration.migrate(config);

        assertThatMaxPageSearchSizeMigrated(updatedConfig, config);
        verify(auditor, only()).info(eq(updatedConfig.getId()), eq(TransformMessages.MAX_PAGE_SEARCH_SIZE_MIGRATION));
    }

    private static TransformConfig randomTransformConfigWithDeprecatedSettings() {
        return randomTransformConfigWithSettings(SettingsConfig.EMPTY, randomPivotConfigWithDeprecatedFields(), null);
    }

    private void assertThatMaxPageSearchSizeMigrated(TransformConfig updatedConfig, TransformConfig originalConfig) {
        assertThat(updatedConfig, not(sameInstance(originalConfig)));
        assertThat(updatedConfig.getPivotConfig().getMaxPageSearchSize(), nullValue());
        assertThat(updatedConfig.getSettings().getMaxPageSearchSize(), equalTo(originalConfig.getPivotConfig().getMaxPageSearchSize()));
    }

    public void testMigrateWithInvalidMaxPageSearchSize() {
        var config = randomTransformConfigWithSettings(
            SettingsConfig.EMPTY,
            new PivotConfig(
                GroupConfigTests.randomGroupConfig(TransformConfigVersion.CURRENT),
                AggregationConfigTests.randomAggregationConfig(),
                1
            ),
            null
        );

        var updatedConfig = autoMigration.migrate(config);

        assertThat(updatedConfig, sameInstance(config));
        verifyNoInteractions(auditor);
    }

    public void testMigrateAndSaveWithNoChanges() throws InterruptedException {
        var originalConfig = randomTransformConfigWithNoDeprecatedSettings();

        testMigration(originalConfig, updatedConfig -> {
            assertThat(updatedConfig, sameInstance(originalConfig));
            verifyNoInteractions(transformConfigManager);
            verifyNoInteractions(auditor);
        });
    }

    private void testMigration(TransformConfig config, CheckedConsumer<TransformConfig, Exception> updatedConfigListener)
        throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        autoMigration.migrateAndSave(config, ActionListener.runAfter(assertNoFailureListener(updatedConfigListener), latch::countDown));
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    public void testMigrateAndSaveSuccess() throws InterruptedException {
        doAnswer(ans -> {
            ActionListener<Boolean> listener = ans.getArgument(2);
            listener.onResponse(true);
            return null;
        }).when(transformConfigManager).updateTransformConfiguration(any(), any(), any());
        var originalConfig = randomTransformConfigWithDeprecatedSettings();
        doAnswer(ans -> {
            ActionListener<Tuple<TransformConfig, SeqNoPrimaryTermAndIndex>> listener = ans.getArgument(1);
            listener.onResponse(Tuple.tuple(originalConfig, mock()));
            return null;
        }).when(transformConfigManager).getTransformConfigurationForUpdate(any(), any());

        testMigration(originalConfig, updatedConfig -> {
            assertThatMaxPageSearchSizeMigrated(updatedConfig, originalConfig);
            verify(auditor, only()).info(eq(updatedConfig.getId()), eq(TransformMessages.MAX_PAGE_SEARCH_SIZE_MIGRATION));
        });
    }

    public void testMigrateAndSaveWithGetSequenceError() throws InterruptedException {
        var originalConfig = randomTransformConfigWithDeprecatedSettings();
        doAnswer(ans -> {
            ActionListener<?> listener = ans.getArgument(1);
            listener.onFailure(new IllegalStateException("This is a getSequence failure."));
            return null;
        }).when(transformConfigManager).getTransformConfigurationForUpdate(any(), any());

        testMigration(originalConfig, updatedConfig -> {
            assertThat(updatedConfig, sameInstance(originalConfig));
            verify(auditor, only()).warning(
                eq(originalConfig.getId()),
                eq("Failed to auto-migrate Config. Please see Elasticsearch logs. Continuing with old config.")
            );
        });
    }

    public void testMigrateAndSaveWithUpdateError() throws InterruptedException {
        doAnswer(ans -> {
            ActionListener<Boolean> listener = ans.getArgument(2);
            listener.onFailure(new IllegalStateException("This is an update failure"));
            return null;
        }).when(transformConfigManager).updateTransformConfiguration(any(), any(), any());
        var originalConfig = randomTransformConfigWithDeprecatedSettings();
        doAnswer(ans -> {
            ActionListener<Tuple<TransformConfig, SeqNoPrimaryTermAndIndex>> listener = ans.getArgument(1);
            listener.onResponse(Tuple.tuple(originalConfig, mock()));
            return null;
        }).when(transformConfigManager).getTransformConfigurationForUpdate(any(), any());

        testMigration(originalConfig, updatedConfig -> {
            assertThat(updatedConfig, sameInstance(originalConfig));
            verify(auditor).info(eq(updatedConfig.getId()), eq(TransformMessages.MAX_PAGE_SEARCH_SIZE_MIGRATION));
            verify(auditor).warning(
                eq(originalConfig.getId()),
                eq("Failed to auto-migrate Config. Please see Elasticsearch logs. Continuing with old config.")
            );
        });
    }

    public void testMigrateWithMaxPageSearchSizeInBothLocations() {
        var settingsMaxPageSearchSize = 555;
        var settingsConfig = new SettingsConfig.Builder(SettingsConfig.EMPTY).setMaxPageSearchSize(settingsMaxPageSearchSize).build();

        var pivotConfigMaxPageSearchSize = 1234;
        var pivotConfig = new PivotConfig(
            GroupConfigTests.randomGroupConfig(TransformConfigVersion.CURRENT),
            AggregationConfigTests.randomAggregationConfig(),
            pivotConfigMaxPageSearchSize
        );

        var originalConfig = randomTransformConfigWithSettings(settingsConfig, pivotConfig, null);

        var updatedConfig = autoMigration.migrate(originalConfig);

        assertThat(updatedConfig, not(sameInstance(originalConfig)));
        assertThat(updatedConfig.getPivotConfig().getMaxPageSearchSize(), nullValue());
        assertThat(updatedConfig.getSettings().getMaxPageSearchSize(), equalTo(settingsMaxPageSearchSize));
        verify(auditor, only()).info(eq(updatedConfig.getId()), eq(TransformMessages.MAX_PAGE_SEARCH_SIZE_MIGRATION));
    }
}
