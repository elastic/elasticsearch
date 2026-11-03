/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

/**
 * Pins the ordering of {@link DatasetService#validatePutDataset}'s shadowed-secret checks around the
 * type validator. The pre-validator scan must win over a validator that rejects unknown keys (the
 * real file validators all do), or a dataset setting shadowing a parent secret reports the generic
 * {@code unknown setting [secret_key]} instead of the message that says what is actually wrong.
 * The post-validator scan covers the opposite validator shape: one that accepts and returns the key.
 */
public class DatasetServiceTests extends ESTestCase {

    private static final String SHADOW_MESSAGE = "shadows a secret data-source setting";

    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void startClusterService() {
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @After
    public void stopClusterService() {
        clusterService.close();
        terminate(threadPool);
    }

    /**
     * A validator with the same unknown-key contract as {@code FileDataSourceValidator}: any dataset
     * setting outside the accepted vocabulary is rejected with the generic unknown-setting error. The
     * real thing needs a full data-source configuration to build, which this test does not care about;
     * the ordering under test only depends on the validator rejecting unknown keys.
     */
    private static class UnknownKeyRejectingValidator implements DataSourceValidator {
        @Override
        public String type() {
            return "strict";
        }

        @Override
        public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
            return Map.of();
        }

        @Override
        public Map<String, Object> validateDataset(
            Map<String, DataSourceSetting> datasourceSettings,
            String resource,
            Map<String, Object> datasetSettings
        ) {
            for (String key : datasetSettings.keySet()) {
                if (key.equals("accepted") == false) {
                    ValidationException e = new ValidationException();
                    e.addValidationError("unknown setting [" + key + "]");
                    throw e;
                }
            }
            return new HashMap<>(datasetSettings);
        }
    }

    /** A validator that accepts everything and returns it unchanged, exercising the post-validator scan. */
    private static class AcceptAllValidator extends UnknownKeyRejectingValidator {
        @Override
        public Map<String, Object> validateDataset(
            Map<String, DataSourceSetting> datasourceSettings,
            String resource,
            Map<String, Object> datasetSettings
        ) {
            return new HashMap<>(datasetSettings);
        }
    }

    public void testShadowedSecretBeatsUnknownKeyRejection() {
        DatasetService service = new DatasetService(clusterService, Map.of("strict", new UnknownKeyRejectingValidator()));
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> service.validatePutDataset(project(), request(Map.of("secret_key", "override")))
        );
        assertThat(e.getMessage(), containsString(SHADOW_MESSAGE));
        assertThat(e.getMessage(), containsString("secret_key"));
        assertThat("the specific shadow message must win over the validator's generic one", e.getMessage(), not(containsString("unknown")));
    }

    public void testNonShadowSettingsStillReachTheValidator() {
        DatasetService service = new DatasetService(clusterService, Map.of("strict", new UnknownKeyRejectingValidator()));
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> service.validatePutDataset(project(), request(Map.of("bogus", 1)))
        );
        assertThat(e.getMessage(), containsString("unknown setting [bogus]"));
        assertThat(e.getMessage(), not(containsString(SHADOW_MESSAGE)));
    }

    /** A non-secret parent setting is the validator's to judge, not the shadow scan's. */
    public void testShadowingANonSecretParentSettingIsNotBlocked() {
        DatasetService service = new DatasetService(clusterService, Map.of("strict", new UnknownKeyRejectingValidator()));
        Dataset dataset = service.validatePutDataset(project(), request(Map.of("accepted", "value")));
        assertEquals("value", dataset.settings().get("accepted"));
    }

    public void testShadowedSecretRejectedEvenWhenTheValidatorAcceptsIt() {
        DatasetService service = new DatasetService(clusterService, Map.of("strict", new AcceptAllValidator()));
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> service.validatePutDataset(project(), request(Map.of("secret_key", "override")))
        );
        assertThat(e.getMessage(), containsString(SHADOW_MESSAGE));
    }

    /**
     * The parent carries one secret setting ({@code secret_key}) and one non-secret setting
     * ({@code accepted}) that also happens to be in the validator's dataset vocabulary.
     */
    private static ProjectMetadata project() {
        DataSource parent = new DataSource(
            "parent",
            "strict",
            null,
            Map.of("secret_key", new DataSourceSetting("s3cr3t", true), "accepted", new DataSourceSetting("parent-value", false))
        );
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("parent", parent)))
            .build();
    }

    private static PutDatasetAction.Request request(Map<String, Object> rawSettings) {
        return new PutDatasetAction.Request(
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(30),
            "ds1",
            "parent",
            "s3://bucket/*.csv",
            null,
            rawSettings
        );
    }
}
