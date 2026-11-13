/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
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

    /** A disabled data source takes no new dataset, and no existing one moving onto it. */
    public void testRepointingAnExistingDatasetToADisabledDataSourceIsRejected() {
        DatasetService service = new DatasetService(clusterService, Map.of("strict", new AcceptAllValidator()));
        ProjectMetadata project = projectWithDisabledParent(existingDataset("ds1", "parent"), null);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> service.validatePutDataset(project, requestOn("off", "ds1"))
        );
        assertThat(e.getMessage(), containsString("disabled data source [off]"));
    }

    /** Replacing a dataset that already sits on a now-disabled data source, without moving it, stays allowed. */
    public void testReplacingADatasetOnItsOwnDisabledDataSourceIsAllowedAndKeepsItsFlag() {
        DatasetService service = new DatasetService(clusterService, Map.of("strict", new AcceptAllValidator()));
        ProjectMetadata project = projectWithDisabledParent(existingDataset("ds1", "off").withEnabled(false), null);

        Dataset replaced = service.validatePutDataset(project, requestOn("off", "ds1"));
        assertThat(replaced.dataSource().getName(), equalTo("off"));
        assertThat(replaced.enabled(), is(false));
    }

    /** A name taken by an index is a conflict whatever the data source's state, so it is reported as one. */
    public void testNameConflictWithAnIndexIsReportedBeforeADisabledDataSource() {
        DatasetService service = new DatasetService(clusterService, Map.of("strict", new AcceptAllValidator()));
        IndexMetadata.Builder index = IndexMetadata.builder("ds1").settings(indexSettings(IndexVersion.current(), 1, 0));
        ProjectMetadata project = projectWithDisabledParent(null, index);

        expectThrows(ResourceAlreadyExistsException.class, () -> service.validatePutDataset(project, requestOn("off", "ds1")));
    }

    /**
     * A node that predates the flag reads every dataset as enabled, so while one is in the cluster the toggle is refused:
     * the call must not report success for a disable that part of the cluster would ignore.
     */
    public void testToggleIsRefusedUntilAllNodesSupportTheFlag() {
        DatasetService service = new DatasetService(clusterService, Map.of("strict", new AcceptAllValidator()));
        ClusterServiceUtils.setState(
            clusterService,
            ClusterState.builder(clusterService.state())
                .putCompatibilityVersions(
                    "old-node",
                    TransportVersionUtils.randomVersionNotSupporting(DataSource.ESQL_DATASOURCE_DATASET_ENABLED),
                    Map.of()
                )
        );

        var future = new PlainActionFuture<AcknowledgedResponse>();
        service.setDatasetsEnabled(
            ProjectId.DEFAULT,
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(30),
            List.of("ds1"),
            false,
            future
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> future.actionGet(10, TimeUnit.SECONDS));
        assertThat(e.getMessage(), containsString("until all nodes in the cluster are upgraded"));
    }

    private static Dataset existingDataset(String name, String dataSource) {
        return new Dataset(name, new DataSourceReference(dataSource), "s3://bucket/old/*.csv", null, Map.of());
    }

    /** {@link #project()}'s data sources plus a disabled one named {@code off}, with an optional dataset and index. */
    private static ProjectMetadata projectWithDisabledParent(Dataset dataset, IndexMetadata.Builder index) {
        ProjectMetadata base = project();
        DataSource off = new DataSource("off", "strict", null, Map.of(), false);
        Map<String, DataSource> sources = new HashMap<>(DataSourceMetadata.get(base).dataSources());
        sources.put("off", off);
        ProjectMetadata.Builder builder = ProjectMetadata.builder(base).putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(sources));
        if (dataset != null) {
            builder.datasets(Map.of(dataset.name(), dataset));
        }
        if (index != null) {
            builder.put(index);
        }
        return builder.build();
    }

    private static PutDatasetAction.Request requestOn(String dataSource, String name) {
        return new PutDatasetAction.Request(
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(30),
            name,
            dataSource,
            "s3://bucket/*.csv",
            null,
            Map.of()
        );
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
