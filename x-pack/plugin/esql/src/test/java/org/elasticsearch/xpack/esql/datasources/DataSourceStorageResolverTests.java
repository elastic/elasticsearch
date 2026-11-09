/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

/**
 * Tests {@link DataSourceStorageResolver} on a registry with a stub scheme: what it passes to the provider factory, that the borrow of
 * the provider is given back, that a provider that is shared is never closed, and what it rejects.
 */
public class DataSourceStorageResolverTests extends ESTestCase {

    private static final String URI = "stub://bucket/dir/file.parquet";

    private static DataSource dataSource(Map<String, DataSourceSetting> settings) {
        return new DataSource("src", "stub", null, settings);
    }

    /** A provider that counts how often it was closed, and opens objects that remember the length they were given. */
    private static final class StubProvider implements StorageProvider {
        private final AtomicInteger closes;

        StubProvider(AtomicInteger closes) {
            this.closes = closes;
        }

        private static StorageObject object(StoragePath path, long length) {
            return new AbstractTestStorageObject() {
                @Override
                public InputStream newStream() {
                    return new ByteArrayInputStream(new byte[0]);
                }

                @Override
                public InputStream newStream(long position, long readLength) {
                    return new ByteArrayInputStream(new byte[0]);
                }

                @Override
                public long length() {
                    return length;
                }

                @Override
                public Instant lastModified() {
                    return Instant.EPOCH;
                }

                @Override
                public boolean exists() {
                    return true;
                }

                @Override
                public StoragePath path() {
                    return path;
                }
            };
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            return object(path, -1);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return object(path, length);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return object(path, length);
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageChildren listChildren(StoragePath prefix, int limit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(StoragePath path) {
            return true;
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of("stub");
        }

        @Override
        public void close() {
            closes.incrementAndGet();
        }
    }

    /** A factory that remembers the configurations it was asked to build providers for. */
    private static final class RecordingFactory implements StorageProviderFactory {
        final List<Map<String, Object>> configs = new ArrayList<>();
        final AtomicInteger closes = new AtomicInteger();

        @Override
        public StorageProvider create(Settings settings) {
            return new StubProvider(closes);
        }

        @Override
        public Configured<StorageProvider> createTrackingConsumedKeys(Settings settings, Map<String, Object> config) {
            configs.add(Map.copyOf(config));
            return new Configured<>(new StubProvider(closes), config.keySet(), "identity", "");
        }
    }

    public void testAFileIsOpenedWithTheSettingsOfTheDataSourceAndItsKnownLength() throws IOException {
        RecordingFactory factory = new RecordingFactory();
        DataSource dataSource = dataSource(
            Map.of(
                "auth",
                new DataSourceSetting("anonymous", false),
                // a region is a setting of a dataset, and is never inherited from a data source
                "region",
                new DataSourceSetting("eu-west-1", false),
                "secret_key",
                new DataSourceSetting("the-secret", true)
            )
        );
        try (StorageProviderRegistry registry = new StorageProviderRegistry(Settings.EMPTY)) {
            registry.registerFactory("stub", factory);
            DataSourceStorageResolver resolver = new DataSourceStorageResolver(registry, Settings.EMPTY, Map.of());

            try (BorrowedStorageObject object = resolver.open(dataSource, URI, OptionalLong.of(42))) {
                assertThat(object.length(), equalTo(42L));
                assertThat(object.path().toString(), equalTo(URI));
            }
            assertThat(factory.configs.size(), equalTo(1));
            Map<String, Object> config = factory.configs.get(0);
            assertThat(config.get("auth"), equalTo("anonymous"));
            assertThat(config.get("secret_key"), equalTo("the-secret"));
            assertThat(config, not(hasKey("region")));
            assertThat(
                "the context of the data source is for messages and never reaches a provider",
                config,
                not(hasKey("_dataset_context"))
            );

            // giving the borrow back returns the client to the pool: the next open with the same settings builds no new client
            try (BorrowedStorageObject object = resolver.open(dataSource, URI, OptionalLong.empty())) {
                assertThat(object.length(), equalTo(-1L));
            }
            assertThat(factory.configs.size(), equalTo(1));
            assertThat("a returned borrow must not shut the client down", factory.closes.get(), equalTo(0));
        }
        assertThat("the registry shuts the pooled client down when it closes", factory.closes.get(), equalTo(1));
    }

    public void testADataSourceWithoutSettingsUsesTheSharedDefaultProviderAndNeverClosesIt() throws IOException {
        AtomicInteger closes = new AtomicInteger();
        try (StorageProviderRegistry registry = new StorageProviderRegistry(Settings.EMPTY)) {
            registry.registerFactory("stub", StorageProviderFactory.noConfigKeys(() -> new StubProvider(closes)));
            DataSourceStorageResolver resolver = new DataSourceStorageResolver(registry, Settings.EMPTY, Map.of());
            try (BorrowedStorageObject object = resolver.open(dataSource(Map.of()), URI, OptionalLong.empty())) {
                assertThat(object.path().toString(), equalTo(URI));
            }
            assertThat("the default provider is shared with every query and must outlive the borrow", closes.get(), equalTo(0));
        }
        assertThat(closes.get(), equalTo(1));
    }

    public void testTheDataSourceIsLookedUpInTheProject() throws IOException {
        DataSource dataSource = dataSource(Map.of("auth", new DataSourceSetting("anonymous", false)));
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("src", dataSource)))
            .build();
        try (StorageProviderRegistry registry = new StorageProviderRegistry(Settings.EMPTY)) {
            registry.registerFactory("stub", new RecordingFactory());
            DataSourceStorageResolver resolver = new DataSourceStorageResolver(registry, Settings.EMPTY, Map.of());
            try (BorrowedStorageObject object = resolver.open(project, "src", URI, OptionalLong.of(7))) {
                assertThat(object.length(), equalTo(7L));
            }
            Exception e = expectThrows(ResourceNotFoundException.class, () -> resolver.open(project, "nope", URI, OptionalLong.empty()));
            assertThat(e.getMessage(), containsString("data source [nope] does not exist"));
            expectThrows(
                ResourceNotFoundException.class,
                () -> resolver.open(ProjectMetadata.builder(ProjectId.DEFAULT).build(), "src", URI, OptionalLong.empty())
            );
        }
    }

    public void testTheValidatorOfTheTypeRejectsAResourceThatItDoesNotRead() throws IOException {
        DataSourceValidator validator = new DataSourceValidator() {
            @Override
            public String type() {
                return "stub";
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
                if (resource.startsWith("stub://") == false) {
                    ValidationException e = new ValidationException();
                    e.addValidationError("unsupported scheme");
                    throw e;
                }
                return Map.of();
            }
        };
        try (StorageProviderRegistry registry = new StorageProviderRegistry(Settings.EMPTY)) {
            registry.registerFactory("stub", new RecordingFactory());
            DataSourceStorageResolver resolver = new DataSourceStorageResolver(registry, Settings.EMPTY, Map.of("stub", validator));
            DataSource dataSource = dataSource(Map.of("auth", new DataSourceSetting("anonymous", false)));
            resolver.open(dataSource, URI, OptionalLong.empty()).close();
            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> resolver.open(dataSource, "gs://bucket/file.parquet", OptionalLong.empty())
            );
            assertThat(e.getMessage(), containsString("[gs://bucket/file.parquet] is not a file that the data source [src] reads"));
            assertThat(e.getMessage(), containsString("unsupported scheme"));
        }
    }

    public void testTheSettingsOfADataSourceKeepASecretAsItIsStoredAndDropTheRegion() {
        DataSource dataSource = dataSource(
            Map.of(
                "endpoint",
                new DataSourceSetting("http://localhost:9111", false),
                "region",
                new DataSourceSetting("eu-west-1", false),
                "secret_key",
                new DataSourceSetting("the-secret", true)
            )
        );
        assertThat(
            DatasetRewriter.dataSourceSettings(dataSource),
            equalTo(Map.of("endpoint", "http://localhost:9111", "secret_key", "the-secret"))
        );
        assertThat(Set.copyOf(DataSourceStorageResolver.config(dataSource).keySet()), equalTo(Set.of("_datasource", "_dataset_context")));
        assertThat(DataSourceStorageResolver.config(dataSource(Map.of())).keySet(), equalTo(Set.of("_dataset_context")));
    }
}
