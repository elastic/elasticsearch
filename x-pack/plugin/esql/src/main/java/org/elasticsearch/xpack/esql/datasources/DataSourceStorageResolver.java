/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.datasources.cache.StorageProviderCache;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;

/**
 * Opens a file that a data source reads, outside of a query: the way for code that is not an ES|QL query, such as a plugin that
 * serves the file in another way, to read through the same definition of a data source that a query uses. It resolves the data
 * source, checks that the URI is a resource that the data source type reads (with the validator that checks a dataset), builds the
 * configuration under which the registry creates the storage provider, borrows the provider from the registry's pool, and returns the
 * file together with the borrow.
 *
 * <p>The caller must close the returned {@link BorrowedStorageObject} when it is done with the file. A file that lives for hours
 * (a segment of an index) holds the borrow for hours: the registry shuts an idle provider down after a while, but never one that is
 * borrowed.
 *
 * <p>Reads are blocking, so the caller must read on a thread that may block.
 */
public final class DataSourceStorageResolver {

    private final StorageProviderRegistry registry;
    private final Settings settings;
    private final Map<String, DataSourceValidator> validators;

    /**
     * @param registry   the registry of the storage providers of the node
     * @param settings   the settings of the node
     * @param validators the validators of the data source types, by type
     */
    public DataSourceStorageResolver(StorageProviderRegistry registry, Settings settings, Map<String, DataSourceValidator> validators) {
        this.registry = registry;
        this.settings = settings;
        this.validators = Map.copyOf(validators);
    }

    /**
     * Opens a file of the data source with the given name, which is looked up in the project.
     *
     * @param knownLength the length of the file when the caller knows it, which makes opening it a request-free call
     * @throws ResourceNotFoundException if the project has no such data source
     * @throws IllegalArgumentException  if the URI is not a resource that the type of the data source reads, or if no provider reads it
     */
    public BorrowedStorageObject open(ProjectMetadata project, String dataSourceName, String uri, OptionalLong knownLength) {
        DataSource dataSource = DataSourceMetadata.get(project).get(dataSourceName);
        if (dataSource == null) {
            throw new ResourceNotFoundException("data source [{}] does not exist", dataSourceName);
        }
        return open(dataSource, uri, knownLength);
    }

    /** Like {@link #open(ProjectMetadata, String, String, OptionalLong)} for a data source that the caller already has. */
    public BorrowedStorageObject open(DataSource dataSource, String uri, OptionalLong knownLength) {
        DataSourceValidator validator = validators.get(dataSource.type());
        if (validator != null) {
            try {
                // the check that a dataset gets at PUT: the scheme of the resource, the endpoint allow-list, the format
                validator.validateDataset(dataSource.settings().asMap(), uri, Map.of());
            } catch (ValidationException e) {
                throw new IllegalArgumentException(
                    "[" + uri + "] is not a file that the data source [" + dataSource.name() + "] reads: " + e.getMessage(),
                    e
                );
            }
        }
        StoragePath path = StoragePath.of(uri);
        StorageProvider provider = registry.createProvider(path.scheme().toLowerCase(Locale.ROOT), settings, config(dataSource));
        try {
            StorageObject object = knownLength.isPresent() ? provider.newObject(path, knownLength.getAsLong()) : provider.newObject(path);
            return new BorrowedStorageObject(object, provider);
        } catch (RuntimeException e) {
            StorageProviderCache.closeLease(provider);
            throw e;
        }
    }

    /**
     * The configuration under which a data source is read when no dataset is involved: its settings under the key that the registry
     * flattens (see {@link DatasetRewriter#dataSourceSettings}), and the context that annotates failures. {@link DatasetRewriter}
     * builds the same two parts for a dataset, so the two cannot disagree about what a data source means.
     */
    static Map<String, Object> config(DataSource dataSource) {
        Map<String, Object> config = new HashMap<>();
        if (dataSource.settings().isEmpty() == false) {
            config.put(ExternalSourceResolver.DATASOURCE_CONFIG_KEY, DatasetRewriter.dataSourceSettings(dataSource));
        }
        config.put(ExternalSourceResolver.DATASET_CONTEXT_KEY, Map.of("datasource", dataSource.name(), "type", dataSource.type()));
        return config;
    }
}
