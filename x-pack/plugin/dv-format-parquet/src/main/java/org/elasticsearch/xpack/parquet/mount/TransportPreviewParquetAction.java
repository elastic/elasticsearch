/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.mount;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasources.DataSourceStorageResolver;
import org.elasticsearch.xpack.parquet.repository.ParquetFiles;

import java.nio.file.NoSuchFileException;

/** Runs a preview: it reads the footers on a thread that may block, and builds the answer from them. */
public class TransportPreviewParquetAction extends HandledTransportAction<ParquetMountRequest, ParquetMountResponse> {

    private final ParquetFiles parquetFiles;
    private final ProjectResolver projectResolver;
    private final ThreadPool threadPool;

    @Inject
    public TransportPreviewParquetAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ParquetFiles parquetFiles,
        ProjectResolver projectResolver,
        ThreadPool threadPool
    ) {
        super(
            PreviewParquetAction.NAME,
            transportService,
            actionFilters,
            ParquetMountRequest::new,
            threadPool.executor(ThreadPool.Names.GENERIC)
        );
        this.parquetFiles = parquetFiles;
        this.projectResolver = projectResolver;
        this.threadPool = threadPool;
    }

    /**
     * The ES|QL resolver that reads data sources is a component of the ES|QL plugin, which only Guice can reach. The transport actions
     * are built when the node starts, so the read-only engine finds the resolver there before the first mount, and on every node. The
     * plugin extends the ES|QL plugin, so the resolver is there on a node; it is optional only so that a test can load this plugin alone.
     */
    @Inject(optional = true)
    public void bindDataSources(DataSourceStorageResolver dataSources) {
        parquetFiles.bind(dataSources);
    }

    @Override
    protected void doExecute(Task task, ParquetMountRequest request, ActionListener<ParquetMountResponse> listener) {
        // reading a footer blocks on the repository, which a transport thread must not do
        threadPool.executor(ThreadPool.Names.GENERIC).execute(ActionRunnable.supply(listener, () -> {
            ParquetMountSpec spec = ParquetMountSpec.parse(request.body(), false);
            try {
                return new ParquetMountResponse(
                    ParquetMountAnalysis.analyze(spec, parquetFiles, projectResolver.getProjectId()).toPreview()
                );
            } catch (NoSuchFileException e) {
                throw new ResourceNotFoundException("the file [" + e.getFile() + "] does not exist");
            }
        }));
    }
}
