/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.TimeSeriesIndexCreationWindowLocator;
import org.elasticsearch.datastreams.DataStreamsPlugin;

import java.time.Instant;

/**
 * Uses lifecycle logic to determine how far in the past a time series backing index can be created before it would either be deleted or set
 * read-only in some way by the DLM service.
 */
public class DLMTimeSeriesIndexCreationWindowLocator
    implements
    TimeSeriesIndexCreationWindowLocator {

    public DLMTimeSeriesIndexCreationWindowLocator() {
        throw new IllegalStateException("Must construct this via the plugin-based constructor");
    }

    public DLMTimeSeriesIndexCreationWindowLocator(DataStreamsPlugin dataStreamsPlugin) {
        this.dataStreamsPlugin = dataStreamsPlugin;
    }

    private final DataStreamsPlugin dataStreamsPlugin;

    @Override
    public Instant locateCreateWindow(DataStream dataStream, ProjectMetadata projectMetadata) {
        var dataStreamLifecycleService = dataStreamsPlugin.getDataStreamLifecycleService();
        if (dataStreamLifecycleService == null) {
            throw new IllegalStateException("Could not obtain data stream lifecycle service because it has not yet been created");
        }
        return dataStreamLifecycleService.timeseriesStartWindow(dataStream, projectMetadata);
    }

}
