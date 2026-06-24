/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.index.IndexMode;

import java.time.Instant;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * References plugin code to determine how far in the past a time series data stream can create a backing index without that backing index
 * being interfered with by some external logic (such as a lifecycle).
 */
public interface TimeSeriesIndexCreationWindowLocator {

    /**
     * Determines the cut-off time for a time series data stream to create a new backing index.
     *
     * @param dataStream      The data stream to surface the start window for, will always be a time-series data stream
     * @param projectMetadata The project metadata this data stream is attached to
     * @param nowSupplier Supplier for the current timestamp in epoch millis
     * @return The earliest instant this data stream could create a backing index
     */
    Instant locateCreateWindow(DataStream dataStream, ProjectMetadata projectMetadata, LongSupplier nowSupplier);

    /**
     * Combines multiple locators together to determine the start window for creating a time series data stream backing index. All locators
     * are checked for a start window, in no defined order, with the most conservative start window chosen.
     */
    class CompositeLocator implements TimeSeriesIndexCreationWindowLocator {

        private final List<? extends TimeSeriesIndexCreationWindowLocator> locators;

        public CompositeLocator(List<? extends TimeSeriesIndexCreationWindowLocator> locators) {
            this.locators = locators;
        }

        /**
         * Determines the cut-off time for a time series data stream to create a new backing index.
         *
         * @param dataStream      The data stream to surface the start window for, must be a time-series data stream
         * @param projectMetadata The project metadata to search for the data stream
         * @param nowSupplier Supplier for the current timestamp in epoch millis
         * @return The earliest instant this data stream could create a backing index
         */
        public Instant locateCreateWindow(DataStream dataStream, ProjectMetadata projectMetadata, LongSupplier nowSupplier) {
            assert IndexMode.TIME_SERIES == dataStream.getIndexMode()
                : "Only time series data streams should be checked for a starting window";
            Instant currentWindow = Instant.MIN;
            for (TimeSeriesIndexCreationWindowLocator locator : locators) {
                Instant startWindow = locator.locateCreateWindow(dataStream, projectMetadata, nowSupplier);
                if (startWindow != null && startWindow.isAfter(currentWindow)) {
                    currentWindow = startWindow;
                }
            }
            return currentWindow;
        }
    }

    static TimeSeriesIndexCreationWindowLocator noOp() {
        return NoOpLocator.INSTANCE;
    }

    enum NoOpLocator implements TimeSeriesIndexCreationWindowLocator {
        INSTANCE;

        @Override
        public Instant locateCreateWindow(DataStream dataStream, ProjectMetadata projectMetadata, LongSupplier nowSupplier) {
            return Instant.MIN;
        }
    }

}
