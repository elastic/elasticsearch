/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;

import java.util.OptionalDouble;

public interface WriteLoadForecaster {
    WriteLoadForecaster DEFAULT = new DefaultWriteLoadForecaster();

    ProjectMetadata.Builder withWriteLoadForecastForWriteIndex(String dataStreamName, ProjectMetadata.Builder metadata);

    OptionalDouble getForecastedWriteLoad(IndexMetadata indexMetadata);

    void refreshLicense();

    class DefaultWriteLoadForecaster implements WriteLoadForecaster {
        @Override
        public ProjectMetadata.Builder withWriteLoadForecastForWriteIndex(String dataStreamName, ProjectMetadata.Builder metadata) {
            return metadata;
        }

        @Override
        public OptionalDouble getForecastedWriteLoad(IndexMetadata indexMetadata) {
            return OptionalDouble.empty();
        }

        @Override
        public void refreshLicense() {}
    }
}
