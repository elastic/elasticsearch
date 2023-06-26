/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;

import java.util.OptionalDouble;

public interface WriteLoadForecaster {
    WriteLoadForecaster DEFAULT = new DefaultWriteLoadForecaster();

    Metadata.Builder withWriteLoadForecastForWriteIndex(String dataStreamName, Metadata.Builder metadata);

    OptionalDouble getForecastedWriteLoad(IndexMetadata indexMetadata);

    class DefaultWriteLoadForecaster implements WriteLoadForecaster {
        @Override
        public Metadata.Builder withWriteLoadForecastForWriteIndex(String dataStreamName, Metadata.Builder metadata) {
            return metadata;
        }

        @Override
        public OptionalDouble getForecastedWriteLoad(IndexMetadata indexMetadata) {
            return OptionalDouble.empty();
        }
    }
}
