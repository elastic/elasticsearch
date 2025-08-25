/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

public class TimeSeriesDimensionsMetadataAccessTests extends ESTestCase {

    public void testCustomMetadataRoundtrip() {
        var dimensions = TimeSeriesDimensionsMetadataAccess.toCustomMetadata(List.of("dim1", "dim2"));
        assertEquals(Map.of("includes", "dim1,dim2"), dimensions);
    }

    public void testFromCustomMetadata() {
        var customMetadata = Map.of(
            TimeSeriesDimensionsMetadataAccess.TIME_SERIES_DIMENSIONS_METADATA_KEY,
            Map.of("includes", "dim1,dim2")
        );
        var dimensions = TimeSeriesDimensionsMetadataAccess.fromCustomMetadata(customMetadata);
        assertEquals(List.of("dim1", "dim2"), dimensions);
    }

    public void testTransferCustomMetadata() {
        var sourceMetadataBuilder = IndexMetadata.builder("source").settings(indexSettings(IndexVersion.current(), 1, 0));
        var targetMetadataBuilder = IndexMetadata.builder("target").settings(indexSettings(IndexVersion.current(), 1, 0));

        TimeSeriesDimensionsMetadataAccess.addToCustomMetadata(sourceMetadataBuilder::putCustom, List.of("dim1", "dim2"));
        TimeSeriesDimensionsMetadataAccess.transferCustomMetadata(sourceMetadataBuilder.build(), targetMetadataBuilder);

        assertEquals(List.of("dim1", "dim2"), TimeSeriesDimensionsMetadataAccess.fromCustomMetadata(targetMetadataBuilder.build()));
    }
}
