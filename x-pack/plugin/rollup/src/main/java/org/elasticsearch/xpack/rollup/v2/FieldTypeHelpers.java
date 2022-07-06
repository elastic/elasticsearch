/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;

import java.io.IOException;
import java.util.Map;

public class FieldTypeHelpers {

    static class Builder {
        private final IndicesService indicesService;
        private final Map<String, Object> indexMapping;
        private final IndexMetadata indexMetadata;

        public Builder(final IndicesService indicesService, final Map<String, Object> indexMapping, final IndexMetadata indexMetadata) {
            this.indicesService = indicesService;
            this.indexMapping = indexMapping;
            this.indexMetadata = indexMetadata;
        }

        public FieldTypeHelper timeseriesHelper(final String timestampField) throws IOException {
            final MapperService mapperService = indicesService.createIndexMapperServiceForValidation(indexMetadata);
            final CompressedXContent sourceIndexCompressedXContent = new CompressedXContent(indexMapping);
            mapperService.merge(MapperService.SINGLE_MAPPING_NAME, sourceIndexCompressedXContent, MapperService.MergeReason.INDEX_TEMPLATE);
            return new TimeseriesFieldTypeHelper(mapperService, timestampField);
        }
    }
}
