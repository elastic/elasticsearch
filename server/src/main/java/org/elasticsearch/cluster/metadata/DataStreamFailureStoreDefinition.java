/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.RoutingFieldMapper;

import java.io.IOException;

/**
 * A utility class that contains the mappings and settings logic for failure store indices that are a part of data streams.
 */
public class DataStreamFailureStoreDefinition {

    public static final CompressedXContent DATA_STREAM_FAILURE_STORE_MAPPING;

    static {
        try {
            /*
             * The data stream failure store mapping. The JSON content is as follows:
             * {
             *   "_doc": {
             *     "dynamic": false,
             *     "_routing": {
             *       "required": false
             *     },
             *     "properties": {
             *       "@timestamp": {
             *         "type": "date",
             *         "ignore_malformed": false
             *       },
             *       "document": {
             *         "properties": {
             *           "id": {
             *             "type": "keyword"
             *           },
             *           "routing": {
             *             "type": "keyword"
             *           },
             *           "index": {
             *             "type": "keyword"
             *           }
             *         }
             *       },
             *       "error": {
             *         "properties": {
             *           "message": {
             *              "type": "wildcard"
             *           },
             *           "stack_trace": {
             *              "type": "text"
             *           },
             *           "type": {
             *              "type": "keyword"
             *           },
             *           "pipeline": {
             *              "type": "keyword"
             *           },
             *           "pipeline_trace": {
             *              "type": "keyword"
             *           },
             *           "processor": {
             *              "type": "keyword"
             *           }
             *         }
             *       }
             *     }
             *   }
             * }
             */
            DATA_STREAM_FAILURE_STORE_MAPPING = new CompressedXContent(
                (builder, params) -> builder.startObject(MapperService.SINGLE_MAPPING_NAME)
                    .field("dynamic", false)
                    .startObject(RoutingFieldMapper.NAME)
                    .field("required", false)
                    .endObject()
                    .startObject("properties")
                    .startObject(MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD)
                    .field("type", DateFieldMapper.CONTENT_TYPE)
                    .field("ignore_malformed", false)
                    .endObject()
                    .startObject("document")
                    .startObject("properties")
                    // document.source is unmapped so that it can be persisted in source only without worrying that the document might cause
                    // a mapping error
                    .startObject("id")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("routing")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("index")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    .startObject("error")
                    .startObject("properties")
                    .startObject("message")
                    .field("type", "wildcard")
                    .endObject()
                    .startObject("stack_trace")
                    .field("type", "text")
                    .endObject()
                    .startObject("type")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("pipeline")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("pipeline_trace")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("processor")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            );
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
