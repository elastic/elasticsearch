/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.utils;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.transform.TransformField;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class DocumentConversionUtils {

    public static IndexRequest convertDocumentToIndexRequest(Map<String, Object> document,
                                                             String destinationIndex,
                                                             String destinationPipeline) {
        String id = (String) document.get(TransformField.DOCUMENT_ID_FIELD);
        if (id == null) {
            throw new RuntimeException("Expected a document id but got null.");
        }

        XContentBuilder builder = skipInternalFields(document);
        IndexRequest request = new IndexRequest(destinationIndex).source(builder).id(id);
        if (destinationPipeline != null) {
            request.setPipeline(destinationPipeline);
        }
        return request;
    }

    private static XContentBuilder skipInternalFields(Map<String, Object> document) {
        XContentBuilder builder;
        try {
            builder = jsonBuilder();
            builder.startObject();
            for (Map.Entry<String, ?> value : document.entrySet()) {
                // skip all internal fields
                if (value.getKey().startsWith("_") == false) {
                    builder.field(value.getKey(), value.getValue());
                }
            }
            builder.endObject();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return builder;
    }

    private DocumentConversionUtils() {}
}
