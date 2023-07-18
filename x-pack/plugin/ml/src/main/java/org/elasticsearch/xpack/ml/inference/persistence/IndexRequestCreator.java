/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.persistence;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * A helper class for creating index requests that handles determining whether the model being indexed can be overwritten if it already
 * exists.
 */
public class IndexRequestCreator {
    private static final ToXContent.Params FOR_INTERNAL_STORAGE_PARAMS = new ToXContent.MapParams(
        Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true")
    );

    /**
     * This is package private so the tests can access it
     */
    static final Set<String> PRE_PACKAGED_MODELS = Set.of(".elser_model_1");

    public static IndexRequest create(String modelId, String docId, String index, ToXContentObject body) {

        return create(new IndexRequest(index), docId, body, getOperation(modelId));
    }

    public static IndexRequest create(String modelId, String docId, ToXContentObject body) {
        return create(new IndexRequest(), docId, body, getOperation(modelId));
    }

    private static DocWriteRequest.OpType getOperation(String modelId) {
        // allow pre-packaged models to be overwritten
        return PRE_PACKAGED_MODELS.contains(modelId) ? DocWriteRequest.OpType.INDEX : DocWriteRequest.OpType.CREATE;
    }

    private static IndexRequest create(IndexRequest request, String docId, ToXContentObject body, DocWriteRequest.OpType operation) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = body.toXContent(builder, FOR_INTERNAL_STORAGE_PARAMS);

            return request.opType(operation).id(docId).source(source);
        } catch (IOException ex) {
            // This should never happen. If we were able to deserialize the object (from Native or REST) and then fail to serialize it again
            // that is not the users fault. We did something wrong and should throw.
            throw ExceptionsHelper.serverError("Unexpected serialization exception for [" + docId + "]", ex);
        }
    }

    private IndexRequestCreator() {}
}
