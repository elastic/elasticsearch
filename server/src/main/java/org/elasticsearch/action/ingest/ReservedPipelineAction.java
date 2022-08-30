/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * This Action is the reserved state save version of RestPutPipelineAction/RestDeletePipelineAction
 * <p>
 * It is used by the ReservedClusterStateService to add/update or remove ingest pipelines. Typical usage
 * for this action is in the context of file based state.
 */
public class ReservedPipelineAction implements ReservedClusterStateHandler<List<PutPipelineRequest>> {
    public static final String NAME = "ingest_pipelines";

    private final IngestService ingestService;

    /**
     * Creates a ReservedPipelineAction
     *
     * @param ingestService requires {@link IngestService} for storing/deleting the pipelines
     */
    public ReservedPipelineAction(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @SuppressWarnings("unchecked")
    public Collection<PutPipelineRequest> prepare(Object input) {
        List<PutPipelineRequest> pipelines = (List<PutPipelineRequest>) input;

        var exceptions = new ArrayList<String>();
        for (var pipeline : pipelines) {
            var exception = pipeline.validate();
            if (exception != null) {
                exceptions.add(exception.getMessage());
            }
        }

        if (exceptions.isEmpty() == false) {
            throw new IllegalStateException(String.join(", ", exceptions));
        }

        return pipelines;
    }

    @Override
    public TransformState transform(Object source, TransformState prevState) throws Exception {
        prepare(source);
        return prevState;
    }

    @Override
    public List<PutPipelineRequest> fromXContent(XContentParser parser) throws IOException {
        List<PutPipelineRequest> result = new ArrayList<>();

        Map<String, ?> source = parser.map();

        for (String id : source.keySet()) {
            @SuppressWarnings("unchecked")
            Map<String, ?> content = (Map<String, ?>) source.get(id);
            try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
                builder.map(content);
                result.add(new PutPipelineRequest(id, BytesReference.bytes(builder), XContentType.JSON));
            } catch (Exception e) {
                throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
            }
        }

        return result;
    }
}
