/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetPipelineResponse extends ActionResponse implements ToXContentObject {

    private final List<PipelineConfiguration> pipelines;
    private final boolean summary;

    public GetPipelineResponse(List<PipelineConfiguration> pipelines, boolean summary) {
        this.pipelines = pipelines;
        this.summary = summary;
    }

    public GetPipelineResponse(List<PipelineConfiguration> pipelines) {
        this(pipelines, false);
    }

    /**
     * Get the list of pipelines that were a part of this response.
     * The pipeline id can be obtained using getId on the PipelineConfiguration object.
     * @return A list of {@link PipelineConfiguration} objects.
     */
    public List<PipelineConfiguration> pipelines() {
        return Collections.unmodifiableList(pipelines);
    }

    /**
     * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC we must remain able to read these requests until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(pipelines);
        out.writeBoolean(summary);
    }

    public boolean isFound() {
        return pipelines.isEmpty() == false;
    }

    public boolean isSummary() {
        return summary;
    }

    public RestStatus status() {
        return isFound() ? RestStatus.OK : RestStatus.NOT_FOUND;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (PipelineConfiguration pipeline : pipelines) {
            builder.field(pipeline.getId(), summary ? Map.of() : pipeline.getConfig());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        } else if (other instanceof GetPipelineResponse otherResponse) {
            if (pipelines == null) {
                return otherResponse.pipelines == null;
            } else {
                // We need a map here because order does not matter for equality
                Map<String, PipelineConfiguration> otherPipelineMap = new HashMap<>();
                for (PipelineConfiguration pipeline : otherResponse.pipelines) {
                    otherPipelineMap.put(pipeline.getId(), pipeline);
                }
                for (PipelineConfiguration pipeline : pipelines) {
                    PipelineConfiguration otherPipeline = otherPipelineMap.get(pipeline.getId());
                    if (pipeline.equals(otherPipeline) == false) {
                        return false;
                    }
                    otherPipelineMap.remove(pipeline.getId());
                }
                if (otherPipelineMap.isEmpty() == false) {
                    return false;
                }
                return true;
            }
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public int hashCode() {
        int result = 1;
        for (PipelineConfiguration pipeline : pipelines) {
            // We only take the sum here to ensure that the order does not matter.
            result += (pipeline == null ? 0 : pipeline.hashCode());
        }
        return result;
    }

}
