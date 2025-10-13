/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * The response object returned by the Explain Lifecycle API.
 * <p>
 * Since the API can be run over multiple indices the response provides a map of
 * index to the explanation of the lifecycle status for that index.
 */
public class ExplainLifecycleResponse extends ActionResponse implements ToXContentObject {

    public static final ParseField INDICES_FIELD = new ParseField("indices");

    private final Map<String, IndexLifecycleExplainResponse> indexResponses;

    public ExplainLifecycleResponse(Map<String, IndexLifecycleExplainResponse> indexResponses) {
        this.indexResponses = indexResponses;
    }

    /**
     * @return a map of the responses from each requested index. The maps key is
     *         the index name and the value is the
     *         {@link IndexLifecycleExplainResponse} describing the current
     *         lifecycle status of that index
     */
    public Map<String, IndexLifecycleExplainResponse> getIndexResponses() {
        return indexResponses;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(INDICES_FIELD.getPreferredName());
        for (IndexLifecycleExplainResponse indexResponse : indexResponses.values()) {
            builder.field(indexResponse.getIndex(), indexResponse);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    /**
     * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC we must remain able to write these responses until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(indexResponses.values());
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexResponses);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        ExplainLifecycleResponse other = (ExplainLifecycleResponse) obj;
        return Objects.equals(indexResponses, other.indexResponses);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

}
