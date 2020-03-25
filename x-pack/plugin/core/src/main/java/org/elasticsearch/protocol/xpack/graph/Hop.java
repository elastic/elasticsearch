/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.graph;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A Hop represents one of potentially many stages in a graph exploration.
 * Each Hop identifies one or more fields in which it will attempt to find
 * terms that are significantly connected to the previous Hop. Each field is identified
 * using a {@link VertexRequest}
 *
 * <p>An example series of Hops on webserver logs would be:
 * <ol>
 * <li>an initial Hop to find
 * the top ten IPAddresses trying to access urls containing the word "admin"</li>
 * <li>a secondary Hop to see which other URLs those IPAddresses were trying to access</li>
 * </ol>
 *
 * <p>
 * Optionally, each hop can contain a "guiding query" that further limits the set of documents considered.
 * In our weblog example above we might choose to constrain the second hop to only look at log records that
 * had a response code of 404.
 * </p>
 * <p>
 * If absent, the list of {@link VertexRequest}s is inherited from the prior Hop's list to avoid repeating
 * the fields that will be examined at each stage.
 * </p>
 *
 */
public class Hop implements ToXContentFragment{
    final Hop parentHop;
    List<VertexRequest> vertices = null;
    QueryBuilder guidingQuery = null;

    public Hop(Hop parent) {
        this.parentHop = parent;
    }

    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {

        if (getEffectiveVertexRequests().size() == 0) {
            validationException = ValidateActions.addValidationError(GraphExploreRequest.NO_VERTICES_ERROR_MESSAGE, validationException);
        }
        return validationException;

    }

    public Hop getParentHop() {
        return parentHop;
    }

    void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalNamedWriteable(guidingQuery);
        if (vertices == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(vertices.size());
            for (VertexRequest vr : vertices) {
                vr.writeTo(out);
            }
        }
    }

    void readFrom(StreamInput in) throws IOException {
        guidingQuery = in.readOptionalNamedWriteable(QueryBuilder.class);
        int size = in.readVInt();
        if (size > 0) {
            vertices = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                VertexRequest vr = new VertexRequest();
                vr.readFrom(in);
                vertices.add(vr);
            }
        }
    }

    public QueryBuilder guidingQuery() {
        if (guidingQuery != null) {
            return guidingQuery;
        }
        return QueryBuilders.matchAllQuery();
    }

    /**
     * Add a field in which this {@link Hop} will look for terms that are highly linked to
     * previous hops and optionally the guiding query.
     *
     * @param fieldName a field in the chosen index
     */
    public VertexRequest addVertexRequest(String fieldName) {
        if (vertices == null) {
            vertices = new ArrayList<>();
        }
        VertexRequest vr = new VertexRequest();
        vr.fieldName(fieldName);
        vertices.add(vr);
        return vr;
    }

    /**
     * An optional parameter that focuses the exploration on documents that
     * match the given query.
     *
     * @param queryBuilder any query
     */
    public void guidingQuery(QueryBuilder queryBuilder) {
        guidingQuery = queryBuilder;
    }

    protected List<VertexRequest> getEffectiveVertexRequests() {
        if (vertices != null) {
            return vertices;
        }
        if (parentHop == null) {
            return Collections.emptyList();
        }
        // otherwise inherit settings from parent
        return parentHop.getEffectiveVertexRequests();
    }

    public int getNumberVertexRequests() {
        return getEffectiveVertexRequests().size();
    }

    public VertexRequest getVertexRequest(int requestNumber) {
        return getEffectiveVertexRequests().get(requestNumber);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (guidingQuery != null) {
            builder.field("query");
            guidingQuery.toXContent(builder, params);
        }
        if(vertices != null && vertices.size()>0) {
            builder.startArray("vertices");
            for (VertexRequest vertexRequest : vertices) {
                vertexRequest.toXContent(builder, params);
            }
            builder.endArray();
        }
        return builder;
    }
}
