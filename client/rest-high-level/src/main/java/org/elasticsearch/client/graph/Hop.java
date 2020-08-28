/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.graph;

import org.elasticsearch.client.ValidationException;
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
 * had a reponse code of 404.
 * </p>
 * <p>
 * If absent, the list of {@link VertexRequest}s is inherited from the prior Hop's list to avoid repeating
 * the fields that will be examined at each stage.
 * </p>
 *
 */
public class Hop implements ToXContentFragment {
    final Hop parentHop;
    List<VertexRequest> vertices = null;
    QueryBuilder guidingQuery = null;

    public Hop(Hop parent) {
        this.parentHop = parent;
    }

    public void validate(ValidationException validationException) {
        if (getEffectiveVertexRequests().size() == 0) {
            validationException.addValidationError(GraphExploreRequest.NO_VERTICES_ERROR_MESSAGE);
        }
    }

    public Hop getParentHop() {
        return parentHop;
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
