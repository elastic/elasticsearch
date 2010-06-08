/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.facets.internal;

import org.elasticsearch.search.facets.Facet;
import org.elasticsearch.search.facets.Facets;
import org.elasticsearch.search.facets.histogram.InternalHistogramFacet;
import org.elasticsearch.search.facets.query.InternalQueryFacet;
import org.elasticsearch.search.facets.statistical.InternalStatisticalFacet;
import org.elasticsearch.search.facets.terms.InternalTermsFacet;
import org.elasticsearch.util.collect.ImmutableList;
import org.elasticsearch.util.collect.ImmutableMap;
import org.elasticsearch.util.collect.Lists;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;
import org.elasticsearch.util.io.stream.Streamable;
import org.elasticsearch.util.xcontent.ToXContent;
import org.elasticsearch.util.xcontent.builder.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.util.collect.Maps.*;

/**
 * @author kimchy (shay.banon)
 */
public class InternalFacets implements Facets, Streamable, ToXContent, Iterable<Facet> {

    private List<Facet> facets = ImmutableList.of();

    private Map<String, Facet> facetsAsMap;

    private InternalFacets() {

    }

    /**
     * Constructs a new facets.
     */
    public InternalFacets(List<Facet> facets) {
        this.facets = facets;
    }

    /**
     * Iterates over the {@link Facet}s.
     */
    @Override public Iterator<Facet> iterator() {
        return facets.iterator();
    }

    /**
     * The list of {@link Facet}s.
     */
    public List<Facet> facets() {
        return facets;
    }

    /**
     * Returns the {@link Facet}s keyed by map.
     */
    public Map<String, Facet> getFacets() {
        return facetsAsMap();
    }

    /**
     * Returns the {@link Facet}s keyed by map.
     */
    public Map<String, Facet> facetsAsMap() {
        if (facetsAsMap != null) {
            return facetsAsMap;
        }
        Map<String, Facet> facetsAsMap = newHashMap();
        for (Facet facet : facets) {
            facetsAsMap.put(facet.name(), facet);
        }
        this.facetsAsMap = facetsAsMap;
        return facetsAsMap;
    }

    /**
     * Returns the facet by name already casted to the specified type.
     */
    public <T extends Facet> T facet(Class<T> facetType, String name) {
        return facetType.cast(facet(name));
    }

    /**
     * A facet of the specified name.
     */
    public Facet facet(String name) {
        return facetsAsMap().get(name);
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("facets");
        for (Facet facet : facets) {
            ((InternalFacet) facet).toXContent(builder, params);
        }
        builder.endObject();
    }

    public static InternalFacets readFacets(StreamInput in) throws IOException {
        InternalFacets result = new InternalFacets();
        result.readFrom(in);
        return result;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        if (size == 0) {
            facets = ImmutableList.of();
            facetsAsMap = ImmutableMap.of();
        } else {
            facets = Lists.newArrayListWithCapacity(size);
            for (int i = 0; i < size; i++) {
                int id = in.readVInt();
                if (id == Facet.Type.TERMS.id()) {
                    facets.add(InternalTermsFacet.readTermsFacet(in));
                } else if (id == Facet.Type.QUERY.id()) {
                    facets.add(InternalQueryFacet.readCountFacet(in));
                } else if (id == Facet.Type.STATISTICAL.id()) {
                    facets.add(InternalStatisticalFacet.readStatisticalFacet(in));
                } else if (id == Facet.Type.HISTOGRAM.id()) {
                    facets.add(InternalHistogramFacet.readHistogramFacet(in));
                } else {
                    throw new IOException("Can't handle facet type with id [" + id + "]");
                }
            }
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(facets.size());
        for (Facet facet : facets) {
            out.writeVInt(facet.type().id());
            ((InternalFacet) facet).writeTo(out);
        }
    }
}

