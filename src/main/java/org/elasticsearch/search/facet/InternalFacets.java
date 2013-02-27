/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.search.facet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 *
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
    @Override
    public Iterator<Facet> iterator() {
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
            facetsAsMap.put(facet.getName(), facet);
        }
        this.facetsAsMap = facetsAsMap;
        return facetsAsMap;
    }

    /**
     * Returns the facet by name already casted to the specified type.
     */
    @Override
    public <T extends Facet> T facet(Class<T> facetType, String name) {
        return facetType.cast(facet(name));
    }

    /**
     * A facet of the specified name.
     */
    @SuppressWarnings({"unchecked"})
    @Override
    public <T extends Facet> T facet(String name) {
        return (T) facetsAsMap().get(name);
    }

    static final class Fields {
        static final XContentBuilderString FACETS = new XContentBuilderString("facets");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.FACETS);
        for (Facet facet : facets) {
            ((InternalFacet) facet).toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    public static InternalFacets readFacets(StreamInput in) throws IOException {
        InternalFacets result = new InternalFacets();
        result.readFrom(in);
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        if (size == 0) {
            facets = ImmutableList.of();
            facetsAsMap = ImmutableMap.of();
        } else {
            facets = Lists.newArrayListWithCapacity(size);
            for (int i = 0; i < size; i++) {
                BytesReference type = in.readBytesReference();
                Facet facet = InternalFacet.Streams.stream(type).readFacet(in);
                facets.add(facet);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(facets.size());
        for (Facet facet : facets) {
            InternalFacet internalFacet = (InternalFacet) facet;
            out.writeBytesReference(internalFacet.streamType());
            internalFacet.writeTo(out);
        }
    }
}

