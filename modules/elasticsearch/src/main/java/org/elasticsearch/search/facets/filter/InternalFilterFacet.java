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

package org.elasticsearch.search.facets.filter;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.builder.XContentBuilder;
import org.elasticsearch.search.facets.Facet;
import org.elasticsearch.search.facets.internal.InternalFacet;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class InternalFilterFacet implements FilterFacet, InternalFacet {

    private String name;

    private long count;

    private InternalFilterFacet() {

    }

    public InternalFilterFacet(String name, long count) {
        this.name = name;
        this.count = count;
    }

    @Override public Type type() {
        return Type.FILTER;
    }

    @Override public Type getType() {
        return type();
    }

    /**
     * The "logical" name of the facet.
     */
    public String name() {
        return name;
    }

    @Override public String getName() {
        return name();
    }

    /**
     * The count of the facet.
     */
    public long count() {
        return count;
    }

    /**
     * The count of the facet.
     */
    public long getCount() {
        return count;
    }

    @Override public Facet aggregate(Iterable<Facet> facets) {
        int count = 0;
        for (Facet facet : facets) {
            if (facet.name().equals(name)) {
                count += ((FilterFacet) facet).count();
            }
        }
        return new InternalFilterFacet(name, count);
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("_type", FilterFacetCollectorParser.NAME);
        builder.field("count", count);
        builder.endObject();
    }

    public static FilterFacet readFilterFacet(StreamInput in) throws IOException {
        InternalFilterFacet result = new InternalFilterFacet();
        result.readFrom(in);
        return result;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        name = in.readUTF();
        count = in.readVLong();
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeVLong(count);
    }
}