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

package org.elasticsearch.search.facets;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.util.io.Streamable;
import org.elasticsearch.util.json.JsonBuilder;
import org.elasticsearch.util.json.ToJson;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Lists.*;
import static org.elasticsearch.search.facets.CountFacet.*;

/**
 * @author kimchy (Shay Banon)
 */
public class Facets implements Streamable, ToJson {

    private final List<Facet> EMPTY = ImmutableList.of();

    private List<Facet> facets;

    private Facets() {

    }

    public Facets(List<Facet> facets) {
        this.facets = facets;
    }

    public List<Facet> facets() {
        return facets;
    }

    public CountFacet countFacet(String name) {
        return (CountFacet) facet(name);
    }

    public Facet facet(String name) {
        if (facets == null) {
            return null;
        }
        for (Facet facet : facets) {
            if (facet.name().equals(name)) {
                return facet;
            }
        }
        return null;
    }

    @Override public void toJson(JsonBuilder builder, Params params) throws IOException {
        builder.startObject("facets");
        for (Facet facet : facets) {
            facet.toJson(builder, params);
        }
        builder.endObject();
    }

    public static Facets readFacets(DataInput in) throws IOException, ClassNotFoundException {
        Facets result = new Facets();
        result.readFrom(in);
        return result;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        if (size == 0) {
            facets = EMPTY;
        } else {
            facets = newArrayListWithCapacity(size);
            for (int i = 0; i < size; i++) {
                byte id = in.readByte();
                if (id == Facet.Type.COUNT.id()) {
                    facets.add(readCountFacet(in));
                } else {
                    throw new IOException("Can't handle facet type with id [" + id + "]");
                }
            }
        }
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.write(facets.size());
        for (Facet facet : facets) {
            out.write(facet.type().id());
            facet.writeTo(out);
        }
    }
}
