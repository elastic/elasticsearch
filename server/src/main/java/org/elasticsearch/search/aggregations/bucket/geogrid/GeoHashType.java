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

package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

public enum GeoHashType implements Writeable {
    GEOHASH(new GeoHashHandler()),
    PLUSCODE(new GeoPlusCodeHandler());

    public static final GeoHashType DEFAULT = GEOHASH;

    private GeoHashTypeProvider handler;

    GeoHashType(GeoHashTypeProvider handler) {
        this.handler = handler;
    }

    /**
     * Case-insensitive from string method.
     *
     * @param value String representation
     * @return The hash type
     */
    public static GeoHashType forString(String value) {
        return valueOf(value.toUpperCase(Locale.ROOT));
    }

    public static GeoHashType readFromStream(StreamInput in) throws IOException {

        // FIXME: update version after backport

        if(in.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            return in.readEnum(GeoHashType.class);
        } else {
            return DEFAULT;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

        // FIXME: update version after backport

        // To maintain binary compatibility, only include type after the given version
        if (out.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            out.writeEnum(this);
        } else if (this != DEFAULT) {
            throw new UnsupportedOperationException("Geo aggregation type [" + toString() +
                "] is not supported by the node version " + out.getVersion().toString());
        }
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public GeoHashTypeProvider getHandler() {
        return handler;
    }
}
