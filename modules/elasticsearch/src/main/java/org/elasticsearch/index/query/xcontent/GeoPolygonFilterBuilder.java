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

package org.elasticsearch.index.query.xcontent;

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.lucene.geo.GeoHashUtils;
import org.elasticsearch.common.lucene.geo.GeoPolygonFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * @author kimchy (shay.banon)
 */
public class GeoPolygonFilterBuilder extends BaseFilterBuilder {

    private final String name;

    private final List<GeoPolygonFilter.Point> points = Lists.newArrayList();

    private String filterName;

    public GeoPolygonFilterBuilder(String name) {
        this.name = name;
    }

    public GeoPolygonFilterBuilder addPoint(double lat, double lon) {
        points.add(new GeoPolygonFilter.Point(lat, lon));
        return this;
    }

    public GeoPolygonFilterBuilder addPoint(String geohash) {
        double[] values = GeoHashUtils.decode(geohash);
        return addPoint(values[0], values[1]);
    }

    public GeoPolygonFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    @Override protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(GeoPolygonFilterParser.NAME);

        builder.startObject(name);
        builder.startArray("points");
        for (GeoPolygonFilter.Point point : points) {
            builder.startArray().value(point.lat).value(point.lon).endArray();
        }
        builder.endArray();
        builder.endObject();

        if (filterName != null) {
            builder.field("_name", filterName);
        }

        builder.endObject();
    }
}
