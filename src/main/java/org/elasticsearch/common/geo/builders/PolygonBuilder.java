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

package org.elasticsearch.common.geo.builders;

import java.util.ArrayList;

import com.vividsolutions.jts.geom.Coordinate;

public class PolygonBuilder extends BasePolygonBuilder<PolygonBuilder> {

    public PolygonBuilder() {
        this(new ArrayList<Coordinate>(), Orientation.RIGHT);
    }

    public PolygonBuilder(Orientation orientation) {
        this(new ArrayList<Coordinate>(), orientation);
    }

    protected PolygonBuilder(ArrayList<Coordinate> points, Orientation orientation) {
        super(orientation);
        this.shell = new Ring<>(this, points);
    }

    @Override
    public PolygonBuilder close() {
        super.close();
        return this;
    }
}
