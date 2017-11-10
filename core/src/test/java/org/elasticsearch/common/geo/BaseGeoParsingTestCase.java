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
package org.elasticsearch.common.geo;

import com.vividsolutions.jts.geom.GeometryFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.common.geo.builders.ShapeBuilder.SPATIAL_CONTEXT;

/**
 * Created by nknize on 9/22/17.
 */
abstract class BaseGeoParsingTestCase extends ESTestCase {
    protected static final GeometryFactory GEOMETRY_FACTORY = SPATIAL_CONTEXT.getGeometryFactory();

    public abstract void testParsePoint() throws IOException;
    public abstract void testParseMultiPoint() throws IOException;
    public abstract void testParseLineString() throws IOException;
    public abstract void testParseMultiLineString() throws IOException;
    public abstract void testParsePolygon() throws IOException;
    public abstract void testParseMultiPolygon() throws IOException;
}
