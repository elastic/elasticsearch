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

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.text.ParseException;

/**
 * Geometry serializer/deserializer
 */
public interface GeometryFormat<ParsedFormat> {

    /**
     * The name of the format, for example 'wkt'.
     */
    String name();

    /**
     * Parser JSON representation of a geometry
     */
    ParsedFormat fromXContent(XContentParser parser) throws IOException, ParseException;

    /**
     * Serializes the geometry into its JSON representation
     */
    XContentBuilder toXContent(ParsedFormat geometry, XContentBuilder builder, ToXContent.Params params) throws IOException;

    /**
     * Serializes the geometry into a standard Java object.
     *
     * For example, the GeoJson format returns the geometry as a map, while WKT returns a string.
     */
    Object toXContentAsObject(ParsedFormat geometry);
}
