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

import org.elasticsearch.common.geo.ShapesAvailability;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;

/**
 * Register the shape builder prototypes with the {@link NamedWriteableRegistry}
 */
public class ShapeBuilderRegistry {

    @Inject
    public ShapeBuilderRegistry(NamedWriteableRegistry namedWriteableRegistry) {
        if (ShapesAvailability.JTS_AVAILABLE && ShapesAvailability.SPATIAL4J_AVAILABLE) {
            namedWriteableRegistry.registerPrototype(ShapeBuilder.class, PointBuilder.PROTOTYPE);
            namedWriteableRegistry.registerPrototype(ShapeBuilder.class, CircleBuilder.PROTOTYPE);
            namedWriteableRegistry.registerPrototype(ShapeBuilder.class, EnvelopeBuilder.PROTOTYPE);
            namedWriteableRegistry.registerPrototype(ShapeBuilder.class, MultiPointBuilder.PROTOTYPE);
            namedWriteableRegistry.registerPrototype(ShapeBuilder.class, LineStringBuilder.PROTOTYPE);
            namedWriteableRegistry.registerPrototype(ShapeBuilder.class, MultiLineStringBuilder.PROTOTYPE);
            namedWriteableRegistry.registerPrototype(ShapeBuilder.class, PolygonBuilder.PROTOTYPE);
            namedWriteableRegistry.registerPrototype(ShapeBuilder.class, MultiPolygonBuilder.PROTOTYPE);
            namedWriteableRegistry.registerPrototype(ShapeBuilder.class, GeometryCollectionBuilder.PROTOTYPE);
        }
    }
}
