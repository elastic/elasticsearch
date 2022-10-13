/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.legacygeo;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LegacyGeoPlugin extends Plugin implements ExtensiblePlugin {

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        if (ShapesAvailability.JTS_AVAILABLE && ShapesAvailability.SPATIAL4J_AVAILABLE) {
            return new ArrayList<>(GeoShapeType.getShapeWriteables());

        }
        return Collections.emptyList();
    }

}
