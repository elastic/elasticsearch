/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.common.geo.GeoFormatterFactory;
import java.util.Collections;
import java.util.Map;

/**
 * An extension point for {@link Plugin} implementations to add geo specific formatters
 */
public interface GeoExtensionPlugin {

    /**
     * Returns the {@link GeoFormatterFactory.GeoFormatterEngine} add by this plugin.
     * <p>
     *  The key of the returned {@link Map} is the unique name for the formatter.
     */
    default Map<String, GeoFormatterFactory.GeoFormatterEngine> getGeoFormatters() {
        return Collections.emptyMap();
    }

}
