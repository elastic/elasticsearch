/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

public class ShapesAvailability {

    public static final boolean SPATIAL4J_AVAILABLE;
    public static final boolean JTS_AVAILABLE;

    static {
        boolean xSPATIAL4J_AVAILABLE;
        try {
            Class.forName("org.locationtech.spatial4j.shape.impl.PointImpl");
            xSPATIAL4J_AVAILABLE = true;
        } catch (ClassNotFoundException ignored) {
            xSPATIAL4J_AVAILABLE = false;
        }
        SPATIAL4J_AVAILABLE = xSPATIAL4J_AVAILABLE;

        boolean xJTS_AVAILABLE;
        try {
            Class.forName("org.locationtech.jts.geom.GeometryFactory");
            xJTS_AVAILABLE = true;
        } catch (ClassNotFoundException ignored) {
            xJTS_AVAILABLE = false;
        }
        JTS_AVAILABLE = xJTS_AVAILABLE;
    }

    private ShapesAvailability() {}
}
