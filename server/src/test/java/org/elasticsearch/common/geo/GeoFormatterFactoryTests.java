/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;


import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

/**
 * Tests for {@link GeoFormatterFactory}
 */
public class GeoFormatterFactoryTests extends ESTestCase {

   public void testSupportedFormats() {
       assertThat(GeoFormatterFactory.getFormatter(GeoFormatterFactory.WKT), Matchers.notNullValue());
       assertThat(GeoFormatterFactory.getFormatter(GeoFormatterFactory.GEOJSON), Matchers.notNullValue());
   }

    public void testThrowErrorWhenPassingMeta() {
        {
            IllegalArgumentException ex =
                expectThrows(IllegalArgumentException.class, () -> GeoFormatterFactory.getFormatter(GeoFormatterFactory.WKT + "(xx)"));
            assertThat(ex.getMessage(), Matchers.equalTo("wkt format does not support extra parameters [xx]"));
        }
        {
            IllegalArgumentException ex =
                expectThrows(IllegalArgumentException.class, () -> GeoFormatterFactory.getFormatter(GeoFormatterFactory.GEOJSON + "(xx)"));
            assertThat(ex.getMessage(), Matchers.equalTo("geojson format does not support extra parameters [xx]"));
        }
    }
}
