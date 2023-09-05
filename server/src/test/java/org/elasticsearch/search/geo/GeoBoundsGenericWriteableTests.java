/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.geo;

import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.GenericNamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;

import java.io.IOException;
import java.util.List;

public class GeoBoundsGenericWriteableTests extends AbstractNamedWriteableTestCase<GenericNamedWriteable> {
    NamedWriteableRegistry registry = new NamedWriteableRegistry(
        List.of(new NamedWriteableRegistry.Entry(GenericNamedWriteable.class, GeoBoundingBox.class.getSimpleName(), GeoBoundingBox::new))
    );

    @Override
    protected GeoBoundingBox createTestInstance() {
        Rectangle box = GeoTestUtil.nextBox();
        return new GeoBoundingBox(new GeoPoint(box.maxLat, box.minLon), new GeoPoint(box.minLat, box.maxLon));
    }

    @Override
    protected GeoBoundingBox mutateInstance(GenericNamedWriteable instance) throws IOException {
        assert instance instanceof GeoBoundingBox : "Expected GeoBoundingBox";
        GeoBoundingBox geoBoundingBox = (GeoBoundingBox) instance;
        double width = geoBoundingBox.right() - geoBoundingBox.left();
        double height = geoBoundingBox.top() - geoBoundingBox.bottom();
        double top = geoBoundingBox.top() - height / 4;
        double left = geoBoundingBox.left() + width / 4;
        double bottom = geoBoundingBox.bottom() + height / 4;
        double right = geoBoundingBox.right() - width / 4;
        return new GeoBoundingBox(new GeoPoint(top, left), new GeoPoint(bottom, right));
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return registry;
    }

    @Override
    protected Class<GenericNamedWriteable> categoryClass() {
        return GenericNamedWriteable.class;
    }
}
