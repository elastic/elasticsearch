/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class InternalBoundsTests extends InternalAggregationTestCase<InternalBounds> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new LocalStateSpatialPlugin();
    }

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        List<NamedXContentRegistry.Entry> extendedNamedXContents = new ArrayList<>(super.getNamedXContents());
        extendedNamedXContents.addAll(new LocalStateSpatialPlugin().getNamedXContent());
        return extendedNamedXContents;
    }

    @Override
    protected InternalBounds createTestInstance(String name, Map<String, Object> metadata) {
        // we occasionally want to test top = Double.NEGATIVE_INFINITY since this triggers empty xContent object
        float top = frequently() ? randomFloat() : Float.NEGATIVE_INFINITY;
        InternalBounds bounds = new InternalBounds(name,
            top, randomFloat(), randomFloat(), randomFloat(), metadata);
        return bounds;
    }

    @Override
    protected void assertReduced(InternalBounds reduced, List<InternalBounds> inputs) {
        float top = Float.NEGATIVE_INFINITY;
        float bottom = Float.POSITIVE_INFINITY;
        float posLeft = Float.POSITIVE_INFINITY;
        float posRight = Float.NEGATIVE_INFINITY;

        for (InternalBounds bounds : inputs) {
            if (bounds.topLeft().getY() > top) {
                top = bounds.topLeft().getY() ;
            }
            if (bounds.bottomRight().getY()  < bottom) {
                bottom = bounds.bottomRight().getY();
            }
            if (bounds.topLeft().getX() < posLeft) {
                posLeft = bounds.topLeft().getX();
            }
            if (bounds.bottomRight().getX() > posRight) {
                posRight = bounds.bottomRight().getX() ;
            }
        }
        assertThat(reduced.topLeft().getY(), equalTo(top));
        assertThat(reduced.bottomRight().getY(), equalTo(bottom));
        assertThat(reduced.topLeft().getX(), equalTo(posLeft));
        assertThat(reduced.bottomRight().getX(), equalTo(posRight));
    }

    @Override
    protected void assertFromXContent(InternalBounds aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof ParsedBounds);
        ParsedBounds parsed = (ParsedBounds) parsedAggregation;

        assertEquals(aggregation.topLeft(), parsed.topLeft());
        assertEquals(aggregation.bottomRight(), parsed.bottomRight());
    }

    @Override
    protected InternalBounds mutateInstance(InternalBounds instance) {
        String name = instance.getName();
        float top = instance.topLeft().getY();
        float bottom = instance.bottomRight().getY();
        float posLeft = instance.topLeft().getX();
        float posRight = instance.bottomRight().getX();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 5)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            if (Float.isFinite(top)) {
                top += between(1, 20);
            } else {
                top = randomFloat();
            }
            break;
        case 2:
            bottom += between(1, 20);
            break;
        case 3:
            posLeft += between(1, 20);
            break;
        case 4:
            posRight += between(1, 20);
            break;
        case 5:
            if (metadata == null) {
                metadata = new HashMap<>(1);
            } else {
                metadata = new HashMap<>(instance.getMetadata());
            }
            metadata.put(randomAlphaOfLength(15), randomInt());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalBounds(name, top, bottom, posLeft, posRight, metadata);
    }
}
