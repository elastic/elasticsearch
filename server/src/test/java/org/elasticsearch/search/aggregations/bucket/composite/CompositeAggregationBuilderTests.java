/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.elasticsearch.common.geo.GeoBoundingBoxTests;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.List;

public class CompositeAggregationBuilderTests extends BaseAggregationTestCase<CompositeAggregationBuilder> {
    private DateHistogramValuesSourceBuilder randomDateHistogramSourceBuilder() {
        DateHistogramValuesSourceBuilder histo = new DateHistogramValuesSourceBuilder(randomAlphaOfLengthBetween(5, 10));
        if (randomBoolean()) {
            histo.field(randomAlphaOfLengthBetween(1, 20));
        } else {
            histo.script(new Script(randomAlphaOfLengthBetween(10, 20)));
        }
        if (randomBoolean()) {
            histo.calendarInterval(randomFrom(DateHistogramInterval.days(1),
                DateHistogramInterval.minutes(1), DateHistogramInterval.weeks(1)));
        } else {
            histo.fixedInterval(randomFrom(new DateHistogramInterval(randomNonNegativeLong() + "ms"),
                DateHistogramInterval.days(10), DateHistogramInterval.hours(10)));
        }
        if (randomBoolean()) {
            histo.timeZone(randomZone());
        }
        if (randomBoolean()) {
            histo.missingBucket(true);
        }
        return histo;
    }

    private GeoTileGridValuesSourceBuilder randomGeoTileGridValuesSourceBuilder() {
        GeoTileGridValuesSourceBuilder geoTile = new GeoTileGridValuesSourceBuilder(randomAlphaOfLengthBetween(5, 10));
        if (randomBoolean()) {
            geoTile.precision(randomIntBetween(0, GeoTileUtils.MAX_ZOOM));
        }
        if (randomBoolean()) {
            geoTile.geoBoundingBox(GeoBoundingBoxTests.randomBBox());
        }
        return geoTile;
    }

    private TermsValuesSourceBuilder randomTermsSourceBuilder() {
        TermsValuesSourceBuilder terms = new TermsValuesSourceBuilder(randomAlphaOfLengthBetween(5, 10));
        if (randomBoolean()) {
            terms.field(randomAlphaOfLengthBetween(1, 20));
        } else {
            terms.script(new Script(randomAlphaOfLengthBetween(10, 20)));
        }
        terms.order(randomFrom(SortOrder.values()));
        if (randomBoolean()) {
            terms.missingBucket(true);
        }
        return terms;
    }

    private HistogramValuesSourceBuilder randomHistogramSourceBuilder() {
        HistogramValuesSourceBuilder histo = new HistogramValuesSourceBuilder(randomAlphaOfLengthBetween(5, 10));
        if (randomBoolean()) {
            histo.field(randomAlphaOfLengthBetween(1, 20));
        } else {
            histo.script(new Script(randomAlphaOfLengthBetween(10, 20)));
        }
        if (randomBoolean()) {
            histo.missingBucket(true);
        }
        histo.interval(randomDoubleBetween(Math.nextUp(0), Double.MAX_VALUE, false));
        return histo;
    }

    @Override
    protected CompositeAggregationBuilder createTestAggregatorBuilder() {
        int numSources = randomIntBetween(1, 10);
        numSources = 1;
        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        for (int i = 0; i < numSources; i++) {
            int type = randomIntBetween(0, 3);
            type = 3;
            switch (type) {
                case 0:
                    sources.add(randomTermsSourceBuilder());
                    break;
                case 1:
                    sources.add(randomDateHistogramSourceBuilder());
                    break;
                case 2:
                    sources.add(randomHistogramSourceBuilder());
                    break;
                case 3:
                    sources.add(randomGeoTileGridValuesSourceBuilder());
                    break;
                default:
                    throw new AssertionError("wrong branch");
            }
        }
        return new CompositeAggregationBuilder(randomAlphaOfLength(10), sources);
    }
}
