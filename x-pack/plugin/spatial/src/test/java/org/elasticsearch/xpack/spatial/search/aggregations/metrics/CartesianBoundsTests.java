/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class CartesianBoundsTests extends BaseAggregationTestCase<CartesianBoundsAggregationBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateSpatialPlugin.class);
    }

    @Override
    protected CartesianBoundsAggregationBuilder createTestAggregatorBuilder() {
        CartesianBoundsAggregationBuilder factory = new CartesianBoundsAggregationBuilder(randomAlphaOfLengthBetween(1, 20));
        String field = randomAlphaOfLengthBetween(3, 20);
        factory.field(field);
        if (randomBoolean()) {
            factory.missing("0,0");
        }
        return factory;
    }

    public void testFailWithSubAgg() throws Exception {
        String source = """
            {
              "viewport": {
                "cartesian_bounds": {
                  "field": "location"
                },
                "aggs": {
                  "names": {
                    "terms": {
                      "field": "name",
                      "size": 10
                    }
                  }
                }
              }
            }
            """;
        XContentParser parser = createParser(JsonXContent.jsonXContent, source);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        Exception e = expectThrows(AggregationInitializationException.class, () -> AggregatorFactories.parseAggregators(parser));
        assertThat(e.toString(), containsString("Aggregator [viewport] of type [cartesian_bounds] cannot accept sub-aggregations"));
    }

    public void testFailWithWrapLongitude() throws Exception {
        String source = """
            {
              "viewport": {
                "cartesian_bounds": {
                  "field": "location",
                  "wrap_longitude": true
                }
              }
            }
            """;
        XContentParser parser = createParser(JsonXContent.jsonXContent, source);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        Exception e = expectThrows(XContentParseException.class, () -> AggregatorFactories.parseAggregators(parser));
        assertThat(e.toString(), containsString("[cartesian_bounds] unknown field [wrap_longitude]"));
    }

}
