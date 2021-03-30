/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.stream.Collectors;

public class ParsedTDigestPercentiles extends ParsedPercentiles implements Percentiles {

    @Override
    public String getType() {
        return InternalTDigestPercentiles.NAME;
    }

    @Override
    public double percentile(double percent) {
        return getPercentile(percent);
    }

    @Override
    public String percentileAsString(double percent) {
        return getPercentileAsString(percent);
    }

    @Override
    public double value(String name) {
        return percentile(Double.parseDouble(name));
    }

    @Override
    public Iterable<String> valueNames() {
        return percentiles.keySet().stream().map(d -> d.toString()).collect(Collectors.toList());
    }

    private static final ObjectParser<ParsedTDigestPercentiles, Void> PARSER =
            new ObjectParser<>(ParsedTDigestPercentiles.class.getSimpleName(), true, ParsedTDigestPercentiles::new);
    static {
        ParsedPercentiles.declarePercentilesFields(PARSER);
    }

    public static ParsedTDigestPercentiles fromXContent(XContentParser parser, String name) throws IOException {
        ParsedTDigestPercentiles aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }
}
