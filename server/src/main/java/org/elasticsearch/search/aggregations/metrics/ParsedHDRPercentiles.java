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

public class ParsedHDRPercentiles extends ParsedPercentiles implements Percentiles {

    @Override
    public String getType() {
        return InternalHDRPercentiles.NAME;
    }

    @Override
    public double percentile(double percent) {
        return getPercentile(percent);
    }

    @Override
    public String percentileAsString(double percent) {
        return getPercentileAsString(percent);
    }

    private static final ObjectParser<ParsedHDRPercentiles, Void> PARSER =
            new ObjectParser<>(ParsedHDRPercentiles.class.getSimpleName(), true, ParsedHDRPercentiles::new);
    static {
        ParsedPercentiles.declarePercentilesFields(PARSER);
    }

    public static ParsedHDRPercentiles fromXContent(XContentParser parser, String name) throws IOException {
        ParsedHDRPercentiles aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    @Override
    public double value(String name) {
        return percentile(Double.parseDouble(name));
    }

    @Override
    public Iterable<String> valueNames() {
        return percentiles.keySet().stream().map(d -> d.toString()).collect(Collectors.toList());
    }
}
