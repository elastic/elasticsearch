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
import java.util.Iterator;

public class ParsedTDigestPercentileRanks extends ParsedPercentileRanks {

    @Override
    public String getType() {
        return InternalTDigestPercentileRanks.NAME;
    }

    @Override
    public Iterator<Percentile> iterator() {
        final Iterator<Percentile> iterator = super.iterator();
        return new Iterator<Percentile>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Percentile next() {
                Percentile percentile = iterator.next();
                return new Percentile(percentile.getValue(), percentile.getPercent());
            }
        };
    }

    private static final ObjectParser<ParsedTDigestPercentileRanks, Void> PARSER = new ObjectParser<>(
        ParsedTDigestPercentileRanks.class.getSimpleName(),
        true,
        ParsedTDigestPercentileRanks::new
    );
    static {
        ParsedPercentiles.declarePercentilesFields(PARSER);
    }

    public static ParsedTDigestPercentileRanks fromXContent(XContentParser parser, String name) throws IOException {
        ParsedTDigestPercentileRanks aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }
}
