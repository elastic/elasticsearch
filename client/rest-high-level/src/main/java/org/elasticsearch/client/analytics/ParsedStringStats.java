/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.analytics;

import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Results from the {@code string_stats} aggregation.
 */
public class ParsedStringStats extends ParsedAggregation {
    private static final ParseField COUNT_FIELD = new ParseField("count");
    private static final ParseField MIN_LENGTH_FIELD = new ParseField("min_length");
    private static final ParseField MAX_LENGTH_FIELD = new ParseField("max_length");
    private static final ParseField AVG_LENGTH_FIELD = new ParseField("avg_length");
    private static final ParseField ENTROPY_FIELD = new ParseField("entropy");
    private static final ParseField DISTRIBUTION_FIELD = new ParseField("distribution");

    private final long count;
    private final int minLength;
    private final int maxLength;
    private final double avgLength;
    private final double entropy;
    private final boolean showDistribution;
    private final Map<String, Double> distribution;

    private ParsedStringStats(
        String name,
        long count,
        int minLength,
        int maxLength,
        double avgLength,
        double entropy,
        boolean showDistribution,
        Map<String, Double> distribution
    ) {
        setName(name);
        this.count = count;
        this.minLength = minLength;
        this.maxLength = maxLength;
        this.avgLength = avgLength;
        this.entropy = entropy;
        this.showDistribution = showDistribution;
        this.distribution = distribution;
    }

    /**
     * The number of non-empty fields counted.
     */
    public long getCount() {
        return count;
    }

    /**
     * The length of the shortest term.
     */
    public int getMinLength() {
        return minLength;
    }

    /**
     * The length of the longest term.
     */
    public int getMaxLength() {
        return maxLength;
    }

    /**
     * The average length computed over all terms.
     */
    public double getAvgLength() {
        return avgLength;
    }

    /**
     * The <a href="https://en.wikipedia.org/wiki/Entropy_(information_theory)">Shannon Entropy</a>
     * value computed over all terms collected by the aggregation.
     * Shannon entropy quantifies the amount of information contained in
     * the field. It is a very useful metric for measuring a wide range of
     * properties of a data set, such as diversity, similarity,
     * randomness etc.
     */
    public double getEntropy() {
        return entropy;
    }

    /**
     * The probability distribution for all characters. {@code null} unless
     * explicitly requested with {@link StringStatsAggregationBuilder#showDistribution(boolean)}.
     */
    public Map<String, Double> getDistribution() {
        return distribution;
    }

    @Override
    public String getType() {
        return StringStatsAggregationBuilder.NAME;
    }

    private static final Object NULL_DISTRIBUTION_MARKER = new Object();
    public static final ConstructingObjectParser<ParsedStringStats, String> PARSER = new ConstructingObjectParser<>(
        StringStatsAggregationBuilder.NAME,
        true,
        (args, name) -> {
            long count = (long) args[0];
            boolean disributionWasExplicitNull = args[5] == NULL_DISTRIBUTION_MARKER;
            if (count == 0) {
                return new ParsedStringStats(name, count, 0, 0, 0, 0, disributionWasExplicitNull, null);
            }
            int minLength = (int) args[1];
            int maxLength = (int) args[2];
            double averageLength = (double) args[3];
            double entropy = (double) args[4];
            if (disributionWasExplicitNull) {
                return new ParsedStringStats(name, count, minLength, maxLength, averageLength, entropy, disributionWasExplicitNull, null);
            } else {
                @SuppressWarnings("unchecked")
                Map<String, Double> distribution = (Map<String, Double>) args[5];
                return new ParsedStringStats(name, count, minLength, maxLength, averageLength, entropy, distribution != null, distribution);
            }
        }
    );
    static {
        PARSER.declareLong(constructorArg(), COUNT_FIELD);
        PARSER.declareIntOrNull(constructorArg(), 0, MIN_LENGTH_FIELD);
        PARSER.declareIntOrNull(constructorArg(), 0, MAX_LENGTH_FIELD);
        PARSER.declareDoubleOrNull(constructorArg(), 0, AVG_LENGTH_FIELD);
        PARSER.declareDoubleOrNull(constructorArg(), 0, ENTROPY_FIELD);
        PARSER.declareObjectOrNull(
            optionalConstructorArg(),
            (p, c) -> unmodifiableMap(p.map(HashMap::new, XContentParser::doubleValue)),
            NULL_DISTRIBUTION_MARKER,
            DISTRIBUTION_FIELD
        );
        ParsedAggregation.declareAggregationFields(PARSER);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(COUNT_FIELD.getPreferredName(), count);
        if (count == 0) {
            builder.nullField(MIN_LENGTH_FIELD.getPreferredName());
            builder.nullField(MAX_LENGTH_FIELD.getPreferredName());
            builder.nullField(AVG_LENGTH_FIELD.getPreferredName());
            builder.field(ENTROPY_FIELD.getPreferredName(), 0.0);
        } else {
            builder.field(MIN_LENGTH_FIELD.getPreferredName(), minLength);
            builder.field(MAX_LENGTH_FIELD.getPreferredName(), maxLength);
            builder.field(AVG_LENGTH_FIELD.getPreferredName(), avgLength);
            builder.field(ENTROPY_FIELD.getPreferredName(), entropy);
        }
        if (showDistribution) {
            builder.field(DISTRIBUTION_FIELD.getPreferredName(), distribution);
        }
        return builder;
    }
}
