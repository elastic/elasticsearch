/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.linear;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class MinMaxScoreNormalizer extends ScoreNormalizer {

    public static final String NAME = "minmax";

    public static final ParseField MIN_FIELD = new ParseField("min");
    public static final ParseField MAX_FIELD = new ParseField("max");

    private static final float EPSILON = 1e-6f;

    public static final ConstructingObjectParser<MinMaxScoreNormalizer, Void> PARSER = new ConstructingObjectParser<>(NAME, args -> {
        Float min = (Float) args[0];
        Float max = (Float) args[1];
        return new MinMaxScoreNormalizer(min, max);
    });

    static {
        PARSER.declareFloat(optionalConstructorArg(), MIN_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), MAX_FIELD);
    }

    private Float min;
    private Float max;

    public MinMaxScoreNormalizer() {
        this.min = null;
        this.max = null;
    }

    public MinMaxScoreNormalizer(Float min, Float max) {
        if (min != null && max != null && min >= max) {
            throw new IllegalArgumentException("[min] must be less than [max]");
        }
        this.min = min;
        this.max = max;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public ScoreDoc[] normalizeScores(ScoreDoc[] docs) {
        if (docs.length == 0) {
            return docs;
        }
        // create a new array to avoid changing ScoreDocs in place
        ScoreDoc[] scoreDocs = new ScoreDoc[docs.length];
        float correction = 0f;
        float xMin = Float.MAX_VALUE;
        float xMax = Float.MIN_VALUE;
        boolean atLeastOneValidScore = false;
        for (ScoreDoc rd : docs) {
            if (false == atLeastOneValidScore && false == Float.isNaN(rd.score)) {
                atLeastOneValidScore = true;
            }
            if (rd.score > xMax) {
                xMax = rd.score;
            }
            if (rd.score < xMin) {
                xMin = rd.score;
            }
        }
        if (false == atLeastOneValidScore) {
            // we do not have any scores to normalize, so we just return the original array
            return docs;
        }

        if (min == null) {
            min = xMin;
        } else {
            if (min > xMin) {
                correction = min - xMin;
            }
        }
        if (max == null) {
            max = xMax;
        }

        if (min > max) {
            throw new IllegalArgumentException("[min=" + min + "] must be less than [max=" + max + "]");
        }
        boolean minEqualsMax = Math.abs(min - max) < EPSILON;
        for (int i = 0; i < docs.length; i++) {
            float score;
            if (minEqualsMax) {
                score = min;
            } else {
                score = correction + (docs[i].score - min) / (max - min);
            }
            scoreDocs[i] = new ScoreDoc(docs[i].doc, score, docs[i].shardIndex);
        }
        return scoreDocs;
    }

    public static MinMaxScoreNormalizer fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public void doToXContent(XContentBuilder builder, Params params) throws IOException {
        if (min != null) {
            builder.field(MIN_FIELD.getPreferredName(), min);
        }
        if (max != null) {
            builder.field(MAX_FIELD.getPreferredName(), max);
        }
    }
}
