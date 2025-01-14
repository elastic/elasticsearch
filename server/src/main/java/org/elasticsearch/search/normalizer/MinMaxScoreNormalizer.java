/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.normalizer;

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

    public MinMaxScoreNormalizer() {}

    public MinMaxScoreNormalizer(Float min, Float max) {
        this.min = min;
        this.max = max;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public ScoreDoc[] normalizeScores(ScoreDoc[] docs) {
        // create a new array to avoid changing ScoreDocs in place
        ScoreDoc[] scoreDocs = new ScoreDoc[docs.length];
        if (min == null || max == null) {
            float localMin = Float.MAX_VALUE;
            float localMax = Float.MIN_VALUE;
            for (ScoreDoc rd : docs) {
                if (max == null && rd.score > localMax) {
                    localMax = rd.score;
                }
                if (min == null && rd.score < localMin) {
                    localMin = rd.score;
                }
            }
            if (min == null) {
                min = localMin;
            }
            if (max == null) {
                max = localMax;
            }
        }
        float epsilon = Float.MIN_NORMAL;
        for (int i = 0; i < docs.length; i++) {
            float score = epsilon + ((docs[i].score - min) / (max - min));
            scoreDocs[i] = new ScoreDoc(docs[i].doc, score, docs[i].shardIndex);
        }
        return scoreDocs;
    }

    public static MinMaxScoreNormalizer fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(MIN_FIELD.getPreferredName(), min);
        builder.field(MAX_FIELD.getPreferredName(), max);
        return builder;
    }
}
