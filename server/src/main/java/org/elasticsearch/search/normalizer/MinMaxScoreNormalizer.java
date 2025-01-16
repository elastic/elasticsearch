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
        if (min == null || max == null) {
            float xMin = Float.MAX_VALUE;
            float xMax = Float.MIN_VALUE;
            for (ScoreDoc rd : docs) {
                if (rd.score > xMax) {
                    xMax = rd.score;
                }
                if (rd.score < xMin) {
                    xMin = rd.score;
                }
            }
            if (min == null) {
                min = xMin;
            }
            if (max == null) {
                max = xMax;
            }
        }
        if (min > max) {
            throw new IllegalArgumentException("[min=" + min + "] must be less than [max=" + max + "]");
        }
        for (int i = 0; i < docs.length; i++) {
            float score;
            if (min.equals(max)) {
                score = min;
            } else {
                score = (docs[i].score - min) / (max - min);
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
