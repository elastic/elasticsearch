/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.search.retriever.LinearRetrieverBuilder.RETRIEVERS_FIELD;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class LinearRetrieverComponent implements ToXContentObject {

    public static final ParseField RETRIEVER_FIELD = new ParseField("retriever");
    public static final ParseField WEIGHT_FIELD = new ParseField("weight");
    public static final ParseField NORMALIZER_FIELD = new ParseField("normalizer");

    static final float DEFAULT_WEIGHT = 1f;
    static final ScoreNormalizer DEFAULT_NORMALIZER = ScoreNormalizer.IDENTITY;

    public static final String NAME = "component";

    static final ConstructingObjectParser<LinearRetrieverComponent, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        args -> {
            RetrieverBuilder base = (RetrieverBuilder) args[0];
            float weight = args[1] == null ? DEFAULT_WEIGHT : (float) args[1];
            ScoreNormalizer normalizer = args[2] == null ? DEFAULT_NORMALIZER : ScoreNormalizer.fromString((String) args[2]);
            return new LinearRetrieverComponent(base, weight, normalizer);
        }
    );

    static {
        PARSER.declareNamedObject(constructorArg(), (p, c, n) -> {
            RetrieverBuilder retrieverBuilder = p.namedObject(RetrieverBuilder.class, n, c);
            c.trackRetrieverUsage(retrieverBuilder.getName());
            return retrieverBuilder;
        }, RETRIEVER_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), WEIGHT_FIELD);
        PARSER.declareString(optionalConstructorArg(), NORMALIZER_FIELD);
    }

    RetrieverBuilder retriever;
    float weight;
    ScoreNormalizer normalizer;

    public LinearRetrieverComponent(RetrieverBuilder base, float weight, ScoreNormalizer normalizer) {
        this.retriever = base;
        this.weight = weight;
        this.normalizer = normalizer;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(RETRIEVERS_FIELD.getPreferredName(), retriever);
        return builder;
    }

    public static LinearRetrieverComponent fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        return PARSER.apply(parser, context);
    }

    public enum ScoreNormalizer {
        IDENTITY {
            @Override
            public ScoreDoc[] normalizeScores(ScoreDoc[] docs) {
                // no-op
                return docs;
            }
        },
        MINMAX {
            @Override
            public ScoreDoc[] normalizeScores(ScoreDoc[] docs) {
                // create a new array to avoid changing ScoreDocs in place
                ScoreDoc[] scoreDocs = new ScoreDoc[docs.length];
                // to avoid 0 scores
                float epsilon = Float.MIN_NORMAL;
                float min = Float.MAX_VALUE;
                float max = Float.MIN_VALUE;
                for (ScoreDoc rd : docs) {
                    if (rd.score > max) {
                        max = rd.score;
                    }
                    if (rd.score < min) {
                        min = rd.score;
                    }
                }
                for (int i = 0; i < docs.length; i++) {
                    float score = epsilon + ((docs[i].score - min) / (max - min));
                    scoreDocs[i] = new ScoreDoc(docs[i].doc, score, docs[i].shardIndex);
                }
                return scoreDocs;
            }
        };

        public abstract ScoreDoc[] normalizeScores(ScoreDoc[] docs);

        public static ScoreNormalizer fromString(final String normalizerName){
            for(ScoreNormalizer normalizer: values()){
                if(normalizer.name().equalsIgnoreCase(normalizerName)){
                    return normalizer;
                }
            }
            throw new IllegalArgumentException("Unknown [" + normalizerName + "] ScoreNormalizer provided.");
        }
    }
}
