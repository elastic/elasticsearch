/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CoreElasticAnalysisPipelineStep implements AnalysisPipelineStep {
    private final Analyzer analyzer;

    public CoreElasticAnalysisPipelineStep(
        TokenizerFactory tokenizerFactory,
        List<CharFilterFactory> charFilterFactories,
        List<TokenFilterFactory> tokenFilterFactories
    ) {
        this.analyzer = new CustomAnalyzer(
            tokenizerFactory,
            charFilterFactories.toArray(new CharFilterFactory[] {}),
            tokenFilterFactories.toArray(new TokenFilterFactory[] {})
        );
    }

    @Override
    public List<AnalyzeAction.AnalyzeToken> process(String field, String[] texts, int maxTokenCount) {
        TokenCounter tc = new TokenCounter(maxTokenCount);
        List<AnalyzeAction.AnalyzeToken> tokens = new ArrayList<>();
        int lastPosition = -1;
        int lastOffset = 0;
        for (String text : texts) {
            try (TokenStream stream = analyzer.tokenStream(field, text)) {
                stream.reset();
                CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
                PositionIncrementAttribute posIncr = stream.addAttribute(PositionIncrementAttribute.class);
                OffsetAttribute offset = stream.addAttribute(OffsetAttribute.class);
                TypeAttribute type = stream.addAttribute(TypeAttribute.class);
                PositionLengthAttribute posLen = stream.addAttribute(PositionLengthAttribute.class);

                while (stream.incrementToken()) {
                    int increment = posIncr.getPositionIncrement();
                    if (increment > 0) {
                        lastPosition = lastPosition + increment;
                    }
                    tokens.add(
                        new AnalyzeAction.AnalyzeToken(
                            term.toString(),
                            lastPosition,
                            lastOffset + offset.startOffset(),
                            lastOffset + offset.endOffset(),
                            posLen.getPositionLength(),
                            type.type(),
                            null
                        )
                    );
                    tc.increment();
                }
                stream.end();
                lastOffset += offset.endOffset();
                lastPosition += posIncr.getPositionIncrement();

                lastPosition += analyzer.getPositionIncrementGap(field);
                lastOffset += analyzer.getOffsetGap(field);
            } catch (IOException e) {
                throw new ElasticsearchException("failed to analyze", e);
            }
        }
        return tokens;
    }
}
