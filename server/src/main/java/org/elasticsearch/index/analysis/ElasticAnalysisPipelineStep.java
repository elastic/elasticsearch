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
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.elasticsearch.index.analysis.AnalysisUtil.createStackedTokenStream;
import static org.elasticsearch.index.analysis.AnalysisUtil.extractExtendedAttributes;
import static org.elasticsearch.index.analysis.AnalysisUtil.writeCharStream;

public class ElasticAnalysisPipelineStep implements AnalysisPipelineStep, AnalysisPipelineFirstStep {
    private final Analyzer analyzer;
    private final CustomAnalyzer wrappedAnalyzer;

    public ElasticAnalysisPipelineStep(
        TokenizerFactory tokenizerFactory,
        List<CharFilterFactory> charFilterFactories,
        List<TokenFilterFactory> tokenFilterFactories
    ) {
        this.wrappedAnalyzer = new CustomAnalyzer(
            tokenizerFactory,
            charFilterFactories.toArray(new CharFilterFactory[] {}),
            tokenFilterFactories.toArray(new TokenFilterFactory[] {})
        );

        this.analyzer = AnalysisRegistry.produceAnalyzer("__custom__", new AnalyzerProvider<>() {
            @Override
            public String name() {
                return "__custom__";
            }

            @Override
            public AnalyzerScope scope() {
                return AnalyzerScope.GLOBAL;
            }

            @Override
            public Analyzer get() {
                return wrappedAnalyzer;
            }
        }, null, null, null);
    }

    static class PositionOffset {
        int lastPosition = -1;
        int lastOffset = 0;
        PositionOffset(int lastPosition, int lastOffset) {
            this.lastOffset = lastOffset;
            this.lastPosition = lastPosition;
        }
    }

    private List<AnalyzeAction.AnalyzeToken> tokenizeStream(
        TokenStream stream,
        PositionOffset pos,
        TokenCounter tc,
        String field,
        String[] attributes) throws IOException {
        List<AnalyzeAction.AnalyzeToken> tokens = new ArrayList<>();

        stream.reset();
        CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
        PositionIncrementAttribute posIncr = stream.addAttribute(PositionIncrementAttribute.class);
        OffsetAttribute offset = stream.addAttribute(OffsetAttribute.class);
        TypeAttribute type = stream.addAttribute(TypeAttribute.class);
        PositionLengthAttribute posLen = stream.addAttribute(PositionLengthAttribute.class);

        Set<String> includeAttributes = null;
        if (attributes != null) {
            includeAttributes = Arrays.stream(attributes).map(s -> s.toLowerCase(Locale.ROOT)).collect(Collectors.toSet());;
        }

        while (stream.incrementToken()) {
            int increment = posIncr.getPositionIncrement();
            if (increment > 0) {
                pos.lastPosition = pos.lastPosition + increment;
            }
            tokens.add(
                new AnalyzeAction.AnalyzeToken(
                    term.toString(),
                    pos.lastPosition,
                    pos.lastOffset + offset.startOffset(),
                    pos.lastOffset + offset.endOffset(),
                    posLen.getPositionLength(),
                    type.type(),
                    (includeAttributes == null) ? null : extractExtendedAttributes(stream, includeAttributes)
                )
            );
            tc.increment();
        }
        stream.end();
        pos.lastOffset += offset.endOffset();
        pos.lastPosition += posIncr.getPositionIncrement();

        pos.lastPosition += analyzer.getPositionIncrementGap(field);
        pos.lastOffset += analyzer.getOffsetGap(field);

        return tokens;
    }

    List<AnalyzeAction.AnalyzeToken> tokenizeTexts(
        String field,
        String texts[],
        int maxTokenCount,
        String[] attributes,
        BiFunction<String, String, TokenStream> streamFactory) {
        TokenCounter tc = new TokenCounter(maxTokenCount);
        List<AnalyzeAction.AnalyzeToken> tokens = new ArrayList<>();
        PositionOffset pos = new PositionOffset(-1, 0);

        for (String text : texts) {
            try (TokenStream stream = streamFactory.apply(field, text)) {
                tokens.addAll(tokenizeStream(stream, pos, tc, field, attributes));
            } catch (IOException e) {
                throw new ElasticsearchException("failed to analyze", e);
            }
        }
        return tokens;
    }

    @Override
    public List<AnalyzeAction.AnalyzeToken> process(String field, String[] texts, int maxTokenCount) {
        return tokenizeTexts(field, texts, maxTokenCount, null, (f, t) -> analyzer.tokenStream(f, t));
    }

    @Override
    public List<AnalyzeAction.AnalyzeToken> process(String field, List<AnalyzeAction.AnalyzeToken> inTokens, int maxTokenCount) {
        TokenCounter tc = new TokenCounter(maxTokenCount);

        AnalyzerComponents components = wrappedAnalyzer.getComponents();
        TokenFilterFactory[] tokenFilterFactories = components.getTokenFilters();
        PositionOffset pos = new PositionOffset(-1, 0);

        try (TokenStream stream = createStackedTokenStream(new PipelineTokenizer(inTokens), tokenFilterFactories)) {
            return tokenizeStream(stream, pos, tc, field, null);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to analyze", e);
        }
    }

    @Override
    public DetailedPipelineAnalysisPackage details(String field, String[] texts, int maxTokenCount, String[] attributes) {
        AnalyzerComponents components = wrappedAnalyzer.getComponents();

        CharFilterFactory[] charFilterFactories = components.getCharFilters();
        TokenizerFactory tokenizerFactory = components.getTokenizerFactory();

        String[][] charFiltersTexts = new String[charFilterFactories != null ? charFilterFactories.length : 0][texts.length];

        for (int i = 0; i < texts.length; i++) {
            String charFilteredSource = texts[i];
            Reader reader = new StringReader(charFilteredSource);

            if (charFilterFactories != null) {
                for (int charFilterIndex = 0; charFilterIndex < charFilterFactories.length; charFilterIndex++) {
                    reader = charFilterFactories[charFilterIndex].create(reader);
                    Reader readerForWriteOut = new StringReader(charFilteredSource);
                    readerForWriteOut = charFilterFactories[charFilterIndex].create(readerForWriteOut);
                    charFilteredSource = writeCharStream(readerForWriteOut);
                    charFiltersTexts[charFilterIndex][i] = charFilteredSource;
                }
            }
        }

        List<AnalyzeAction.CharFilteredText> charFilteredLists = new ArrayList<>();

        if (charFilterFactories != null) {
            for (int charFilterIndex = 0; charFilterIndex < charFiltersTexts.length; charFilterIndex++) {
                charFilteredLists.add(new AnalyzeAction.CharFilteredText(
                    charFilterFactories[charFilterIndex].name(),
                    charFiltersTexts[charFilterIndex]
                ));
            }
        }

        List<AnalyzeAction.AnalyzeToken> tokenizerList = tokenizeTexts(field, texts, maxTokenCount, attributes, (f, t) -> {
            Tokenizer tokenizer = tokenizerFactory.create();
            tokenizer.setReader(new StringReader(t));
            return tokenizer;
        });

        return new DetailedPipelineAnalysisPackage(
            new AnalyzeAction.AnalyzeTokenList(tokenizerFactory.name(), tokenizerList.toArray(new AnalyzeAction.AnalyzeToken[0])),
            charFilteredLists,
            detailedFilters(field, texts, maxTokenCount, attributes)
        );
    }

    public List<AnalyzeAction.AnalyzeTokenList> detailedFilters(String field, String[] texts, int maxTokenCount, String[] attributes) {
        AnalyzerComponents components = wrappedAnalyzer.getComponents();

        CharFilterFactory[] charFilterFactories = components.getCharFilters();
        TokenizerFactory tokenizerFactory = components.getTokenizerFactory();
        TokenFilterFactory[] tokenFilterFactories = components.getTokenFilters();

        List<AnalyzeAction.AnalyzeTokenList> result = new ArrayList<>();

        if (tokenFilterFactories != null) {
            for (int i = 0; i < tokenFilterFactories.length; i++) {
                int tokenizerIndex = i;
                List<AnalyzeAction.AnalyzeToken> filterTokens = tokenizeTexts(field, texts, maxTokenCount, attributes, (f, t) ->
                    createStackedTokenStream(
                        t,
                        charFilterFactories,
                        tokenizerFactory,
                        tokenFilterFactories,
                        tokenizerIndex + 1
                    )
                );
                result.add(new AnalyzeAction.AnalyzeTokenList(
                    tokenFilterFactories[i].name(),
                    filterTokens.toArray(new AnalyzeAction.AnalyzeToken[0])));
            }
        }

        return result;
    }

    @Override
    public List<AnalyzeAction.AnalyzeTokenList> detailedFilters(String field, List<AnalyzeAction.AnalyzeToken> tokens, int maxTokenCount, String[] attributes) {
        AnalyzerComponents components = wrappedAnalyzer.getComponents();

        TokenFilterFactory[] tokenFilterFactories = components.getTokenFilters();

        List<AnalyzeAction.AnalyzeTokenList> result = new ArrayList<>();

        if (tokenFilterFactories != null) {
            for (int i = 0; i < tokenFilterFactories.length; i++) {
                TokenCounter tc = new TokenCounter(maxTokenCount);
                PositionOffset pos = new PositionOffset(-1, 0);

                try (TokenStream stream = createStackedTokenStream(
                    new PipelineTokenizer(tokens),
                    tokenFilterFactories,
                    i+1)) {
                    List<AnalyzeAction.AnalyzeToken> filterTokens = tokenizeStream(stream, pos, tc, field, attributes);
                    result.add(new AnalyzeAction.AnalyzeTokenList(
                        tokenFilterFactories[i].name(),
                        filterTokens.toArray(new AnalyzeAction.AnalyzeToken[0])));
                } catch (IOException e) {
                    throw new ElasticsearchException("failed to analyze", e);
                }
            }
        }

        return result;
    }
}
