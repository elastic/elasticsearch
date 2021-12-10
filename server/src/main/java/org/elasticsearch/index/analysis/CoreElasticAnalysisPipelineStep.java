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
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.BytesRef;
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
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class CoreElasticAnalysisPipelineStep implements AnalysisPipelineStep {
    private final Analyzer analyzer;

    public CoreElasticAnalysisPipelineStep(
        TokenizerFactory tokenizerFactory,
        List<CharFilterFactory> charFilterFactories,
        List<TokenFilterFactory> tokenFilterFactories
    ) {
        Analyzer wrappedAnalyzer = new CustomAnalyzer(
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

    private Map<String, Object> buildAttributeMap(TokenStream stream, String[] attributes) {
        if (attributes == null) {
            return null;
        }
        final Set<String> includeAttributes = Arrays.stream(attributes).map(s -> s.toLowerCase(Locale.ROOT)).collect(Collectors.toSet());
        return extractExtendedAttributes(stream, includeAttributes);
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

        Map<String, Object> attributeMap = buildAttributeMap(stream, attributes);

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
                    attributeMap
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

    private static String writeCharStream(Reader input) {
        final int BUFFER_SIZE = 1024;
        char[] buf = new char[BUFFER_SIZE];
        int len;
        StringBuilder sb = new StringBuilder();
        do {
            try {
                len = input.read(buf, 0, BUFFER_SIZE);
            } catch (IOException e) {
                throw new ElasticsearchException("failed to analyze (charFiltering)", e);
            }
            if (len > 0) {
                sb.append(buf, 0, len);
            }
        } while (len == BUFFER_SIZE);
        return sb.toString();
    }

    @Override
    public DetailedPipelineAnalysisPackage details(String field, String[] texts, int maxTokenCount, String[] attributes) {
        CustomAnalyzer customAnalyzer = (CustomAnalyzer)analyzer;
        AnalyzerComponents components = customAnalyzer.getComponents();

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

    private static TokenStream createStackedTokenStream(
        String source,
        CharFilterFactory[] charFilterFactories,
        TokenizerFactory tokenizerFactory,
        TokenFilterFactory[] tokenFilterFactories,
        int current
    ) {
        Reader reader = new StringReader(source);
        for (CharFilterFactory charFilterFactory : charFilterFactories) {
            reader = charFilterFactory.create(reader);
        }
        Tokenizer tokenizer = tokenizerFactory.create();
        tokenizer.setReader(reader);
        TokenStream tokenStream = tokenizer;
        for (int i = 0; i < current; i++) {
            tokenStream = tokenFilterFactories[i].create(tokenStream);
        }
        return tokenStream;
    }

    @Override
    public List<AnalyzeAction.AnalyzeTokenList> detailedFilters(String field, String[] texts, int maxTokenCount, String[] attributes) {
        CustomAnalyzer customAnalyzer = (CustomAnalyzer)analyzer;
        AnalyzerComponents components = customAnalyzer.getComponents();

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

    private static Map<String, Object> extractExtendedAttributes(TokenStream stream, final Set<String> includeAttributes) {
        final Map<String, Object> extendedAttributes = new TreeMap<>();

        stream.reflectWith((attClass, key, value) -> {
            if (CharTermAttribute.class.isAssignableFrom(attClass)) {
                return;
            }
            if (PositionIncrementAttribute.class.isAssignableFrom(attClass)) {
                return;
            }
            if (OffsetAttribute.class.isAssignableFrom(attClass)) {
                return;
            }
            if (TypeAttribute.class.isAssignableFrom(attClass)) {
                return;
            }
            if (includeAttributes == null || includeAttributes.isEmpty() || includeAttributes.contains(key.toLowerCase(Locale.ROOT))) {
                if (value instanceof BytesRef) {
                    final BytesRef p = (BytesRef) value;
                    value = p.toString();
                }
                extendedAttributes.put(key, value);
            }
        });

        return extendedAttributes;
    }
}
