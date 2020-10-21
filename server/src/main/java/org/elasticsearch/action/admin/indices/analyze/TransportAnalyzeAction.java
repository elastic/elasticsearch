/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.indices.analyze;

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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.AnalyzerComponents;
import org.elasticsearch.index.analysis.AnalyzerComponentsProvider;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.NameOrDefinition;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Transport action used to execute analyze requests
 */
public class TransportAnalyzeAction extends TransportSingleShardAction<AnalyzeAction.Request, AnalyzeAction.Response> {

    private final Settings settings;
    private final IndicesService indicesService;

    @Inject
    public TransportAnalyzeAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                  TransportService transportService, IndicesService indicesService, ActionFilters actionFilters,
                                  IndexNameExpressionResolver indexNameExpressionResolver) {
        super(AnalyzeAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
            AnalyzeAction.Request::new, ThreadPool.Names.ANALYZE);
        this.settings = settings;
        this.indicesService = indicesService;
    }

    @Override
    protected Writeable.Reader<AnalyzeAction.Response> getResponseReader() {
        return AnalyzeAction.Response::new;
    }

    @Override
    protected boolean resolveIndex(AnalyzeAction.Request request) {
        return request.index() != null;
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, InternalRequest request) {
        if (request.concreteIndex() != null) {
            return super.checkRequestBlock(state, request);
        }
        return null;
    }

    @Override
    protected ShardsIterator shards(ClusterState state, InternalRequest request) {
        if (request.concreteIndex() == null) {
            // just execute locally....
            return null;
        }
        return state.routingTable().index(request.concreteIndex()).randomAllActiveShardsIt();
    }

    @Override
    protected AnalyzeAction.Response shardOperation(AnalyzeAction.Request request, ShardId shardId) throws IOException {
        final IndexService indexService = getIndexService(shardId);
        final int maxTokenCount = indexService == null ?
            IndexSettings.MAX_TOKEN_COUNT_SETTING.get(settings) : indexService.getIndexSettings().getMaxTokenCount();

        return analyze(request, indicesService.getAnalysis(), indexService, maxTokenCount);
    }

    public static AnalyzeAction.Response analyze(AnalyzeAction.Request request, AnalysisRegistry analysisRegistry,
                                          IndexService indexService, int maxTokenCount) throws IOException {

        IndexSettings settings = indexService == null ? null : indexService.getIndexSettings();

        // First, we check to see if the request requires a custom analyzer.  If so, then we
        // need to build it and then close it after use.
        try (Analyzer analyzer = buildCustomAnalyzer(request, analysisRegistry, settings)) {
            if (analyzer != null) {
                return analyze(request, analyzer, maxTokenCount);
            }
        }

        // Otherwise we use a built-in analyzer, which should not be closed
        return analyze(request, getAnalyzer(request, analysisRegistry, indexService), maxTokenCount);
    }

    private IndexService getIndexService(ShardId shardId) {
        if (shardId != null) {
            return indicesService.indexServiceSafe(shardId.getIndex());
        }
        return null;
    }

    private static Analyzer getAnalyzer(AnalyzeAction.Request request, AnalysisRegistry analysisRegistry,
                                        IndexService indexService) throws IOException {
        if (request.analyzer() != null) {
            if (indexService == null) {
                Analyzer analyzer = analysisRegistry.getAnalyzer(request.analyzer());
                if (analyzer == null) {
                    throw new IllegalArgumentException("failed to find global analyzer [" + request.analyzer() + "]");
                }
                return analyzer;
            } else {
                Analyzer analyzer = indexService.getIndexAnalyzers().get(request.analyzer());
                if (analyzer == null) {
                    throw new IllegalArgumentException("failed to find analyzer [" + request.analyzer() + "]");
                }
                return analyzer;
            }
        }
        if (request.normalizer() != null) {
            // Get normalizer from indexAnalyzers
            if (indexService == null) {
                throw new IllegalArgumentException("analysis based on a normalizer requires an index");
            }
            Analyzer analyzer = indexService.getIndexAnalyzers().getNormalizer(request.normalizer());
            if (analyzer == null) {
                throw new IllegalArgumentException("failed to find normalizer under [" + request.normalizer() + "]");
            }
            return analyzer;
        }
        if (request.field() != null) {
            if (indexService == null) {
                throw new IllegalArgumentException("analysis based on a specific field requires an index");
            }
            MappedFieldType fieldType = indexService.mapperService().fieldType(request.field());
            if (fieldType != null) {
                if (fieldType instanceof StringFieldType) {
                    return fieldType.indexAnalyzer();
                } else {
                    throw new IllegalArgumentException("Can't process field [" + request.field() +
                        "], Analysis requests are only supported on tokenized fields");
                }
            }
        }
        if (indexService == null) {
            return analysisRegistry.getAnalyzer("standard");
        } else {
            return indexService.getIndexAnalyzers().getDefaultIndexAnalyzer();
        }
    }

    private static Analyzer buildCustomAnalyzer(AnalyzeAction.Request request, AnalysisRegistry analysisRegistry,
                                                IndexSettings indexSettings) throws IOException {
        if (request.tokenizer() != null) {
            return analysisRegistry.buildCustomAnalyzer(indexSettings, false,
                request.tokenizer(), request.charFilters(), request.tokenFilters());
        } else if (((request.tokenFilters() != null && request.tokenFilters().size() > 0)
            || (request.charFilters() != null && request.charFilters().size() > 0))) {
            return analysisRegistry.buildCustomAnalyzer(indexSettings, true, new NameOrDefinition("keyword"),
                request.charFilters(), request.tokenFilters());
        }
        return null;
    }

    private static AnalyzeAction.Response analyze(AnalyzeAction.Request request, Analyzer analyzer, int maxTokenCount) {
        if (request.explain()) {
            return new AnalyzeAction.Response(null, detailAnalyze(request, analyzer, maxTokenCount));
        }
        return new AnalyzeAction.Response(simpleAnalyze(request, analyzer, maxTokenCount), null);
    }

    private static List<AnalyzeAction.AnalyzeToken> simpleAnalyze(AnalyzeAction.Request request,
                                                                  Analyzer analyzer, int maxTokenCount) {
        TokenCounter tc = new TokenCounter(maxTokenCount);
        List<AnalyzeAction.AnalyzeToken> tokens = new ArrayList<>();
        int lastPosition = -1;
        int lastOffset = 0;
        // Note that we always pass "" as the field to the various Analyzer methods, because
        // the analyzers we use here are all field-specific and so ignore this parameter
        for (String text : request.text()) {
            try (TokenStream stream = analyzer.tokenStream("", text)) {
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
                    tokens.add(new AnalyzeAction.AnalyzeToken(term.toString(), lastPosition, lastOffset + offset.startOffset(),
                        lastOffset + offset.endOffset(), posLen.getPositionLength(), type.type(), null));
                    tc.increment();
                }
                stream.end();
                lastOffset += offset.endOffset();
                lastPosition += posIncr.getPositionIncrement();

                lastPosition += analyzer.getPositionIncrementGap("");
                lastOffset += analyzer.getOffsetGap("");
            } catch (IOException e) {
                throw new ElasticsearchException("failed to analyze", e);
            }
        }
        return tokens;
    }

    private static AnalyzeAction.DetailAnalyzeResponse detailAnalyze(AnalyzeAction.Request request, Analyzer analyzer,
                                                                     int maxTokenCount) {
        AnalyzeAction.DetailAnalyzeResponse detailResponse;
        final Set<String> includeAttributes = new HashSet<>();
        if (request.attributes() != null) {
            for (String attribute : request.attributes()) {
                includeAttributes.add(attribute.toLowerCase(Locale.ROOT));
            }
        }

        // maybe unwrap analyzer from NamedAnalyzer
        Analyzer potentialCustomAnalyzer = analyzer;
        if (analyzer instanceof NamedAnalyzer) {
            potentialCustomAnalyzer = ((NamedAnalyzer) analyzer).analyzer();
        }

        if (potentialCustomAnalyzer instanceof AnalyzerComponentsProvider) {
            AnalyzerComponentsProvider customAnalyzer = (AnalyzerComponentsProvider) potentialCustomAnalyzer;
            // note: this is not field-name dependent in our cases so we can leave out the argument
            int positionIncrementGap = potentialCustomAnalyzer.getPositionIncrementGap("");
            int offsetGap = potentialCustomAnalyzer.getOffsetGap("");
            AnalyzerComponents components = customAnalyzer.getComponents();
            // divide charfilter, tokenizer tokenfilters
            CharFilterFactory[] charFilterFactories = components.getCharFilters();
            TokenizerFactory tokenizerFactory = components.getTokenizerFactory();
            TokenFilterFactory[] tokenFilterFactories = components.getTokenFilters();

            String[][] charFiltersTexts = new String[charFilterFactories != null ? charFilterFactories.length : 0][request.text().length];
            TokenListCreator[] tokenFiltersTokenListCreator = new TokenListCreator[tokenFilterFactories != null ?
                tokenFilterFactories.length : 0];

            TokenListCreator tokenizerTokenListCreator = new TokenListCreator(maxTokenCount);

            for (int textIndex = 0; textIndex < request.text().length; textIndex++) {
                String charFilteredSource = request.text()[textIndex];

                Reader reader = new StringReader(charFilteredSource);
                if (charFilterFactories != null) {

                    for (int charFilterIndex = 0; charFilterIndex < charFilterFactories.length; charFilterIndex++) {
                        reader = charFilterFactories[charFilterIndex].create(reader);
                        Reader readerForWriteOut = new StringReader(charFilteredSource);
                        readerForWriteOut = charFilterFactories[charFilterIndex].create(readerForWriteOut);
                        charFilteredSource = writeCharStream(readerForWriteOut);
                        charFiltersTexts[charFilterIndex][textIndex] = charFilteredSource;
                    }
                }

                // analyzing only tokenizer
                Tokenizer tokenizer = tokenizerFactory.create();
                tokenizer.setReader(reader);
                tokenizerTokenListCreator.analyze(tokenizer, includeAttributes, positionIncrementGap, offsetGap);

                // analyzing each tokenfilter
                if (tokenFilterFactories != null) {
                    for (int tokenFilterIndex = 0; tokenFilterIndex < tokenFilterFactories.length; tokenFilterIndex++) {
                        if (tokenFiltersTokenListCreator[tokenFilterIndex] == null) {
                            tokenFiltersTokenListCreator[tokenFilterIndex] = new TokenListCreator(maxTokenCount);
                        }
                        TokenStream stream = createStackedTokenStream(request.text()[textIndex],
                            charFilterFactories, tokenizerFactory, tokenFilterFactories, tokenFilterIndex + 1);
                        tokenFiltersTokenListCreator[tokenFilterIndex].analyze(stream, includeAttributes, positionIncrementGap, offsetGap);
                    }
                }
            }

            AnalyzeAction.CharFilteredText[] charFilteredLists =
                new AnalyzeAction.CharFilteredText[charFiltersTexts.length];

            if (charFilterFactories != null) {
                for (int charFilterIndex = 0; charFilterIndex < charFiltersTexts.length; charFilterIndex++) {
                    charFilteredLists[charFilterIndex] = new AnalyzeAction.CharFilteredText(
                        charFilterFactories[charFilterIndex].name(), charFiltersTexts[charFilterIndex]);
                }
            }
            AnalyzeAction.AnalyzeTokenList[] tokenFilterLists =
                new AnalyzeAction.AnalyzeTokenList[tokenFiltersTokenListCreator.length];

            if (tokenFilterFactories != null) {
                for (int tokenFilterIndex = 0; tokenFilterIndex < tokenFiltersTokenListCreator.length; tokenFilterIndex++) {
                    tokenFilterLists[tokenFilterIndex] = new AnalyzeAction.AnalyzeTokenList(
                        tokenFilterFactories[tokenFilterIndex].name(), tokenFiltersTokenListCreator[tokenFilterIndex].getArrayTokens());
                }
            }
            detailResponse = new AnalyzeAction.DetailAnalyzeResponse(charFilteredLists,
                new AnalyzeAction.AnalyzeTokenList(tokenizerFactory.name(), tokenizerTokenListCreator.getArrayTokens()),
                tokenFilterLists);
        } else {
            String name;
            if (analyzer instanceof NamedAnalyzer) {
                name = ((NamedAnalyzer) analyzer).name();
            } else {
                name = analyzer.getClass().getName();
            }

            TokenListCreator tokenListCreator = new TokenListCreator(maxTokenCount);
            for (String text : request.text()) {
                tokenListCreator.analyze(analyzer.tokenStream("", text), includeAttributes, analyzer.getPositionIncrementGap(""),
                        analyzer.getOffsetGap(""));
            }
            detailResponse
                = new AnalyzeAction.DetailAnalyzeResponse(new AnalyzeAction.AnalyzeTokenList(name, tokenListCreator.getArrayTokens()));
        }
        return detailResponse;
    }

    private static TokenStream createStackedTokenStream(String source, CharFilterFactory[] charFilterFactories,
                                                        TokenizerFactory tokenizerFactory, TokenFilterFactory[] tokenFilterFactories,
                                                        int current) {
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

    private static class TokenCounter{
        private int tokenCount = 0;
        private int maxTokenCount;

        private TokenCounter(int maxTokenCount){
            this.maxTokenCount = maxTokenCount;
        }
        private void increment(){
            tokenCount++;
            if (tokenCount > maxTokenCount) {
                throw new IllegalStateException(
                    "The number of tokens produced by calling _analyze has exceeded the allowed maximum of [" + maxTokenCount + "]."
                        + " This limit can be set by changing the [index.analyze.max_token_count] index level setting.");
            }
        }
    }

    private static class TokenListCreator {
        int lastPosition = -1;
        int lastOffset = 0;
        List<AnalyzeAction.AnalyzeToken> tokens;
        private TokenCounter tc;

        TokenListCreator(int maxTokenCount) {
            tokens = new ArrayList<>();
            tc = new TokenCounter(maxTokenCount);
        }

        private void analyze(TokenStream stream, Set<String> includeAttributes, int positionIncrementGap, int offsetGap) {
            try {
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
                    tokens.add(new AnalyzeAction.AnalyzeToken(term.toString(), lastPosition, lastOffset + offset.startOffset(),
                        lastOffset + offset.endOffset(), posLen.getPositionLength(), type.type(),
                        extractExtendedAttributes(stream, includeAttributes)));
                    tc.increment();
                }
                stream.end();
                lastOffset += offset.endOffset();
                lastPosition += posIncr.getPositionIncrement();

                lastPosition += positionIncrementGap;
                lastOffset += offsetGap;

            } catch (IOException e) {
                throw new ElasticsearchException("failed to analyze", e);
            } finally {
                IOUtils.closeWhileHandlingException(stream);
            }
        }

        private AnalyzeAction.AnalyzeToken[] getArrayTokens() {
            return tokens.toArray(new AnalyzeAction.AnalyzeToken[0]);
        }

    }

    /**
     * other attribute extract object.
     * Extracted object group by AttributeClassName
     *
     * @param stream current TokenStream
     * @param includeAttributes filtering attributes
     * @return Map&lt;key value&gt;
     */
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
