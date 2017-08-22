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
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.CustomAnalyzerProvider;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.MultiTermAwareComponent;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.mapper.AllFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.io.Reader;
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
public class TransportAnalyzeAction extends TransportSingleShardAction<AnalyzeRequest, AnalyzeResponse> {

    private final IndicesService indicesService;
    private final Environment environment;

    @Inject
    public TransportAnalyzeAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                  IndicesService indicesService, ActionFilters actionFilters,
                                  IndexNameExpressionResolver indexNameExpressionResolver, Environment environment) {
        super(settings, AnalyzeAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver, AnalyzeRequest::new, ThreadPool.Names.INDEX);
        this.indicesService = indicesService;
        this.environment = environment;
    }

    @Override
    protected AnalyzeResponse newResponse() {
        return new AnalyzeResponse();
    }

    @Override
    protected boolean resolveIndex(AnalyzeRequest request) {
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
    protected AnalyzeResponse shardOperation(AnalyzeRequest request, ShardId shardId) {
        try {
            final IndexService indexService;
            if (shardId != null) {
                indexService = indicesService.indexServiceSafe(shardId.getIndex());
            } else {
                indexService = null;
            }
            String field = null;
            Analyzer analyzer = null;
            if (request.field() != null) {
                if (indexService == null) {
                    throw new IllegalArgumentException("No index provided, and trying to analyzer based on a specific field which requires the index parameter");
                }
                MappedFieldType fieldType = indexService.mapperService().fullName(request.field());
                if (fieldType != null) {
                    if (fieldType.tokenized()) {
                        analyzer = fieldType.indexAnalyzer();
                    } else if (fieldType instanceof KeywordFieldMapper.KeywordFieldType) {
                        analyzer = ((KeywordFieldMapper.KeywordFieldType) fieldType).normalizer();
                        if (analyzer == null) {
                            // this will be KeywordAnalyzer
                            analyzer = fieldType.indexAnalyzer();
                        }
                    } else {
                        throw new IllegalArgumentException("Can't process field [" + request.field() + "], Analysis requests are only supported on tokenized fields");
                    }
                    field = fieldType.name();
                }
            }
            if (field == null) {
                /**
                 * TODO: _all is disabled by default and index.query.default_field can define multiple fields or pattterns so we should
                 * probably makes the field name mandatory in analyze query.
                 **/
                if (indexService != null) {
                    field = indexService.getIndexSettings().getDefaultFields().get(0);
                } else {
                    field = AllFieldMapper.NAME;
                }
            }
            final AnalysisRegistry analysisRegistry = indicesService.getAnalysis();
            return analyze(request, field, analyzer, indexService != null ? indexService.getIndexAnalyzers() : null, analysisRegistry, environment);
        } catch (IOException e) {
            throw new ElasticsearchException("analysis failed", e);
        }

    }

    public static AnalyzeResponse analyze(AnalyzeRequest request, String field, Analyzer analyzer, IndexAnalyzers indexAnalyzers, AnalysisRegistry analysisRegistry, Environment environment) throws IOException {

        boolean closeAnalyzer = false;
        if (analyzer == null && request.analyzer() != null) {
            if (indexAnalyzers == null) {
                analyzer = analysisRegistry.getAnalyzer(request.analyzer());
                if (analyzer == null) {
                    throw new IllegalArgumentException("failed to find global analyzer [" + request.analyzer() + "]");
                }
            } else {
                analyzer = indexAnalyzers.get(request.analyzer());
                if (analyzer == null) {
                    throw new IllegalArgumentException("failed to find analyzer [" + request.analyzer() + "]");
                }
            }
        } else if (request.tokenizer() != null) {
            final IndexSettings indexSettings = indexAnalyzers == null ? null : indexAnalyzers.getIndexSettings();
            Tuple<String, TokenizerFactory> tokenizerFactory = parseTokenizerFactory(request, indexAnalyzers,
                        analysisRegistry, environment);

            List<CharFilterFactory> charFilterFactoryList =
                parseCharFilterFactories(request, indexSettings, analysisRegistry, environment, false);

            List<TokenFilterFactory> tokenFilterFactoryList = parseTokenFilterFactories(request, indexSettings, analysisRegistry,
                environment, tokenizerFactory, charFilterFactoryList, false);

            analyzer = new CustomAnalyzer(tokenizerFactory.v1(), tokenizerFactory.v2(),
                charFilterFactoryList.toArray(new CharFilterFactory[charFilterFactoryList.size()]),
                tokenFilterFactoryList.toArray(new TokenFilterFactory[tokenFilterFactoryList.size()]));
            closeAnalyzer = true;
        } else if (request.normalizer() != null) {
            // Get normalizer from indexAnalyzers
            analyzer = indexAnalyzers.getNormalizer(request.normalizer());
            if (analyzer == null) {
                throw new IllegalArgumentException("failed to find normalizer under [" + request.normalizer() + "]");
            }
        } else if (((request.tokenFilters() != null && request.tokenFilters().size() > 0)
                || (request.charFilters() != null && request.charFilters().size() > 0))) {
            final IndexSettings indexSettings = indexAnalyzers == null ? null : indexAnalyzers.getIndexSettings();
            // custom normalizer = if normalizer == null but filter or char_filter is not null and tokenizer/analyzer is null
            // get charfilter and filter from request
            List<CharFilterFactory> charFilterFactoryList =
                parseCharFilterFactories(request, indexSettings, analysisRegistry, environment, true);

            final String keywordTokenizerName = "keyword";
            TokenizerFactory keywordTokenizerFactory = getTokenizerFactory(analysisRegistry, environment, keywordTokenizerName);

            List<TokenFilterFactory> tokenFilterFactoryList =
                parseTokenFilterFactories(request, indexSettings, analysisRegistry, environment, new Tuple<>(keywordTokenizerName, keywordTokenizerFactory), charFilterFactoryList, true);

            analyzer = new CustomAnalyzer("keyword_for_normalizer",
                keywordTokenizerFactory,
                charFilterFactoryList.toArray(new CharFilterFactory[charFilterFactoryList.size()]),
                tokenFilterFactoryList.toArray(new TokenFilterFactory[tokenFilterFactoryList.size()]));
            closeAnalyzer = true;
        } else if (analyzer == null) {
            if (indexAnalyzers == null) {
                analyzer = analysisRegistry.getAnalyzer("standard");
            } else {
                analyzer = indexAnalyzers.getDefaultIndexAnalyzer();
            }
        }
        if (analyzer == null) {
            throw new IllegalArgumentException("failed to find analyzer");
        }

        List<AnalyzeResponse.AnalyzeToken> tokens = null;
        DetailAnalyzeResponse detail = null;

        if (request.explain()) {
            detail = detailAnalyze(request, analyzer, field);
        } else {
            tokens = simpleAnalyze(request, analyzer, field);
        }

        if (closeAnalyzer) {
            analyzer.close();
        }

        return new AnalyzeResponse(tokens, detail);
    }

    private static List<AnalyzeResponse.AnalyzeToken> simpleAnalyze(AnalyzeRequest request, Analyzer analyzer, String field) {
        List<AnalyzeResponse.AnalyzeToken> tokens = new ArrayList<>();
        int lastPosition = -1;
        int lastOffset = 0;
        for (String text : request.text()) {
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
                    tokens.add(new AnalyzeResponse.AnalyzeToken(term.toString(), lastPosition, lastOffset + offset.startOffset(),
                        lastOffset + offset.endOffset(), posLen.getPositionLength(), type.type(), null));

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

    private static DetailAnalyzeResponse detailAnalyze(AnalyzeRequest request, Analyzer analyzer, String field) {
        DetailAnalyzeResponse detailResponse;
        final Set<String> includeAttributes = new HashSet<>();
        if (request.attributes() != null) {
            for (String attribute : request.attributes()) {
                includeAttributes.add(attribute.toLowerCase(Locale.ROOT));
            }
        }

        CustomAnalyzer customAnalyzer = null;
        if (analyzer instanceof CustomAnalyzer) {
            customAnalyzer = (CustomAnalyzer) analyzer;
        } else if (analyzer instanceof NamedAnalyzer && ((NamedAnalyzer) analyzer).analyzer() instanceof CustomAnalyzer) {
            customAnalyzer = (CustomAnalyzer) ((NamedAnalyzer) analyzer).analyzer();
        }

        if (customAnalyzer != null) {
            // customAnalyzer = divide charfilter, tokenizer tokenfilters
            CharFilterFactory[] charFilterFactories = customAnalyzer.charFilters();
            TokenizerFactory tokenizerFactory = customAnalyzer.tokenizerFactory();
            TokenFilterFactory[] tokenFilterFactories = customAnalyzer.tokenFilters();

            String[][] charFiltersTexts = new String[charFilterFactories != null ? charFilterFactories.length : 0][request.text().length];
            TokenListCreator[] tokenFiltersTokenListCreator = new TokenListCreator[tokenFilterFactories != null ? tokenFilterFactories.length : 0];

            TokenListCreator tokenizerTokenListCreator = new TokenListCreator();

            for (int textIndex = 0; textIndex < request.text().length; textIndex++) {
                String charFilteredSource = request.text()[textIndex];

                Reader reader = new FastStringReader(charFilteredSource);
                if (charFilterFactories != null) {

                    for (int charFilterIndex = 0; charFilterIndex < charFilterFactories.length; charFilterIndex++) {
                        reader = charFilterFactories[charFilterIndex].create(reader);
                        Reader readerForWriteOut = new FastStringReader(charFilteredSource);
                        readerForWriteOut = charFilterFactories[charFilterIndex].create(readerForWriteOut);
                        charFilteredSource = writeCharStream(readerForWriteOut);
                        charFiltersTexts[charFilterIndex][textIndex] = charFilteredSource;
                    }
                }

                // analyzing only tokenizer
                Tokenizer tokenizer = tokenizerFactory.create();
                tokenizer.setReader(reader);
                tokenizerTokenListCreator.analyze(tokenizer, customAnalyzer, field, includeAttributes);

                // analyzing each tokenfilter
                if (tokenFilterFactories != null) {
                    for (int tokenFilterIndex = 0; tokenFilterIndex < tokenFilterFactories.length; tokenFilterIndex++) {
                        if (tokenFiltersTokenListCreator[tokenFilterIndex] == null) {
                            tokenFiltersTokenListCreator[tokenFilterIndex] = new TokenListCreator();
                        }
                        TokenStream stream = createStackedTokenStream(request.text()[textIndex],
                            charFilterFactories, tokenizerFactory, tokenFilterFactories, tokenFilterIndex + 1);
                        tokenFiltersTokenListCreator[tokenFilterIndex].analyze(stream, customAnalyzer, field, includeAttributes);
                    }
                }
            }

            DetailAnalyzeResponse.CharFilteredText[] charFilteredLists = new DetailAnalyzeResponse.CharFilteredText[charFiltersTexts.length];
            if (charFilterFactories != null) {
                for (int charFilterIndex = 0; charFilterIndex < charFiltersTexts.length; charFilterIndex++) {
                    charFilteredLists[charFilterIndex] = new DetailAnalyzeResponse.CharFilteredText(
                        charFilterFactories[charFilterIndex].name(), charFiltersTexts[charFilterIndex]);
                }
            }
            DetailAnalyzeResponse.AnalyzeTokenList[] tokenFilterLists = new DetailAnalyzeResponse.AnalyzeTokenList[tokenFiltersTokenListCreator.length];
            if (tokenFilterFactories != null) {
                for (int tokenFilterIndex = 0; tokenFilterIndex < tokenFiltersTokenListCreator.length; tokenFilterIndex++) {
                    tokenFilterLists[tokenFilterIndex] = new DetailAnalyzeResponse.AnalyzeTokenList(
                        tokenFilterFactories[tokenFilterIndex].name(), tokenFiltersTokenListCreator[tokenFilterIndex].getArrayTokens());
                }
            }
            detailResponse = new DetailAnalyzeResponse(charFilteredLists, new DetailAnalyzeResponse.AnalyzeTokenList(
                    customAnalyzer.getTokenizerName(), tokenizerTokenListCreator.getArrayTokens()), tokenFilterLists);
        } else {
            String name;
            if (analyzer instanceof NamedAnalyzer) {
                name = ((NamedAnalyzer) analyzer).name();
            } else {
                name = analyzer.getClass().getName();
            }

            TokenListCreator tokenListCreator = new TokenListCreator();
            for (String text : request.text()) {
                tokenListCreator.analyze(analyzer.tokenStream(field, text), analyzer, field,
                        includeAttributes);
            }
            detailResponse = new DetailAnalyzeResponse(new DetailAnalyzeResponse.AnalyzeTokenList(name, tokenListCreator.getArrayTokens()));
        }
        return detailResponse;
    }

    private static TokenStream createStackedTokenStream(String source, CharFilterFactory[] charFilterFactories, TokenizerFactory tokenizerFactory, TokenFilterFactory[] tokenFilterFactories, int current) {
        Reader reader = new FastStringReader(source);
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

    private static class TokenListCreator {
        int lastPosition = -1;
        int lastOffset = 0;
        List<AnalyzeResponse.AnalyzeToken> tokens;

        TokenListCreator() {
            tokens = new ArrayList<>();
        }

        private void analyze(TokenStream stream, Analyzer analyzer, String field, Set<String> includeAttributes) {
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
                    tokens.add(new AnalyzeResponse.AnalyzeToken(term.toString(), lastPosition, lastOffset + offset.startOffset(),
                        lastOffset + offset.endOffset(), posLen.getPositionLength(), type.type(), extractExtendedAttributes(stream, includeAttributes)));

                }
                stream.end();
                lastOffset += offset.endOffset();
                lastPosition += posIncr.getPositionIncrement();

                lastPosition += analyzer.getPositionIncrementGap(field);
                lastOffset += analyzer.getOffsetGap(field);

            } catch (IOException e) {
                throw new ElasticsearchException("failed to analyze", e);
            } finally {
                IOUtils.closeWhileHandlingException(stream);
            }
        }

        private AnalyzeResponse.AnalyzeToken[] getArrayTokens() {
            return tokens.toArray(new AnalyzeResponse.AnalyzeToken[tokens.size()]);
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

    private static List<CharFilterFactory> parseCharFilterFactories(AnalyzeRequest request, IndexSettings indexSettings, AnalysisRegistry analysisRegistry,
                                                              Environment environment, boolean normalizer) throws IOException {
        List<CharFilterFactory> charFilterFactoryList = new ArrayList<>();
        if (request.charFilters() != null && request.charFilters().size() > 0) {
            List<AnalyzeRequest.NameOrDefinition> charFilters = request.charFilters();
            for (AnalyzeRequest.NameOrDefinition charFilter : charFilters) {
                CharFilterFactory charFilterFactory;
                // parse anonymous settings
                if (charFilter.definition != null) {
                    Settings settings = getAnonymousSettings(charFilter.definition);
                    String charFilterTypeName = settings.get("type");
                    if (charFilterTypeName == null) {
                        throw new IllegalArgumentException("Missing [type] setting for anonymous char filter: " + charFilter.definition);
                    }
                    AnalysisModule.AnalysisProvider<CharFilterFactory> charFilterFactoryFactory =
                        analysisRegistry.getCharFilterProvider(charFilterTypeName);
                    if (charFilterFactoryFactory == null) {
                        throw new IllegalArgumentException("failed to find global char filter under [" + charFilterTypeName + "]");
                    }
                    // Need to set anonymous "name" of char_filter
                    charFilterFactory = charFilterFactoryFactory.get(getNaIndexSettings(settings), environment, "_anonymous_charfilter", settings);
                } else {
                    AnalysisModule.AnalysisProvider<CharFilterFactory> charFilterFactoryFactory;
                    if (indexSettings == null) {
                        charFilterFactoryFactory = analysisRegistry.getCharFilterProvider(charFilter.name);
                        if (charFilterFactoryFactory == null) {
                            throw new IllegalArgumentException("failed to find global char filter under [" + charFilter.name + "]");
                        }
                        charFilterFactory = charFilterFactoryFactory.get(environment, charFilter.name);
                    } else {
                        charFilterFactoryFactory = analysisRegistry.getCharFilterProvider(charFilter.name, indexSettings);
                        if (charFilterFactoryFactory == null) {
                            throw new IllegalArgumentException("failed to find char filter under [" + charFilter.name + "]");
                        }
                        charFilterFactory = charFilterFactoryFactory.get(indexSettings, environment, charFilter.name,
                            AnalysisRegistry.getSettingsFromIndexSettings(indexSettings,
                                AnalysisRegistry.INDEX_ANALYSIS_CHAR_FILTER + "." + charFilter.name));
                    }
                }
                if (charFilterFactory == null) {
                    throw new IllegalArgumentException("failed to find char filter under [" + charFilter.name + "]");
                }
                if (normalizer) {
                    if (charFilterFactory instanceof MultiTermAwareComponent == false) {
                        throw new IllegalArgumentException("Custom normalizer may not use char filter ["
                            + charFilterFactory.name() + "]");
                    }
                    charFilterFactory = (CharFilterFactory) ((MultiTermAwareComponent) charFilterFactory).getMultiTermComponent();
                }
                charFilterFactoryList.add(charFilterFactory);
            }
        }
        return charFilterFactoryList;
    }

    private static List<TokenFilterFactory> parseTokenFilterFactories(AnalyzeRequest request, IndexSettings indexSettings, AnalysisRegistry analysisRegistry,
                                                                Environment environment, Tuple<String, TokenizerFactory> tokenizerFactory,
                                                                List<CharFilterFactory> charFilterFactoryList, boolean normalizer) throws IOException {
        List<TokenFilterFactory> tokenFilterFactoryList = new ArrayList<>();
        if (request.tokenFilters() != null && request.tokenFilters().size() > 0) {
            List<AnalyzeRequest.NameOrDefinition> tokenFilters = request.tokenFilters();
            for (AnalyzeRequest.NameOrDefinition tokenFilter : tokenFilters) {
                TokenFilterFactory tokenFilterFactory;
                // parse anonymous settings
                if (tokenFilter.definition != null) {
                    Settings settings = getAnonymousSettings(tokenFilter.definition);
                    String filterTypeName = settings.get("type");
                    if (filterTypeName == null) {
                        throw new IllegalArgumentException("Missing [type] setting for anonymous token filter: " + tokenFilter.definition);
                    }
                    AnalysisModule.AnalysisProvider<TokenFilterFactory> tokenFilterFactoryFactory =
                        analysisRegistry.getTokenFilterProvider(filterTypeName);
                    if (tokenFilterFactoryFactory == null) {
                        throw new IllegalArgumentException("failed to find global token filter under [" + filterTypeName + "]");
                    }
                    // Need to set anonymous "name" of tokenfilter
                    tokenFilterFactory = tokenFilterFactoryFactory.get(getNaIndexSettings(settings), environment, "_anonymous_tokenfilter", settings);
                    tokenFilterFactory = CustomAnalyzerProvider.checkAndApplySynonymFilter(tokenFilterFactory, tokenizerFactory.v1(), tokenizerFactory.v2(), tokenFilterFactoryList,
                        charFilterFactoryList, environment);


                } else {
                    AnalysisModule.AnalysisProvider<TokenFilterFactory> tokenFilterFactoryFactory;
                    if (indexSettings == null) {
                        tokenFilterFactoryFactory = analysisRegistry.getTokenFilterProvider(tokenFilter.name);
                        if (tokenFilterFactoryFactory == null) {
                            throw new IllegalArgumentException("failed to find global token filter under [" + tokenFilter.name + "]");
                        }
                        tokenFilterFactory = tokenFilterFactoryFactory.get(environment, tokenFilter.name);
                    } else {
                        tokenFilterFactoryFactory = analysisRegistry.getTokenFilterProvider(tokenFilter.name, indexSettings);
                        if (tokenFilterFactoryFactory == null) {
                            throw new IllegalArgumentException("failed to find token filter under [" + tokenFilter.name + "]");
                        }
                        Settings settings = AnalysisRegistry.getSettingsFromIndexSettings(indexSettings,
                            AnalysisRegistry.INDEX_ANALYSIS_FILTER + "." + tokenFilter.name);
                        tokenFilterFactory = tokenFilterFactoryFactory.get(indexSettings, environment, tokenFilter.name, settings);
                        tokenFilterFactory = CustomAnalyzerProvider.checkAndApplySynonymFilter(tokenFilterFactory, tokenizerFactory.v1(), tokenizerFactory.v2(), tokenFilterFactoryList,
                            charFilterFactoryList, environment);
                    }
                }
                if (tokenFilterFactory == null) {
                    throw new IllegalArgumentException("failed to find or create token filter under [" + tokenFilter.name + "]");
                }
                if (normalizer) {
                    if (tokenFilterFactory instanceof MultiTermAwareComponent == false) {
                        throw new IllegalArgumentException("Custom normalizer may not use filter ["
                            + tokenFilterFactory.name() + "]");
                    }
                    tokenFilterFactory = (TokenFilterFactory) ((MultiTermAwareComponent) tokenFilterFactory).getMultiTermComponent();
                }
                tokenFilterFactoryList.add(tokenFilterFactory);
            }
        }
        return tokenFilterFactoryList;
    }

    private static Tuple<String, TokenizerFactory> parseTokenizerFactory(AnalyzeRequest request, IndexAnalyzers indexAnalzyers,
                                                          AnalysisRegistry analysisRegistry, Environment environment) throws IOException {
        String name;
        TokenizerFactory tokenizerFactory;
        final AnalyzeRequest.NameOrDefinition tokenizer = request.tokenizer();
        // parse anonymous settings
        if (tokenizer.definition != null) {
            Settings settings = getAnonymousSettings(tokenizer.definition);
            String tokenizerTypeName = settings.get("type");
            if (tokenizerTypeName == null) {
                throw new IllegalArgumentException("Missing [type] setting for anonymous tokenizer: " + tokenizer.definition);
            }
            AnalysisModule.AnalysisProvider<TokenizerFactory> tokenizerFactoryFactory =
                analysisRegistry.getTokenizerProvider(tokenizerTypeName);
            if (tokenizerFactoryFactory == null) {
                throw new IllegalArgumentException("failed to find global tokenizer under [" + tokenizerTypeName + "]");
            }
            // Need to set anonymous "name" of tokenizer
            name = "_anonymous_tokenizer";
            tokenizerFactory = tokenizerFactoryFactory.get(getNaIndexSettings(settings), environment, "_anonymous_tokenizer", settings);
        } else {
            AnalysisModule.AnalysisProvider<TokenizerFactory> tokenizerFactoryFactory;
            if (indexAnalzyers == null) {
                tokenizerFactory = getTokenizerFactory(analysisRegistry, environment, tokenizer.name);
                name = tokenizer.name;
            } else {
                tokenizerFactoryFactory = analysisRegistry.getTokenizerProvider(tokenizer.name, indexAnalzyers.getIndexSettings());
                if (tokenizerFactoryFactory == null) {
                    throw new IllegalArgumentException("failed to find tokenizer under [" + tokenizer.name + "]");
                }
                name = tokenizer.name;
                tokenizerFactory = tokenizerFactoryFactory.get(indexAnalzyers.getIndexSettings(), environment, tokenizer.name,
                    AnalysisRegistry.getSettingsFromIndexSettings(indexAnalzyers.getIndexSettings(),
                        AnalysisRegistry.INDEX_ANALYSIS_TOKENIZER + "." + tokenizer.name));
            }
        }
        return new Tuple<>(name, tokenizerFactory);
    }

    private static TokenizerFactory getTokenizerFactory(AnalysisRegistry analysisRegistry, Environment environment, String name) throws IOException {
        AnalysisModule.AnalysisProvider<TokenizerFactory> tokenizerFactoryFactory;
        TokenizerFactory tokenizerFactory;
        tokenizerFactoryFactory = analysisRegistry.getTokenizerProvider(name);
        if (tokenizerFactoryFactory == null) {
            throw new IllegalArgumentException("failed to find global tokenizer under [" + name + "]");
        }
        tokenizerFactory = tokenizerFactoryFactory.get(environment, name);
        return tokenizerFactory;
    }

    private static IndexSettings getNaIndexSettings(Settings settings) {
        IndexMetaData metaData = IndexMetaData.builder(IndexMetaData.INDEX_UUID_NA_VALUE).settings(settings).build();
        return new IndexSettings(metaData, Settings.EMPTY);
    }

    private static Settings getAnonymousSettings(Settings providerSetting) {
        return Settings.builder().put(providerSetting)
            // for _na_
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();
    }

}
