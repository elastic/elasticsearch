/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlChunkedTextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.NlpTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchInferenceResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;

public class TextExpansionProcessor extends NlpTask.Processor {

    private final NlpTask.RequestBuilder requestBuilder;
    private Map<Integer, String> replacementVocab;

    public TextExpansionProcessor(NlpTokenizer tokenizer) {
        super(tokenizer);
        this.requestBuilder = tokenizer.requestBuilder();
        replacementVocab = buildSanitizedVocabMap(tokenizer.getVocabulary());
    }

    static Map<Integer, String> buildSanitizedVocabMap(List<String> inputVocab) {
        Map<Integer, String> sanitized = new HashMap<>();
        for (int i = 0; i < inputVocab.size(); i++) {
            if (inputVocab.get(i).contains(".")) {
                sanitized.put(i, inputVocab.get(i).replace(".", "__"));
            }
        }
        return sanitized;
    }

    @Override
    public void validateInputs(List<String> inputs) {}

    @Override
    public NlpTask.RequestBuilder getRequestBuilder(NlpConfig config) {
        return requestBuilder;
    }

    @Override
    public NlpTask.ResultProcessor getResultProcessor(NlpConfig config) {
        return (tokenization, pyTorchResult, chunkResults) -> processResult(
            tokenization,
            pyTorchResult,
            replacementVocab,
            config.getResultsField(),
            chunkResults
        );
    }

    static InferenceResults processResult(
        TokenizationResult tokenization,
        PyTorchInferenceResult pyTorchResult,
        Map<Integer, String> replacementVocab,
        String resultsField,
        boolean chunkResults
    ) {
        if (chunkResults) {
            var chunkedResults = new ArrayList<MlChunkedTextExpansionResults.ChunkedResult>();

            for (int i = 0; i < pyTorchResult.getInferenceResult()[0].length; i++) {
                String matchedText;
                if (tokenization.getTokenization(i).tokens().get(0).isEmpty() == false) {
                    int startOffset = tokenization.getTokenization(i).tokens().get(0).get(0).startOffset();
                    int lastIndex = tokenization.getTokenization(i).tokens().get(0).size() - 1;
                    int endOffset = tokenization.getTokenization(i).tokens().get(0).get(lastIndex).endOffset();
                    matchedText = tokenization.getTokenization(i).input().get(0).substring(startOffset, endOffset);
                } else {
                    // No tokens in the input, this should only happen with and empty string
                    assert tokenization.getTokenization(i).input().get(0).isEmpty();
                    matchedText = "";
                }

                var weightedTokens = sparseVectorToTokenWeights(pyTorchResult.getInferenceResult()[0][i], tokenization, replacementVocab);
                weightedTokens.sort((t1, t2) -> Float.compare(t2.weight(), t1.weight()));
                chunkedResults.add(new MlChunkedTextExpansionResults.ChunkedResult(matchedText, weightedTokens));
            }

            return new MlChunkedTextExpansionResults(
                Optional.ofNullable(resultsField).orElse(DEFAULT_RESULTS_FIELD),
                chunkedResults,
                tokenization.anyTruncated()
            );
        } else {
            var weightedTokens = sparseVectorToTokenWeights(pyTorchResult.getInferenceResult()[0][0], tokenization, replacementVocab);
            weightedTokens.sort((t1, t2) -> Float.compare(t2.weight(), t1.weight()));
            return new TextExpansionResults(
                Optional.ofNullable(resultsField).orElse(DEFAULT_RESULTS_FIELD),
                weightedTokens,
                tokenization.anyTruncated()
            );
        }
    }

    static List<WeightedToken> sparseVectorToTokenWeights(
        double[] vector,
        TokenizationResult tokenization,
        Map<Integer, String> replacementVocab
    ) {
        // Anything with a score > 0.0 is retained.
        List<WeightedToken> weightedTokens = new ArrayList<>();
        for (int i = 0; i < vector.length; i++) {
            if (vector[i] > 0.0) {
                weightedTokens.add(new WeightedToken(tokenForId(i, tokenization, replacementVocab), (float) vector[i]));
            }
        }
        return weightedTokens;
    }

    static String tokenForId(int id, TokenizationResult tokenization, Map<Integer, String> replacementVocab) {
        String token = replacementVocab.get(id);
        if (token == null) {
            token = tokenization.getFromVocab(id);
        }
        return token;
    }
}
