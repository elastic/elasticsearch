/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.inference.results.NerResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NerConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.DelimitedToken;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.NlpTokenizer;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchInferenceResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;

public class NerProcessor extends NlpTask.Processor {

    record IobTag(String tag, String entity) {
        static IobTag fromTag(String tag) {
            String entity = tag.toUpperCase(Locale.ROOT);
            if (entity.startsWith("B-") || entity.startsWith("I-") || entity.startsWith("B_") || entity.startsWith("I_")) {
                entity = entity.substring(2);
                return new IobTag(tag, entity);
            } else if (entity.equals("O")) {
                return new IobTag(tag, entity);
            } else {
                throw new IllegalArgumentException("classification label [" + tag + "] is not an entity I-O-B tag.");
            }
        }

        boolean isBeginning() {
            return tag.startsWith("b") || tag.startsWith("B");
        }

        boolean isNone() {
            return tag.equals("o") || tag.equals("O");
        }

        @Override
        public String toString() {
            return tag;
        }
    }

    static final IobTag[] DEFAULT_IOB_TAGS = new IobTag[] {
        IobTag.fromTag("O"),       // Outside a named entity
        IobTag.fromTag("B_MISC"),  // Beginning of a miscellaneous entity right after another miscellaneous entity
        IobTag.fromTag("I_MISC"),  // Miscellaneous entity
        IobTag.fromTag("B_PER"),   // Beginning of a person's name right after another person's name
        IobTag.fromTag("I_PER"),   // Person's name
        IobTag.fromTag("B_ORG"),   // Beginning of an organisation right after another organisation
        IobTag.fromTag("I_ORG"),   // Organisation
        IobTag.fromTag("B_LOC"),   // Beginning of a location right after another location
        IobTag.fromTag("I_LOC")    // Location
    };

    private final NlpTask.RequestBuilder requestBuilder;
    private final IobTag[] iobMap;
    private final String resultsField;
    private final boolean ignoreCase;

    NerProcessor(NlpTokenizer tokenizer, NerConfig config) {
        super(tokenizer);
        validate(config.getClassificationLabels());
        this.iobMap = buildIobMap(config.getClassificationLabels());
        this.requestBuilder = tokenizer.requestBuilder();
        this.resultsField = config.getResultsField();
        this.ignoreCase = config.getTokenization().doLowerCase();
    }

    /**
     * Checks labels are valid entity tags and none are duplicated
     */
    private static void validate(List<String> classificationLabels) {
        if (classificationLabels == null || classificationLabels.isEmpty()) {
            return;
        }

        ValidationException ve = new ValidationException();
        Set<IobTag> tags = new HashSet<>();
        for (String label : classificationLabels) {
            try {
                IobTag iobTag = IobTag.fromTag(label);
                if (tags.contains(iobTag)) {
                    ve.addValidationError("the classification label [" + label + "] is duplicated in the list " + classificationLabels);
                }
                tags.add(iobTag);
            } catch (IllegalArgumentException iae) {
                ve.addValidationError("classification label [" + label + "] is not an entity I-O-B tag.");
            }
        }
        if (ve.validationErrors().isEmpty() == false) {
            throw ve;
        }
    }

    static IobTag[] buildIobMap(List<String> classificationLabels) {
        if (classificationLabels == null || classificationLabels.isEmpty()) {
            return DEFAULT_IOB_TAGS;
        }

        IobTag[] map = new IobTag[classificationLabels.size()];
        for (int i = 0; i < classificationLabels.size(); i++) {
            map[i] = IobTag.fromTag(classificationLabels.get(i));
        }
        return map;
    }

    @Override
    public void validateInputs(List<String> inputs) {
        // No validation
    }

    @Override
    public NlpTask.RequestBuilder getRequestBuilder(NlpConfig config) {
        return requestBuilder;
    }

    @Override
    public NlpTask.ResultProcessor getResultProcessor(NlpConfig config) {
        if (config instanceof NerConfig nerConfig) {
            return new NerResultProcessor(iobMap, nerConfig.getResultsField(), ignoreCase);
        }
        return new NerResultProcessor(iobMap, resultsField, ignoreCase);
    }

    static String buildAnnotatedText(String seq, List<NerResults.EntityGroup> entities) {
        if (entities.isEmpty()) {
            return seq;
        }
        StringBuilder annotatedResultBuilder = new StringBuilder();
        int curPos = 0;
        for (var entity : entities) {
            if (entity.getStartPos() == -1) {
                continue;
            }
            if (entity.getStartPos() != curPos) {
                annotatedResultBuilder.append(seq, curPos, entity.getStartPos());
            }
            String entitySeq = seq.substring(entity.getStartPos(), entity.getEndPos());
            annotatedResultBuilder.append("[")
                .append(entitySeq)
                .append("]")
                .append("(")
                .append(entity.getClassName())
                .append("&")
                .append(entitySeq.replace(" ", "+"))
                .append(")");
            curPos = entity.getEndPos();
        }
        if (curPos < seq.length()) {
            annotatedResultBuilder.append(seq, curPos, seq.length());
        }
        return annotatedResultBuilder.toString();
    }

    record NerResultProcessor(IobTag[] iobMap, String resultsField, boolean ignoreCase) implements NlpTask.ResultProcessor {

        NerResultProcessor(IobTag[] iobMap, String resultsField, boolean ignoreCase) {
            this.iobMap = iobMap;
            this.resultsField = Optional.ofNullable(resultsField).orElse(DEFAULT_RESULTS_FIELD);
            this.ignoreCase = ignoreCase;
        }

        @Override
        public InferenceResults processResult(TokenizationResult tokenization, PyTorchInferenceResult pyTorchResult, boolean chunkResult) {
            if (tokenization.isEmpty()) {
                throw new ElasticsearchStatusException("no valid tokenization to build result", RestStatus.INTERNAL_SERVER_ERROR);
            }
            // TODO - process all results in the batch
            if (chunkResult) {
                throw chunkingNotSupportedException(TaskType.NER);
            }

            // TODO It might be best to do the soft max after averaging scores for
            // sub-tokens. If we had a word that is "elastic" which is tokenized to
            // "el" and "astic" then perhaps we get a prediction for org of 10 for "el"
            // and -5 for "astic". Averaging after softmax would produce a prediction
            // of maybe (1 + 0) / 2 = 0.5 while before softmax it'd be exp(10 - 5) / normalization
            // which could easily be close to 1.
            double[][] normalizedScores = NlpHelpers.convertToProbabilitiesBySoftMax(pyTorchResult.getInferenceResult()[0]);
            List<TaggedToken> taggedTokens = tagTokens(tokenization.getTokenization(0), normalizedScores, iobMap);

            List<NerResults.EntityGroup> entities = groupTaggedTokens(
                taggedTokens,
                ignoreCase
                    ? tokenization.getTokenization(0).input().get(0).toLowerCase(Locale.ROOT)
                    : tokenization.getTokenization(0).input().get(0)
            );

            return new NerResults(
                resultsField,
                buildAnnotatedText(tokenization.getTokenization(0).input().get(0), entities),
                entities,
                tokenization.anyTruncated()
            );
        }

        /**
         * Here we tag each token with the IoB label that has the max score.
         * Additionally, we merge sub-tokens that are part of the same word
         * in the original input replacing them with a single token that
         * gets labelled based on the average score of all its sub-tokens.
         */
        static List<TaggedToken> tagTokens(TokenizationResult.Tokens tokenization, double[][] scores, IobTag[] iobMap) {
            List<TaggedToken> taggedTokens = new ArrayList<>();
            int startTokenIndex = 0;
            int numSpecialTokens = 0;
            while (startTokenIndex < tokenization.tokenIds().length) {
                int inputMapping = tokenization.tokenMap()[startTokenIndex];
                if (inputMapping < 0) {
                    // This token does not map to a token in the input (special tokens)
                    startTokenIndex++;
                    numSpecialTokens++;
                    continue;
                }
                int endTokenIndex = startTokenIndex;
                while (endTokenIndex < tokenization.tokenMap().length - 1 && tokenization.tokenMap()[endTokenIndex + 1] == inputMapping) {
                    endTokenIndex++;
                }
                double[] avgScores = Arrays.copyOf(scores[startTokenIndex], iobMap.length);
                for (int i = startTokenIndex + 1; i <= endTokenIndex; i++) {
                    for (int j = 0; j < scores[i].length; j++) {
                        avgScores[j] += scores[i][j];
                    }
                }
                int numTokensInBlock = endTokenIndex - startTokenIndex + 1;
                if (numTokensInBlock > 1) {
                    for (int i = 0; i < avgScores.length; i++) {
                        avgScores[i] /= numTokensInBlock;
                    }
                }
                int maxScoreIndex = NlpHelpers.argmax(avgScores);
                double score = avgScores[maxScoreIndex];
                taggedTokens.add(
                    new TaggedToken(tokenization.tokens().get(0).get(startTokenIndex - numSpecialTokens), iobMap[maxScoreIndex], score)
                );
                startTokenIndex = endTokenIndex + 1;
            }
            return taggedTokens;
        }

        /**
         * Now that we have merged sub-tokens and tagged them with their IoB label,
         * we group tokens together into the final entity groups. Effectively,
         * we want to group B_X I_X B_X so that it results into two
         * entities, one for the first B_X I_X and another for the latter B_X,
         * where X is the same entity.
         * When multiple tokens are grouped together, the entity score is the
         * mean score of the tokens.
         */
        static List<NerResults.EntityGroup> groupTaggedTokens(List<TaggedToken> tokens, String inputSeq) {
            if (tokens.isEmpty()) {
                return Collections.emptyList();
            }
            List<NerResults.EntityGroup> entities = new ArrayList<>();
            int startTokenIndex = 0;
            while (startTokenIndex < tokens.size()) {
                TaggedToken token = tokens.get(startTokenIndex);
                if (token.tag.isNone()) {
                    startTokenIndex++;
                    continue;
                }
                int endTokenIndex = startTokenIndex + 1;
                double scoreSum = token.score;
                while (endTokenIndex < tokens.size()) {
                    TaggedToken endToken = tokens.get(endTokenIndex);
                    if (endToken.tag.isBeginning() || endToken.tag.entity().equals(token.tag.entity()) == false) {
                        break;
                    }
                    scoreSum += endToken.score;
                    endTokenIndex++;
                }

                int startPos = token.token.startOffset();
                int endPos = tokens.get(endTokenIndex - 1).token.endOffset();
                String entity = inputSeq.substring(startPos, endPos);
                entities.add(
                    new NerResults.EntityGroup(entity, token.tag.entity(), scoreSum / (endTokenIndex - startTokenIndex), startPos, endPos)
                );
                startTokenIndex = endTokenIndex;
            }

            return entities;
        }

        record TaggedToken(DelimitedToken token, IobTag tag, double score) {
            @Override
            public String toString() {
                return "{" + "token:" + token + ", " + tag + ", " + score + "}";
            }
        }
    }
}
