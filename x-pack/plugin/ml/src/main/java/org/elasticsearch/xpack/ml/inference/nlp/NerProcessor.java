/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.NerResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NerConfig;
import org.elasticsearch.xpack.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;

public class NerProcessor implements NlpTask.Processor {

    public enum Entity implements Writeable {
        NONE, MISC, PERSON, ORGANISATION, LOCATION;

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    // Inside-Outside-Beginning (IOB) tag
    enum IobTag {
        O(Entity.NONE),                 // Outside of a named entity
        B_MISC(Entity.MISC),            // Beginning of a miscellaneous entity right after another miscellaneous entity
        I_MISC(Entity.MISC),            // Miscellaneous entity
        B_PER(Entity.PERSON),           // Beginning of a person's name right after another person's name
        I_PER(Entity.PERSON),           // Person's name
        B_ORG(Entity.ORGANISATION),     // Beginning of an organisation right after another organisation
        I_ORG(Entity.ORGANISATION),     // Organisation
        B_LOC(Entity.LOCATION),         // Beginning of a location right after another location
        I_LOC(Entity.LOCATION);         // Location

        private final Entity entity;

        IobTag(Entity entity) {
            this.entity = entity;
        }

        Entity getEntity() {
            return entity;
        }

        boolean isBeginning() {
            return name().toLowerCase(Locale.ROOT).startsWith("b");
        }
    }

    private final BertRequestBuilder bertRequestBuilder;
    private final IobTag[] iobMap;

    NerProcessor(BertTokenizer tokenizer, NerConfig config) {
        this.bertRequestBuilder = new BertRequestBuilder(tokenizer, config.getTokenizationParams().maxSequenceLength());
        validate(config.getClassificationLabels());
        iobMap = buildIobMap(config.getClassificationLabels());
    }

    /**
     * Checks labels are valid entity tags and none are duplicated
     */
    private void validate(List<String> classificationLabels) {
        if (classificationLabels == null || classificationLabels.isEmpty()) {
            return;
        }

        ValidationException ve = new ValidationException();
        EnumSet<IobTag> tags = EnumSet.noneOf(IobTag.class);
        for (String label : classificationLabels) {
            try {
                IobTag iobTag = IobTag.valueOf(label);
                if (tags.contains(iobTag)) {
                    ve.addValidationError("the classification label [" + label + "] is duplicated in the list " + classificationLabels);
                }
                tags.add(iobTag);
            } catch (IllegalArgumentException iae) {
                ve.addValidationError("classification label [" + label + "] is not an entity I-O-B tag.");
            }
        }

        if (ve.validationErrors().isEmpty() == false) {
            ve.addValidationError("Valid entity I-O-B tags are " + Arrays.toString(IobTag.values()));
            throw ve;
        }
    }

    static IobTag[] buildIobMap(List<String> classificationLabels) {
        if (classificationLabels == null || classificationLabels.isEmpty()) {
            return IobTag.values();
        }

        IobTag[] map = new IobTag[classificationLabels.size()];
        for (int i=0; i<classificationLabels.size(); i++) {
            map[i] = IobTag.valueOf(classificationLabels.get(i));
        }

        return map;
    }

    @Override
    public void validateInputs(String inputs) {
        // No validation
    }

    @Override
    public NlpTask.RequestBuilder getRequestBuilder() {
        return bertRequestBuilder;
    }

    @Override
    public NlpTask.ResultProcessor getResultProcessor() {
        return new NerResultProcessor(iobMap);
    }

    static class NerResultProcessor implements NlpTask.ResultProcessor {

        private final IobTag[] iobMap;

        NerResultProcessor(IobTag[] iobMap) {
            this.iobMap = iobMap;
        }

        @Override
        public InferenceResults processResult(BertTokenizer.TokenizationResult tokenization, PyTorchResult pyTorchResult) {
            if (tokenization.getTokens().isEmpty()) {
                return new NerResults(Collections.emptyList());
            }
            // TODO It might be best to do the soft max after averaging scores for
            // sub-tokens. If we had a word that is "elastic" which is tokenized to
            // "el" and "astic" then perhaps we get a prediction for org of 10 for "el"
            // and -5 for "astic". Averaging after softmax would produce a prediction
            // of maybe (1 + 0) / 2 = 0.5 while before softmax it'd be exp(10 - 5) / normalization
            // which could easily be close to 1.
            double[][] normalizedScores = NlpHelpers.convertToProbabilitiesBySoftMax(pyTorchResult.getInferenceResult());
            List<TaggedToken> taggedTokens = tagTokens(tokenization, normalizedScores);
            List<NerResults.EntityGroup> entities = groupTaggedTokens(taggedTokens);
            return new NerResults(entities);
        }

        /**
         * Here we tag each token with the IoB label that has the max score.
         * Additionally, we merge sub-tokens that are part of the same word
         * in the original input replacing them with a single token that
         * gets labelled based on the average score of all its sub-tokens.
         */
        private List<TaggedToken> tagTokens(BertTokenizer.TokenizationResult tokenization, double[][] scores) {
            List<TaggedToken> taggedTokens = new ArrayList<>();
            int startTokenIndex = 0;
            while (startTokenIndex < tokenization.getTokens().size()) {
                int inputMapping = tokenization.getTokenMap()[startTokenIndex];
                if (inputMapping < 0) {
                    // This token does not map to a token in the input (special tokens)
                    startTokenIndex++;
                    continue;
                }
                int endTokenIndex = startTokenIndex;
                StringBuilder word = new StringBuilder(tokenization.getTokens().get(startTokenIndex));
                while (endTokenIndex < tokenization.getTokens().size() - 1
                    && tokenization.getTokenMap()[endTokenIndex + 1] == inputMapping) {
                    endTokenIndex++;
                    // TODO Here we try to get rid of the continuation hashes at the beginning of sub-tokens.
                    // It is probably more correct to implement detokenization on the tokenizer
                    // that does reverse lookup based on token IDs.
                    String endTokenWord = tokenization.getTokens().get(endTokenIndex).substring(2);
                    word.append(endTokenWord);
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
                taggedTokens.add(new TaggedToken(word.toString(), iobMap[maxScoreIndex], score));
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
        static List<NerResults.EntityGroup> groupTaggedTokens(List<TaggedToken> tokens) {
            if (tokens.isEmpty()) {
                return Collections.emptyList();
            }
            List<NerResults.EntityGroup> entities = new ArrayList<>();
            int startTokenIndex = 0;
            while (startTokenIndex < tokens.size()) {
                TaggedToken token = tokens.get(startTokenIndex);
                if (token.tag.getEntity() == Entity.NONE) {
                    startTokenIndex++;
                    continue;
                }
                StringBuilder entityWord = new StringBuilder(token.word);
                int endTokenIndex = startTokenIndex + 1;
                double scoreSum = token.score;
                while (endTokenIndex < tokens.size()) {
                    TaggedToken endToken = tokens.get(endTokenIndex);
                    if (endToken.tag.isBeginning() || endToken.tag.getEntity() != token.tag.getEntity()) {
                        break;
                    }
                    // TODO Here we add a space between tokens.
                    // It is probably more correct to implement detokenization on the tokenizer
                    // that does reverse lookup based on token IDs.
                    entityWord.append(" ").append(endToken.word);
                    scoreSum += endToken.score;
                    endTokenIndex++;
                }
                entities.add(new NerResults.EntityGroup(token.tag.getEntity().toString(),
                    scoreSum / (endTokenIndex - startTokenIndex), entityWord.toString()));
                startTokenIndex = endTokenIndex;
            }

            return entities;
        }

        static class TaggedToken {
            private final String word;
            private final IobTag tag;
            private final double score;

            TaggedToken(String word, IobTag tag, double score) {
                this.word = word;
                this.tag = tag;
                this.score = score;
            }
        }
    }
}
