/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization2;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.xpack.ml.aggs.categorization2.TokenListCategory.TokenAndWeight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.lucene.util.RamUsageEstimator.sizeOfCollection;
import static org.elasticsearch.xpack.ml.aggs.categorization2.CategorizeTextAggregationBuilder.MAX_MAX_MATCHED_TOKENS;

/**
 * Port of the C++ class <a href="https://github.com/elastic/ml-cpp/blob/main/include/model/CTokenListDataCategorizerBase.h">
 * <code>CTokenListDataCategorizerBase</code></a> and parts of its base class and derived class.
 */
public class TokenListCategorizer implements Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TokenListCategorizer.class);
    private static final float EPSILON = 0.000001f;
    private static final Logger logger = LogManager.getLogger(TokenListCategorizer.class);

    /**
     * The lower threshold for comparison. If another category matches this
     * closely, we'll take it providing there's no other better match.
     */
    private final float lowerThreshold;

    /**
     * The upper threshold for comparison. If another category matches this
     * closely, we accept it immediately (i.e. don't look for a better one).
     */
    private final float upperThreshold;

    private final CategorizationBytesRefHash bytesRefHash;
    @Nullable
    private final CategorizationPartOfSpeechDictionary partOfSpeechDictionary;

    /**
     * Categories stored in such a way that the most common are accessed first.
     * This is implemented as an {@link ArrayList} with bespoke ordering rather
     * than a {@link PriorityQueue} to make the regular modification and indexing
     * possible.
     */
    private final List<TokenListCategory> categoriesByNumMatches;

    public TokenListCategorizer(
        CategorizationBytesRefHash bytesRefHash,
        CategorizationPartOfSpeechDictionary partOfSpeechDictionary,
        float threshold
    ) {

        if (threshold < 0.01f || threshold > 0.99f) {
            throw new IllegalArgumentException("threshold must be between 0.01 and 0.99: got " + threshold);
        }

        this.bytesRefHash = bytesRefHash;
        this.partOfSpeechDictionary = partOfSpeechDictionary;
        this.lowerThreshold = threshold;
        this.upperThreshold = (1.0f + threshold) / 2.0f;
        this.categoriesByNumMatches = new ArrayList<>();
    }

    public TokenListCategory computeCategory(TokenStream ts, int unfilteredStringLen, long numDocs) throws IOException {
        assert partOfSpeechDictionary != null
            : "This version of computeCategory should only be used when a part-of-speech dictionary is available";
        if (numDocs <= 0) {
            assert numDocs == 0 : "number of documents was negative: " + numDocs;
            return null;
        }
        ArrayList<TokenAndWeight> weightedTokenIds = new ArrayList<>();
        CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
        ts.reset();
        WeightCalculator weightCalculator = new WeightCalculator(partOfSpeechDictionary);
        // Only categorize the first MAX_MAX_MATCHED_TOKENS tokens
        while (ts.incrementToken() && weightedTokenIds.size() < MAX_MAX_MATCHED_TOKENS) {
            if (termAtt.length() > 0) {
                int weight = weightCalculator.calculateWeight(termAtt);
                weightedTokenIds.add(new TokenAndWeight(bytesRefHash.put(new BytesRef(termAtt)), weight));
            }
        }
        if (weightedTokenIds.isEmpty()) {
            return null;
        }
        return computeCategory(weightedTokenIds, unfilteredStringLen, numDocs);
    }

    public synchronized TokenListCategory computeCategory(List<TokenAndWeight> weightedTokenIds, int unfilteredStringLen, long numDocs) {

        // First set up the data structures based on the weighted tokenized string.
        List<TokenAndWeight> workTokenUniqueIds = weightedTokenIds.stream()
            .collect(Collectors.groupingBy(TokenAndWeight::getTokenId, TreeMap::new, Collectors.summingInt(TokenAndWeight::getWeight)))
            .entrySet()
            .stream()
            .map(entry -> new TokenAndWeight(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList());
        int workWeight = weightedTokenIds.stream().mapToInt(TokenAndWeight::getWeight).sum();

        return computeCategory(weightedTokenIds, workTokenUniqueIds, workWeight, unfilteredStringLen, unfilteredStringLen, numDocs);
    }

    public TokenListCategory mergeWireCategory(SerializableTokenListCategory serializableCategory) {

        TokenListCategory foreignCategory = new TokenListCategory(0, serializableCategory, bytesRefHash);
        return computeCategory(
            foreignCategory.getBaseWeightedTokenIds(),
            foreignCategory.getCommonUniqueTokenIds(),
            foreignCategory.getCommonUniqueTokenWeight(),
            foreignCategory.getBaseUnfilteredLength(),
            foreignCategory.getMaxUnfilteredStringLength(),
            foreignCategory.getNumMatches()
        );
    }

    private synchronized TokenListCategory computeCategory(
        List<TokenAndWeight> weightedTokenIds,
        List<TokenAndWeight> workTokenUniqueIds,
        int workWeight,
        int unfilteredStringLen,
        int maxUnfilteredStringLen,
        long numDocs
    ) {

        // Determine the minimum and maximum token weight that could possibly match the weight we've got.
        int minWeight = minMatchingWeight(workWeight, lowerThreshold);
        int maxWeight = maxMatchingWeight(workWeight, lowerThreshold);

        // We search previous categories in descending order of the number of matches we've seen for them.
        int bestSoFarIndex = -1;
        float bestSoFarSimilarity = lowerThreshold;
        for (int index = 0; index < categoriesByNumMatches.size(); ++index) {
            TokenListCategory compCategory = categoriesByNumMatches.get(index);
            List<TokenAndWeight> baseTokenIds = compCategory.getBaseWeightedTokenIds();
            int baseWeight = compCategory.getBaseWeight();

            // Check whether the current record matches the search for the existing category. If it
            // does then we'll put it in the existing category without any further checks. The first
            // condition here ensures that we never say a string with tokens matches the reverse
            // search of a string with no tokens (which the other criteria alone might say matched).
            boolean matchesSearch = compCategory.matchesSearchForCategory(
                workWeight,
                maxUnfilteredStringLen,
                workTokenUniqueIds,
                weightedTokenIds
            );
            if (matchesSearch == false) {
                // Quickly rule out wildly different token weights prior to doing the expensive similarity calculations.
                if (baseWeight < minWeight || baseWeight > maxWeight) {
                    continue;
                }

                // Rule out categories where adding the current string would unacceptably reduce the number of unique common tokens.
                int origUniqueTokenWeight = compCategory.getOrigUniqueTokenWeight();
                int commonUniqueTokenWeight = compCategory.getCommonUniqueTokenWeight();
                int missingCommonTokenWeight = compCategory.missingCommonTokenWeight(workTokenUniqueIds);
                float proportionOfOrig = (float) (commonUniqueTokenWeight - missingCommonTokenWeight) / (float) origUniqueTokenWeight;
                if (proportionOfOrig < lowerThreshold) {
                    continue;
                }
            }

            float similarity = similarity(weightedTokenIds, workWeight, baseTokenIds, baseWeight);

            if (matchesSearch || similarity > upperThreshold) {
                if (similarity <= lowerThreshold) {
                    // Not an ideal situation, but log at trace level to avoid excessive log file spam.
                    logger.trace(
                        "Reverse search match below threshold [{}]: orig tokens {} new tokens {}",
                        similarity,
                        compCategory.getBaseWeightedTokenIds(),
                        weightedTokenIds
                    );
                }

                // This is a strong match, so accept it immediately and stop looking for better matches.
                return addCategoryMatch(maxUnfilteredStringLen, weightedTokenIds, workTokenUniqueIds, numDocs, index);
            }

            if (similarity > bestSoFarSimilarity) {
                // This is a weak match, but remember it because it's the best we've seen.
                bestSoFarIndex = index;
                bestSoFarSimilarity = similarity;

                // Recalculate the minimum and maximum token counts that might produce a better match.
                minWeight = minMatchingWeight(workWeight, similarity);
                maxWeight = maxMatchingWeight(workWeight, similarity);
            }
        }

        if (bestSoFarIndex >= 0) {
            return addCategoryMatch(maxUnfilteredStringLen, weightedTokenIds, workTokenUniqueIds, numDocs, bestSoFarIndex);
        }

        // If we get here we haven't matched, so create a new category.
        int newIndex = categoriesByNumMatches.size();
        TokenListCategory newCategory = new TokenListCategory(
            newIndex,
            unfilteredStringLen,
            weightedTokenIds,
            workTokenUniqueIds,
            maxUnfilteredStringLen,
            numDocs
        );
        categoriesByNumMatches.add(newCategory);
        return repositionCategory(newCategory, newIndex);
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + sizeOfCollection(categoriesByNumMatches);
    }

    public int getCategoryCount() {
        return categoriesByNumMatches.size();
    }

    private TokenListCategory addCategoryMatch(
        int unfilteredLength,
        List<TokenAndWeight> weightedTokenIds,
        List<TokenAndWeight> uniqueTokenIds,
        long numDocs,
        int matchIndex
    ) {
        TokenListCategory category = categoriesByNumMatches.get(matchIndex);
        category.addString(unfilteredLength, weightedTokenIds, uniqueTokenIds, numDocs);
        if (numDocs == 1) {
            // If we're just incrementing a count by 1 (likely during initial per-shard categorization),
            // then we can keep the list sorted with a single swap operation instead of a full sort
            return repositionCategory(category, matchIndex);
        }
        categoriesByNumMatches.sort(Comparator.comparing(TokenListCategory::getNumMatches).reversed());
        return category;
    }

    private TokenListCategory repositionCategory(TokenListCategory category, int currentIndex) {
        long newNumMatches = category.getNumMatches();

        // Search backwards for the point where the incremented count belongs.
        int swapIndex = currentIndex;
        while (swapIndex > 0) {
            --swapIndex;
            if (newNumMatches <= categoriesByNumMatches.get(swapIndex).getNumMatches()) {
                // Move the changed category as little as possible - if its
                // incremented count is equal to another category's count then
                // leave that other category nearer the beginning of the list.
                ++swapIndex;
                break;
            }
        }

        if (swapIndex != currentIndex) {
            Collections.swap(categoriesByNumMatches, currentIndex, swapIndex);
        }
        return category;
    }

    static int minMatchingWeight(int weight, float threshold) {
        if (weight == 0) {
            return 0;
        }

        // The result of the floating point multiplication can be slightly out, so add a small amount of tolerance.
        // This assumes threshold is not negative - other code in this file must enforce this.
        // Using floor + 1 due to threshold check being exclusive.
        // If threshold check is changed to inclusive, change formula to ceil (without the + 1).
        return (int) Math.floor((float) weight * threshold + EPSILON) + 1;
    }

    static int maxMatchingWeight(int weight, float threshold) {
        if (weight == 0) {
            return 0;
        }

        // The result of the floating point division can be slightly out, so subtract a small amount of tolerance.
        // This assumes threshold is not negative - other code in this file must enforce this.
        // Using ceil - 1 due to threshold check being exclusive.
        // If threshold check is changed to inclusive, change formula to floor (without the - 1).
        return (int) Math.ceil((float) weight / threshold - EPSILON) - 1;
    }

    /**
     * Compute similarity between two vectors
     */
    static float similarity(List<TokenAndWeight> left, int leftWeight, List<TokenAndWeight> right, int rightWeight) {
        float similarity = 1.0f;

        int maxWeight = Math.max(leftWeight, rightWeight);
        if (maxWeight > 0) {
            similarity = 1.0f - (float) StringSimilarityTester.weightedEditDistance(left, right) / (float) maxWeight;
        }

        return similarity;
    }

    public InternalCategorizationAggregation.Bucket[] toOrderedBuckets(int size) {
        return categoriesByNumMatches.stream()
            .limit(size)
            .map(
                category -> new InternalCategorizationAggregation.Bucket(
                    new SerializableTokenListCategory(category, bytesRefHash),
                    category.getBucketOrd()
                )
            )
            .toArray(InternalCategorizationAggregation.Bucket[]::new);
    }

    public InternalCategorizationAggregation.Bucket[] toOrderedBuckets(
        int size,
        long minNumMatches,
        InternalAggregation.ReduceContext reduceContext
    ) {
        return categoriesByNumMatches.stream()
            .limit(size)
            .takeWhile(category -> category.getNumMatches() >= minNumMatches)
            .map(
                category -> new InternalCategorizationAggregation.Bucket(
                    new SerializableTokenListCategory(category, bytesRefHash),
                    category.getBucketOrd(),
                    category.getSubAggs().isEmpty()
                        ? InternalAggregations.EMPTY
                        : InternalAggregations.reduce(category.getSubAggs(), reduceContext)
                )
            )
            .toArray(InternalCategorizationAggregation.Bucket[]::new);
    }

    /**
     * Equivalent to the <code>TWeightVerbs5Other2AdjacentBoost6</code> type from
     * <a href="https://github.com/elastic/ml-cpp/blob/main/include/core/CWordDictionary.h"><code>CWordDictionary</code></a>
     * in the C++ code.
     */
    static class WeightCalculator {

        private static final int MIN_DICTIONARY_LENGTH = 2;
        private static final int CONSECUTIVE_DICTIONARY_WORDS_FOR_EXTRA_WEIGHT = 3;

        private final CategorizationPartOfSpeechDictionary partOfSpeechDictionary;
        private int consecutiveHighWeights;

        WeightCalculator(CategorizationPartOfSpeechDictionary partOfSpeechDictionary) {
            this.partOfSpeechDictionary = partOfSpeechDictionary;
        }

        /**
         * The idea here is that human readable phrases are more likely to define the message category, with
         * verbs being more important for distinguishing similar messages (for example, "starting" versus
         * "stopping" with other tokens being equal). Tokens that aren't in the dictionary are more likely
         * to be entity names. Therefore, the weighting prefers dictionary words to non-dictionary words,
         * prefers verbs to nouns, and prefers long uninterrupted sequences of dictionary words over short
         * sequences.
         */
        int calculateWeight(CharSequence term) {
            if (term.length() < MIN_DICTIONARY_LENGTH) {
                consecutiveHighWeights = 0;
                return 1;
            }
            CategorizationPartOfSpeechDictionary.PartOfSpeech pos = partOfSpeechDictionary.getPartOfSpeech(term);
            if (pos == CategorizationPartOfSpeechDictionary.PartOfSpeech.NOT_IN_DICTIONARY) {
                consecutiveHighWeights = 0;
                return 1;
            }
            ++consecutiveHighWeights;
            int posWeight = (pos == CategorizationPartOfSpeechDictionary.PartOfSpeech.VERB) ? 6 : 3;
            int adjacencyBoost = (consecutiveHighWeights >= CONSECUTIVE_DICTIONARY_WORDS_FOR_EXTRA_WEIGHT) ? 6 : 0;
            return posWeight + adjacencyBoost;
        }
    }
}
