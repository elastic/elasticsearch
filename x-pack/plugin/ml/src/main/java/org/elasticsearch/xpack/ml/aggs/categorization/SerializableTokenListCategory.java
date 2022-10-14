/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * {@link TokenListCategory} cannot be serialized between nodes as its token IDs
 * are local to a particular {@link CategorizationBytesRefHash}, which is node-specific.
 * This class stores the same data as {@link TokenListCategory}, but in a form that's
 * less efficient to store, compare and manipulate. Instances of this class should be
 * created, serialized to the wire, then discarded as quickly as possible.
 */
public class SerializableTokenListCategory implements Writeable {

    /**
     * This controls the length of the key we generate. We don't want an enormous terms list if categorizing a very long message.
     * Matches the value used in <a href="https://github.com/elastic/ml-cpp/blob/main/lib/model/CTokenListReverseSearchCreator.cc">
     * <code>CTokenListReverseSearchCreator</code></a> in the C++ code.
     */
    public static final int KEY_BUDGET = 10000;

    private static final String REGEX_NEEDS_ESCAPE_PATTERN = "([\\\\|()\\[\\]{}^$.+*?])";

    final BytesRef[] baseTokens;
    final int[] baseTokenWeights;
    final int baseUnfilteredLength;
    final int maxUnfilteredStringLength;
    final int orderedCommonTokenBeginIndex;
    final int orderedCommonTokenEndIndex;
    final int[] commonUniqueTokenIndexes;
    final int[] commonUniqueTokenWeights;
    final int[] keyTokenIndexes;
    final int origUniqueTokenWeight;
    final long numMatches;

    /**
     * @param category     The category to be serialized.
     * @param bytesRefHash This <em>must</em> be the same {@link CategorizationBytesRefHash} that was
     *                     used to create the token IDs originally passed to the constructor of <code>category</code>.
     */
    public SerializableTokenListCategory(TokenListCategory category, CategorizationBytesRefHash bytesRefHash) {
        List<TokenListCategory.TokenAndWeight> baseWeightedTokenIds = category.getBaseWeightedTokenIds();
        this.baseTokens = baseWeightedTokenIds.stream().map(tw -> bytesRefHash.getDeep(tw.getTokenId())).toArray(BytesRef[]::new);
        this.baseTokenWeights = category.getBaseWeightedTokenIds().stream().mapToInt(TokenListCategory.TokenAndWeight::getWeight).toArray();
        this.baseUnfilteredLength = category.getBaseUnfilteredLength();
        this.maxUnfilteredStringLength = category.getMaxUnfilteredStringLength();
        this.orderedCommonTokenBeginIndex = category.getOrderedCommonTokenBeginIndex();
        this.orderedCommonTokenEndIndex = category.getOrderedCommonTokenEndIndex();
        // For the common unique tokens and key tokens, rather than serialize
        // the full strings twice we serialize indexes into the base tokens
        Map<Integer, Integer> tokenIdToIndex = new HashMap<>();
        for (int index = 0; index < baseWeightedTokenIds.size(); ++index) {
            tokenIdToIndex.putIfAbsent(baseWeightedTokenIds.get(index).getTokenId(), index);
        }
        List<TokenListCategory.TokenAndWeight> commonUniqueTokenIds = category.getCommonUniqueTokenIds();
        this.commonUniqueTokenIndexes = commonUniqueTokenIds.stream().mapToInt(tw -> tokenIdToIndex.get(tw.getTokenId())).toArray();
        this.commonUniqueTokenWeights = commonUniqueTokenIds.stream().mapToInt(TokenListCategory.TokenAndWeight::getWeight).toArray();
        // Get the terms that will be displayed in the results.
        // When categorization was first written there were three aspects to the category definition:
        // 1. Common unique terms
        // 2. Common terms that appear in the same order for every string in the category
        // 3. Max matching length
        //
        // The aim was to build a fast and accurate reverse search to find the original strings, so
        // the idea was that you'd search for strings that contained all the unique terms (fast), then
        // confirm those by using a regex that checked the order of the terms expected to be in order,
        // and finally discard ridiculously long matches when the category contained few terms.
        //
        // This history can be seen in the member variables of the TokenListCategory class and the way
        // it modifies them when new strings are added.
        //
        // However, over the years what actually got presented to users got changed. Instead of
        // presenting the results of the reverse search a set of terms was presented that consisted of
        // all base terms that are present in the common unique tokens, in the order they occur in the
        // base tokens (potentially including duplicates). In the C++ code the length is capped at
        // 10000 UTF-8 bytes (including separating spaces) and where the initially chosen tokens would
        // exceed that length the rarest tokens (across all categories) are picked until the budget
        // is reached. We cannot easily do this in the Java code, as the {@link CategorizationBytesRefHash}
        // class does not store usage counts for each token. So instead we simply chop the sequence of
        // tokens at an appropriate limit when mapping back to strings.
        List<Integer> keyTokenIndexes = new ArrayList<>();
        int budgetRemaining = KEY_BUDGET + 1;
        for (TokenListCategory.TokenAndWeight tokenAndWeight : category.getBaseWeightedTokenIds()) {
            if (category.isTokenIdCommon(tokenAndWeight) == false) {
                continue;
            }
            int index = tokenIdToIndex.get(tokenAndWeight.getTokenId());
            // For the space separator - not needed for the first token, but we took that into account by adding 1 to the budget
            --budgetRemaining;
            if (baseTokens[index].length > budgetRemaining) {
                break;
            }
            budgetRemaining -= baseTokens[index].length;
            keyTokenIndexes.add(index);
        }
        this.keyTokenIndexes = keyTokenIndexes.stream().mapToInt(Integer::intValue).toArray();
        this.origUniqueTokenWeight = category.getOrigUniqueTokenWeight();
        this.numMatches = category.getNumMatches();
    }

    public SerializableTokenListCategory(StreamInput in) throws IOException {
        this.baseTokens = in.readArray(StreamInput::readBytesRef, BytesRef[]::new);
        this.baseTokenWeights = in.readVIntArray();
        this.baseUnfilteredLength = in.readVInt();
        this.maxUnfilteredStringLength = in.readVInt();
        this.orderedCommonTokenBeginIndex = in.readVInt();
        this.orderedCommonTokenEndIndex = in.readVInt();
        this.commonUniqueTokenIndexes = in.readVIntArray();
        this.commonUniqueTokenWeights = in.readVIntArray();
        this.keyTokenIndexes = in.readVIntArray();
        this.origUniqueTokenWeight = in.readVInt();
        this.numMatches = in.readVLong();
    }

    /**
     * Used to copy an existing category but scaling the number of matches.
     * (For use with sampling.)
     * @param other Category to largely copy.
     * @param scaledMatches Override for the number of matches.
     */
    public SerializableTokenListCategory(SerializableTokenListCategory other, long scaledMatches) {
        this.baseTokens = other.baseTokens;
        this.baseTokenWeights = other.baseTokenWeights;
        this.baseUnfilteredLength = other.baseUnfilteredLength;
        this.maxUnfilteredStringLength = other.maxUnfilteredStringLength;
        this.orderedCommonTokenBeginIndex = other.orderedCommonTokenBeginIndex;
        this.orderedCommonTokenEndIndex = other.orderedCommonTokenEndIndex;
        this.commonUniqueTokenIndexes = other.commonUniqueTokenIndexes;
        this.commonUniqueTokenWeights = other.commonUniqueTokenWeights;
        this.keyTokenIndexes = other.keyTokenIndexes;
        this.origUniqueTokenWeight = other.origUniqueTokenWeight;
        this.numMatches = scaledMatches;
    }

    public long getNumMatches() {
        return numMatches;
    }

    public int maxMatchingStringLen() {
        return TokenListCategory.maxMatchingStringLen(baseUnfilteredLength, maxUnfilteredStringLength, commonUniqueTokenIndexes.length);
    }

    public BytesRef[] getKeyTokens() {
        return Arrays.stream(keyTokenIndexes).mapToObj(index -> baseTokens[index]).toArray(BytesRef[]::new);
    }

    public String getRegex() {
        if (keyTokenIndexes.length == 0 || orderedCommonTokenBeginIndex == orderedCommonTokenEndIndex) {
            return ".*";
        }
        return Arrays.stream(keyTokenIndexes)
            .filter(index -> index >= orderedCommonTokenBeginIndex && index < orderedCommonTokenEndIndex)
            .mapToObj(index -> baseTokens[index].utf8ToString().replaceAll(REGEX_NEEDS_ESCAPE_PATTERN, "\\\\$1"))
            .collect(Collectors.joining(".+?", ".*?", ".*?"));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(StreamOutput::writeBytesRef, baseTokens);
        out.writeVIntArray(baseTokenWeights);
        out.writeVInt(baseUnfilteredLength);
        out.writeVInt(maxUnfilteredStringLength);
        out.writeVInt(orderedCommonTokenBeginIndex);
        out.writeVInt(orderedCommonTokenEndIndex);
        out.writeVIntArray(commonUniqueTokenIndexes);
        out.writeVIntArray(commonUniqueTokenWeights);
        out.writeVIntArray(keyTokenIndexes);
        out.writeVInt(origUniqueTokenWeight);
        out.writeVLong(numMatches);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            Arrays.hashCode(baseTokens),
            Arrays.hashCode(baseTokenWeights),
            baseUnfilteredLength,
            maxUnfilteredStringLength,
            orderedCommonTokenBeginIndex,
            orderedCommonTokenEndIndex,
            Arrays.hashCode(commonUniqueTokenIndexes),
            Arrays.hashCode(commonUniqueTokenWeights),
            Arrays.hashCode(keyTokenIndexes),
            origUniqueTokenWeight,
            numMatches
        );
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        SerializableTokenListCategory that = (SerializableTokenListCategory) other;
        return Arrays.deepEquals(this.baseTokens, that.baseTokens)
            && Arrays.equals(this.baseTokenWeights, that.baseTokenWeights)
            && this.baseUnfilteredLength == that.baseUnfilteredLength
            && this.maxUnfilteredStringLength == that.maxUnfilteredStringLength
            && this.orderedCommonTokenBeginIndex == that.orderedCommonTokenBeginIndex
            && this.orderedCommonTokenEndIndex == that.orderedCommonTokenEndIndex
            && Arrays.equals(this.commonUniqueTokenIndexes, that.commonUniqueTokenIndexes)
            && Arrays.equals(this.commonUniqueTokenWeights, that.commonUniqueTokenWeights)
            && Arrays.equals(this.keyTokenIndexes, that.keyTokenIndexes)
            && this.origUniqueTokenWeight == that.origUniqueTokenWeight
            && this.numMatches == that.numMatches;
    }

    @Override
    public String toString() {
        return Arrays.stream(keyTokenIndexes)
            .mapToObj(index -> baseTokens[index].utf8ToString())
            .collect(Collectors.joining(", ", "Category with key tokens [", "]")) + Arrays.stream(baseTokens)
                .map(BytesRef::utf8ToString)
                .collect(Collectors.joining(", ", " and base tokens [", "]"));
    }
}
