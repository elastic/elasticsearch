/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.analysis.CharArrayMap;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.CharsRef;
import org.elasticsearch.common.Strings;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizerUtils.splitOutNeverSplit;

public class BpeTokenizer extends Tokenizer {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);

    static char[] byteEncoder() {
        List<Integer> bytes = IntStream.concat(
            IntStream.range(Character.codePointAt("!", 0), Character.codePointAt("~", 0) + 1),
            IntStream.concat(
                IntStream.range(Character.codePointAt("¡", 0), Character.codePointAt("¬", 0) + 1),
                IntStream.range(Character.codePointAt("®", 0), Character.codePointAt("ÿ", 0) + 1)
            )
        ).boxed().collect(Collectors.toList());
        List<Integer> chars = new ArrayList<>(bytes);
        int n = 0;
        for (int i = 0; i < 256; i++) {
            if (bytes.contains(i)) {
                continue;
            }
            bytes.add(i);
            chars.add(256 + n);
            n++;
        }
        char[] charArray = new char[chars.size()];
        for (int j = 0; j < bytes.size(); j++) {
            charArray[bytes.get(j)] = Character.toChars(chars.get(j))[0];
        }
        return charArray;
    }

    private static final char[] BYTES_CHAR = byteEncoder();

    // NOTE: 32 is " " utf-8 encoded byte
    private static final char ENCODED_SPACE_CHAR = BYTES_CHAR[32];

    public static BpeTokenizer build(
        List<String> neverSplit,
        List<String> dictionary,
        List<String> merges,
        String unknownToken,
        boolean isPrefixSpace
    ) {
        CharArraySet neverSplitSet = new CharArraySet(neverSplit, false);
        CharTrie neverSplitTree = CharTrie.build(neverSplit);
        CharArrayMap<Integer> mergeRanks = new CharArrayMap<>(merges.size(), false);
        int mergePos = 0;
        for (String merge : merges) {
            mergeRanks.put(Strings.replace(merge, " ", ""), mergePos++);
        }
        CharArrayMap<Integer> vocabHash = new CharArrayMap<>(dictionary.size(), false);
        int vocabPos = 0;
        for (String v : dictionary) {
            vocabHash.put(v, vocabPos++);
        }
        return new BpeTokenizer(isPrefixSpace, mergeRanks, neverSplitSet, neverSplitTree, vocabHash, unknownToken);
    }

    private final StringBuilder inputStr = new StringBuilder();
    private final LinkedList<BpeToken> tokens = new LinkedList<>();
    private final List<BpeToken> tokenizedValues = new ArrayList<>();
    private final CharArrayMap<Integer> mergeRanks;
    private final CharArrayMap<Integer> vocabulary;
    private final CharSequence unknownToken;
    private final CharArraySet neverSplitSet;
    private final CharTrie neverSplit;
    private final int tokenizedUnknown;
    private final boolean prefixSpace;
    private boolean filled;

    public BpeTokenizer(
        boolean prefixSpace,
        CharArrayMap<Integer> mergeRanks,
        CharArraySet neverSplitSet,
        CharTrie neverSplit,
        CharArrayMap<Integer> vocabulary,
        CharSequence unknownToken
    ) {
        super();
        this.mergeRanks = mergeRanks;
        this.neverSplitSet = neverSplitSet;
        this.neverSplit = neverSplit;
        this.vocabulary = vocabulary;
        if (vocabulary.containsKey(unknownToken) == false) {
            throw new IllegalArgumentException(
                "provided vocabulary does not contain the unknown token of [" + unknownToken.toString() + "]"
            );
        }
        this.unknownToken = unknownToken;
        this.tokenizedUnknown = vocabulary.get(unknownToken);
        this.prefixSpace = prefixSpace;
    }

    List<BpeToken> getTokenizedValues() {
        return tokenizedValues;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        fillBuffer(input);
        tokens.clear();
        tokenizedValues.clear();
        filled = false;
    }

    @Override
    public final void end() throws IOException {
        super.end();
        // set final offset
        offsetAtt.setOffset(inputStr.length(), inputStr.length());
    }

    @Override
    public final boolean incrementToken() throws IOException {
        // This could probably be actually incremental, read until we get a whole match from our regex
        // This would require us to transform the regex into codepoint iteration/logic, which shouldn't be too difficult
        if (filled && tokens.isEmpty()) {
            return false;
        }
        if (tokens.isEmpty()) {
            fillTokens();
        }
        if (tokens.isEmpty()) {
            return false;
        }
        clearAttributes();
        BpeToken token = tokens.removeFirst();
        tokenizedValues.add(token);
        termAtt.setEmpty().append(token.charSequence());
        offsetAtt.setOffset(token.startOffset(), token.endOffset());
        if (token.subWordToken) {
            posIncAtt.setPositionIncrement(0);
        }
        return true;
    }

    private void fillTokens() {
        boolean firstFind = true;
        LinkedList<DelimitedToken> largeTokensWithNeverSplits = splitOutNeverSplit(inputStr.toString(), neverSplit, neverSplitSet);
        // This contains the sequence split on never_split tokens
        int split = 0;
        for (DelimitedToken token : largeTokensWithNeverSplits) {
            if (neverSplitSet.contains(token.charSequence())) {
                Integer tokenId = vocabulary.get(token.charSequence());
                BpeToken toAdd = tokenId == null
                    ? new BpeToken(unknownToken, false, tokenizedUnknown, token.startOffset(), token.endOffset())
                    : new BpeToken(token.charSequence().toString(), false, tokenId, token.startOffset(), token.endOffset());
                tokens.add(toAdd);
                firstFind = false;
                split++;
                continue;
            }
            final int offsetOffset = token.startOffset();
            CharSequence delimitedTokenSequence = token.charSequence();
            // If we have splits on a "never_split", it may be that our split ends in " ".
            // Example, original seq "Never <mask> split".
            // Our split sequences would be "Never ", "<mask>", " split". We need to keep the prefix space (important for bpe)
            // But should treat the trailing space "Never " as if its part of the never_split sequence, and thus trim it here.
            if (split < largeTokensWithNeverSplits.size() - 1
                && delimitedTokenSequence.charAt(delimitedTokenSequence.length() - 1) == ' ') {
                delimitedTokenSequence = new TokenizerUtils.CharSequenceRef(delimitedTokenSequence, 0, delimitedTokenSequence.length() - 1);
            }
            BpeTokenReader tokenReader = new BpeTokenReader(delimitedTokenSequence);
            Optional<TokenizerUtils.CharSequenceRef> tokenSequence;
            while ((tokenSequence = tokenReader.next()).isPresent()) {
                boolean addedSpace = false;
                final int offsetStart = tokenSequence.get().getOffset();
                final int offsetEnd = tokenSequence.get().getOffset() + tokenSequence.get().length();
                // If we could get the utf-bytes by iterating the `chars`, we would't have to do `toString` here.
                String subStr = tokenSequence.get().toString();
                if (firstFind && prefixSpace && subStr.startsWith(" ") == false) {
                    subStr = " " + subStr;
                    addedSpace = true;
                }
                firstFind = false;
                byte[] bytes = subStr.getBytes(StandardCharsets.UTF_8);
                char[] cs = new char[bytes.length];
                for (int i = 0; i < bytes.length; i++) {
                    int b = bytes[i];
                    // In java `byte` is signed, the map assumes unsigned as it is built with `int`
                    if (b < 0) {
                        b += 256;
                    }
                    cs[i] = BYTES_CHAR[b];
                }
                List<CharSequence> bpeTokens = new ArrayList<>(cs.length);
                for (int i = 0; i < cs.length; i++) {
                    bpeTokens.add(new CharsRef(cs, i, 1));
                }
                while (bpeTokens.size() > 1) {
                    int minRank = Integer.MAX_VALUE;
                    CharSequencePair minSeq = null;
                    List<CharSequencePair> pairs = pairs(bpeTokens);
                    for (CharSequencePair sequence : pairs) {
                        int rank = mergeRanks.getOrDefault(sequence, Integer.MAX_VALUE);
                        if (rank < minRank) {
                            minSeq = sequence;
                            minRank = rank;
                        }
                    }
                    if (minSeq == null) {
                        break;
                    }
                    List<CharSequence> mergedBpeTokens = new ArrayList<>(bpeTokens.size() - 1);
                    for (int i = 0; i < minSeq.firstPos; i++) {
                        mergedBpeTokens.add(bpeTokens.get(i));
                    }
                    mergedBpeTokens.add(minSeq);
                    for (int i = minSeq.secondPos + 1; i < bpeTokens.size(); i++) {
                        mergedBpeTokens.add(bpeTokens.get(i));
                    }
                    bpeTokens = mergedBpeTokens;
                }
                boolean subWordToken = false;
                for (CharSequence charSequence : bpeTokens) {
                    // call toString here to do a copy of the char[] array.
                    // It is dangerous to have each token continue to share the same underlying array
                    Integer tokenId = vocabulary.get(charSequence);
                    // If this is the start of a new set of sub-word tokens AND it starts with a space, adjust the offsets to not include
                    // the space. But, don't consider our potentially added space as it is not part of the original string
                    int startOffsetAdj = subWordToken == false
                        && charSequence.charAt(0) == ENCODED_SPACE_CHAR
                        && addedSpace == false
                        && charSequence.length() > 1 ? 1 : 0;
                    BpeToken toAdd = tokenId == null
                        ? new BpeToken(
                            unknownToken,
                            subWordToken,
                            tokenizedUnknown,
                            offsetStart + offsetOffset + startOffsetAdj,
                            offsetEnd + offsetOffset
                        )
                        : new BpeToken(
                            charSequence.toString(),
                            subWordToken,
                            tokenId,
                            offsetStart + offsetOffset + startOffsetAdj,
                            offsetEnd + offsetOffset
                        );
                    tokens.add(toAdd);
                    subWordToken = true;
                }
            }
        }
        filled = true;
    }

    private static List<CharSequencePair> pairs(List<CharSequence> tokens) {
        List<CharSequencePair> pairs = new ArrayList<>(tokens.size() - 1);
        for (int i = 0; i < tokens.size() - 1; i++) {
            pairs.add(new CharSequencePair(MultiCharSequence.from(tokens.get(i), tokens.get(i + 1)), i, i + 1));
        }
        return pairs;
    }

    private void fillBuffer(Reader input) throws IOException {
        int len;
        // This is pretty much stolen from PatternTokenizer. We really should stream the input and stop using regex
        final char[] buffer = new char[1024];
        inputStr.setLength(0);
        while ((len = input.read(buffer)) > 0) {
            inputStr.append(buffer, 0, len);
        }
    }

    public static class BpeToken extends DelimitedToken.Encoded {
        private final boolean subWordToken;

        public BpeToken(CharSequence charSequence, boolean subWordToken, int tokenId, int startOffset, int endOffset) {
            super(charSequence, tokenId, startOffset, endOffset);
            this.subWordToken = subWordToken;
        }
    }

    private record CharSequencePair(CharSequence pair, int firstPos, int secondPos) implements CharSequence {

        @Override
        public int length() {
            return pair.length();
        }

        @Override
        public char charAt(int index) {
            return pair.charAt(index);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return pair.subSequence(start, end);
        }

        @Override
        public String toString() {
            return pair.toString();
        }
    }

}
