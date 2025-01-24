/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.CharacterUtils;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizerUtils.splitOutNeverSplit;

/**
 * Sentence-piece unigram tokenizer.
 *
 * Does whitespace tokenization with unigram tokenization on the resulting tokens.
 *
 * This cannot be a token-filter as it needs access to the offset correction logic provided by the upstream CharFilter.
 *
 * You may notice that the offsets are always matching the individual tokens position back to the original string. This is because
 * there aren't "sub-word" tokens, per-se. So, we don't have tokens that share the same offsets as in WordPiece.
 */
public final class UnigramTokenizer extends Tokenizer {
    private static final double K_UNK_PENALTY = 10.0;
    static final String PREFIX = "▁";

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

    static UnigramTokenizer build(
        List<String> neverSplit,
        List<String> dictionary,
        double[] scores,
        String unknownToken,
        boolean byteFallback
    ) {
        if (dictionary.isEmpty()) {
            throw new IllegalArgumentException("vocab empty");
        }
        if (unknownToken == null) {
            throw new IllegalArgumentException("unknown token ID");
        }
        CharArraySet neverSplitSet = new CharArraySet(neverSplit, false);
        CharTrie neverSplitTree = CharTrie.build(neverSplit);
        if (dictionary.size() != scores.length) {
            throw new IllegalArgumentException(
                format("provided vocabulary [%s] and scores [%s] must have the same size", dictionary.size(), scores.length)
            );
        }
        int vocabSize = dictionary.size();
        BytesTrie vocabTrie = new BytesTrie();
        Map<BytesRef, Integer> tokenToId = Maps.newHashMapWithExpectedSize(vocabSize);
        int vocabIndex = 0;
        double minScore = Double.POSITIVE_INFINITY;
        for (String word : dictionary) {
            minScore = Double.min(minScore, scores[vocabIndex]);
            BytesRef vocab = new BytesRef(word);
            tokenToId.put(vocab, vocabIndex++);
            vocabTrie.insert(vocab);
        }
        return new UnigramTokenizer(
            minScore,
            scores,
            neverSplitTree,
            neverSplitSet,
            tokenToId,
            vocabTrie,
            Optional.ofNullable(tokenToId.get(new BytesRef(unknownToken)))
                .orElseThrow(
                    () -> new IllegalArgumentException("provided vocabulary does not contain the unknown token of [" + unknownToken + "]")
                ),
            byteFallback
        );
    }

    private final LinkedList<DelimitedToken.Encoded> tokens;
    private final List<DelimitedToken.Encoded> tokenizedValues;
    private final SimpleWhitespaceTokenizer whitespaceTokenizer;

    private final double minScore;
    // This may be configurable in the future
    private boolean fuseUnk = true;
    private final double[] vocabScores;
    private final CharTrie neverSplit;
    private final CharArraySet neverSplitHash;
    private final Map<BytesRef, Integer> vocabToId;
    private final BytesTrie vocabTrie;
    private final int unknownTokenId;
    // This is a buffer that is reused per token for decoding the normalized char-sequence into utf-8 bytes
    // It's usage is NOT thread safe
    private byte[] normalizedByteBuffer = new byte[128];
    private boolean byteFallback = false; // If true, decompose unknown pieces into UTF-8 byte pieces

    public UnigramTokenizer(
        double minScore,
        double[] vocabScores,
        CharTrie neverSplit,
        CharArraySet neverSplitHash,
        Map<BytesRef, Integer> vocabToId,
        BytesTrie vocabTrie,
        int unknownTokenId
    ) {
        super();
        this.tokens = new LinkedList<>();
        this.tokenizedValues = new ArrayList<>();
        this.minScore = minScore;
        this.neverSplit = neverSplit;
        this.neverSplitHash = neverSplitHash;
        this.vocabToId = vocabToId;
        this.vocabTrie = vocabTrie;
        this.unknownTokenId = unknownTokenId;
        this.vocabScores = vocabScores;
        this.whitespaceTokenizer = new SimpleWhitespaceTokenizer();
    }

    public UnigramTokenizer(
        double minScore,
        double[] vocabScores,
        CharTrie neverSplit,
        CharArraySet neverSplitHash,
        Map<BytesRef, Integer> vocabToId,
        BytesTrie vocabTrie,
        int unknownTokenId,
        boolean byteFallback
    ) {
        super();
        this.tokens = new LinkedList<>();
        this.tokenizedValues = new ArrayList<>();
        this.minScore = minScore;
        this.neverSplit = neverSplit;
        this.neverSplitHash = neverSplitHash;
        this.vocabToId = vocabToId;
        this.vocabTrie = vocabTrie;
        this.unknownTokenId = unknownTokenId;
        this.vocabScores = vocabScores;
        this.whitespaceTokenizer = new SimpleWhitespaceTokenizer();
        this.byteFallback = byteFallback;
        this.fuseUnk = byteFallback == false;
    }

    List<DelimitedToken.Encoded> getTokenizedValues() {
        return tokenizedValues;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        tokens.clear();
        tokenizedValues.clear();
        whitespaceTokenizer.reset();
    }

    @Override
    public void end() throws IOException {
        super.end();
        offsetAtt.setOffset(correctOffset(whitespaceTokenizer.finalOffset), correctOffset(whitespaceTokenizer.finalOffset));
    }

    private void popFromTokens() {
        if (tokens.isEmpty() == false) {
            DelimitedToken.Encoded token = tokens.removeFirst();
            tokenizedValues.add(token);
            termAtt.setEmpty().append(token.charSequence());
            offsetAtt.setOffset(token.startOffset(), token.endOffset());
        }
    }

    @Override
    public boolean incrementToken() throws IOException {
        clearAttributes();
        if (tokens.isEmpty() == false) {
            popFromTokens();
            return true;
        }
        // First, whitespace tokenize
        DelimitedToken whitespaceToken = whitespaceTokenizer.next();
        if (whitespaceToken != null) {
            if (neverSplitHash.contains(whitespaceToken.charSequence())) {
                Integer maybeTokenized = vocabToId.get(new BytesRef(whitespaceToken.charSequence()));
                tokens.add(
                    new DelimitedToken.Encoded(
                        whitespaceToken.charSequence().toString(),
                        Objects.requireNonNullElse(maybeTokenized, unknownTokenId),
                        correctOffset(whitespaceToken.startOffset()),
                        correctOffset(whitespaceToken.endOffset())
                    )
                );
                popFromTokens();
                return true;
            }
            int inputOffsetStart = whitespaceToken.startOffset();
            // Split out our neverSplit tokens
            LinkedList<DelimitedToken> largeTokensWithNeverSplits = splitOutNeverSplit(
                whitespaceToken.charSequence(),
                neverSplit,
                neverSplitHash
            );
            // Encode each token, skipping our "never split" ones.
            for (DelimitedToken token : largeTokensWithNeverSplits) {
                if (neverSplitHash.contains(token.charSequence())) {
                    Integer tokenId = vocabToId.get(new BytesRef(token.charSequence()));
                    DelimitedToken.Encoded toAdd = tokenId == null
                        ? new DelimitedToken.Encoded(
                            token.charSequence().toString(),
                            unknownTokenId,
                            correctOffset(token.startOffset() + inputOffsetStart),
                            correctOffset(token.endOffset() + inputOffsetStart)
                        )
                        : new DelimitedToken.Encoded(
                            token.charSequence().toString(),
                            tokenId,
                            correctOffset(token.startOffset() + inputOffsetStart),
                            correctOffset(token.endOffset() + inputOffsetStart)
                        );
                    tokens.add(toAdd);
                    continue;
                }
                // We always prefix the initial sub-tokens
                // e.g. "<mask> asdf<mask>-asdf " -> ['<mask>', '▁as', 'd', 'f', '<mask>', '▁-', 'as', 'd', 'f']
                IntToIntFunction offsetCorrectorFunction = i -> {
                    int adj = i + inputOffsetStart + token.startOffset();
                    // if the passed offset to set is `0`, that means the tokenization probably matched on the meta-space character
                    // Meaning, the start and end offsets for that token will be the same and ultimately discarded when re-constituting
                    // tokenized results (if that is necessary for the task).
                    if (i > 0) {
                        // We always apply the prefix, so account for that when correcting the offsets, basically, the original
                        // normalization
                        // doesn't know about our prefix, so we should find out the correct offsets when not taking it into account.
                        adj -= PREFIX.length();
                    }
                    return correctOffset(adj);
                };
                List<DelimitedToken.Encoded> tokenList = tokenize(
                    MultiCharSequence.from(PREFIX, token.charSequence()),
                    offsetCorrectorFunction
                );
                tokens.addAll(tokenList);
            }
            popFromTokens();
            return true;
        }
        return false;
    }

    private int[] decomposeBytePieces(byte[] bytes) {
        assert this.byteFallback;

        int[] pieces = new int[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            BytesRef decomposedToken = new BytesRef(Strings.format("<0x%02X>", bytes[i]));
            Integer piece = vocabToId.get(decomposedToken);
            if (piece == null) {
                piece = unknownTokenId;
            }
            pieces[i] = piece;
        }
        return pieces;
    }

    /**
     * This algorithm does the following:
     *
     *  - iterates all the prefixes for the given input sequence, byte by byte.
     *  - Keeps track of the best scores for the prefixes we find and reconstitutes the tokens from those prefixes
     *
     *  This is derived from:
     *  https://github.com/google/sentencepiece/blob/901368e0752b57a408ac5c84bca0a219d62c648f/src/unigram_model.cc#L890
     *  https://github.com/huggingface/tokenizers/blob/1f1f86dd320fa653924eb1560e51d1b287ab0613/tokenizers/src/models/unigram/model.rs#L229
     *
     * @param inputSequence The sequence to encode, should have NO whitespace characters
     * @param offsetCorrection Offset corrections to apply to the tokens. Should take into account any previous char-filtering and tokens.
     * @return The list of delimited and encoded tokens
     */
    List<DelimitedToken.Encoded> tokenize(CharSequence inputSequence, IntToIntFunction offsetCorrection) {
        int bytelen = UnicodeUtil.calcUTF16toUTF8Length(inputSequence, 0, inputSequence.length());
        if (bytelen > normalizedByteBuffer.length) {
            normalizedByteBuffer = new byte[bytelen + 1];
        }
        int numBytes = UnicodeUtil.UTF16toUTF8(inputSequence, 0, inputSequence.length(), normalizedByteBuffer);
        double unkScore = minScore - K_UNK_PENALTY;
        BestPathNode[] bestPathNodes = new BestPathNode[numBytes + 1];
        int bytePos = 0;
        int charPos = 0;
        while (charPos < inputSequence.length()) {
            double bestScoreTillHere = bestPathNodes[bytePos] == null ? 0 : bestPathNodes[bytePos].score;

            boolean isSurrogatePair = (charPos + 1 < inputSequence.length()
                && Character.isSurrogatePair(inputSequence.charAt(charPos), inputSequence.charAt(charPos + 1)));
            int numUtf16Chars = isSurrogatePair ? 2 : 1;
            int mblen = UnicodeUtil.calcUTF16toUTF8Length(inputSequence, charPos, numUtf16Chars);

            boolean hasSingleNode = false;
            // Find the matching prefixes, incrementing by the chars, each time
            for (BytesRef prefix : vocabTrie.matchingPrefixes(new BytesRef(normalizedByteBuffer, bytePos, numBytes - bytePos))) {
                int pathKey = bytePos + prefix.length;
                int tokenId = vocabToId.get(prefix);
                double score = vocabScores[tokenId];
                BestPathNode node = bestPathNodes[pathKey];
                double candidateScore = score + bestScoreTillHere;
                if (node == null || candidateScore > node.score) {
                    if (node == null) {
                        node = new BestPathNode();
                        bestPathNodes[pathKey] = node;
                    }
                    node.id = tokenId;
                    node.score = candidateScore;
                    node.startsAtBytePos = bytePos;
                    node.startsAtCharPos = charPos;
                }
                hasSingleNode = hasSingleNode || (pathKey - bytePos) == mblen;
            }
            if (hasSingleNode == false) {
                BestPathNode node = bestPathNodes[bytePos + mblen];
                double candidateScore = unkScore + bestScoreTillHere;
                if (node == null || candidateScore > node.score) {
                    if (node == null) {
                        node = new BestPathNode();
                        bestPathNodes[bytePos + mblen] = node;
                    }
                    node.id = unknownTokenId;
                    node.score = candidateScore;
                    node.startsAtBytePos = bytePos;
                    node.startsAtCharPos = charPos;
                }
            }
            // Move our prefix search to the next char
            bytePos += mblen;
            charPos = charPos + numUtf16Chars;
        }
        int endsAtBytes = numBytes;
        int endsAtChars = inputSequence.length();
        List<DelimitedToken.Encoded> unknownTokens = new ArrayList<>();
        List<DelimitedToken.Encoded> results = new ArrayList<>();
        // Now we work our way backwards finding the best path nodes, using the `startAtBytePos` as backward links.
        while (endsAtBytes > 0) {
            BestPathNode node = bestPathNodes[endsAtBytes];
            int startsAtBytes = node.startsAtBytePos;
            if (node.id == unknownTokenId && byteFallback) {
                CharSequence multiByteSequence = inputSequence.subSequence(node.startsAtCharPos, endsAtChars);
                byte[] bytes = multiByteSequence.toString().getBytes(StandardCharsets.UTF_8);
                int[] pieces = decomposeBytePieces(bytes);
                for (int i = pieces.length - 1; i >= 0; i--) {
                    results.add(
                        new DelimitedToken.Encoded(
                            Strings.format("<0x%02X>", bytes[i]),
                            pieces[i],
                            // even though we are changing the number of characters in the output, we don't
                            // need to change the offsets. The offsets refer to the input characters
                            offsetCorrection.apply(node.startsAtCharPos),
                            offsetCorrection.apply(endsAtChars)
                        )
                    );
                }
            } else if (node.id == unknownTokenId && fuseUnk) {
                unknownTokens.add(
                    new DelimitedToken.Encoded(
                        new String(normalizedByteBuffer, startsAtBytes, endsAtBytes - startsAtBytes, StandardCharsets.UTF_8),
                        unknownTokenId,
                        offsetCorrection.apply(node.startsAtCharPos),
                        offsetCorrection.apply(endsAtChars)
                    )
                );
            } else {
                if (unknownTokens.isEmpty() == false) {
                    Collections.reverse(unknownTokens);
                    results.add(DelimitedToken.Encoded.mergeEncodedTokens(unknownTokens));
                    unknownTokens.clear();
                }
                results.add(
                    new DelimitedToken.Encoded(
                        new String(normalizedByteBuffer, startsAtBytes, endsAtBytes - startsAtBytes, StandardCharsets.UTF_8),
                        node.id,
                        offsetCorrection.apply(node.startsAtCharPos),
                        offsetCorrection.apply(endsAtChars)
                    )
                );
            }
            endsAtBytes = startsAtBytes;
            endsAtChars = node.startsAtCharPos;
        }
        if (unknownTokens.isEmpty() == false) {
            Collections.reverse(unknownTokens);
            results.add(DelimitedToken.Encoded.mergeEncodedTokens(unknownTokens));
            unknownTokens.clear();
        }
        Collections.reverse(results);
        return results;
    }

    private static byte fromBytesRef(BytesRef bytesRef, int index) {
        return bytesRef.bytes[index + bytesRef.offset];
    }

    /**
     * This is a bytes-trie, this is used for gathering known matching prefixes given the original vocabulary.
     *
     * NOTE: it is possible for a node to be a "leaf" and have children. It being a "leaf", just means that it is the end of a possible
     * vocab entry that matches a given prefix.
     */
    static class BytesTrie {
        private final Map<Byte, BytesTrie> children;
        private boolean isLeaf;

        BytesTrie() {
            children = new HashMap<>();
        }

        private void setLeaf(boolean isLeaf) {
            this.isLeaf = isLeaf;
        }

        private boolean isLeaf() {
            return isLeaf;
        }

        List<BytesRef> matchingPrefixes(BytesRef input) {
            List<BytesRef> prefixes = new ArrayList<>();
            int numMatchedChildren = 0;
            BytesTrie node = this;
            for (int i = input.offset; i < input.length + input.offset; i++) {
                if (node == null) {
                    break;
                }
                if (node.isLeaf() && numMatchedChildren > 0) {
                    prefixes.add(new BytesRef(input.bytes, input.offset, numMatchedChildren));
                }
                node = node.children.get(input.bytes[i]);
                numMatchedChildren++;
            }
            if (node != null && node.isLeaf() && numMatchedChildren > 0) {
                prefixes.add(new BytesRef(input.bytes, input.offset, numMatchedChildren));
            }
            return prefixes;
        }

        void insert(BytesRef bytes) {
            if (bytes.length == 0) {
                return;
            }
            BytesTrie currentNode = this;
            int currentTokenIndex = 0;

            // find last child
            while (currentTokenIndex < bytes.length) {
                currentNode = currentNode.children.computeIfAbsent(fromBytesRef(bytes, currentTokenIndex), k -> new BytesTrie());
                currentTokenIndex++;
            }
            currentNode.setLeaf(true);
        }

        public static BytesTrie build(Collection<BytesRef> tokens) {
            BytesTrie root = new BytesTrie();
            for (BytesRef token : tokens) {
                root.insert(token);
            }
            return root;
        }
    }

    /**
     * This keeps track of the best-path in the vocab for given prefixes
     */
    private static class BestPathNode {
        // Token Id, -1 if its unknown
        private int id = -1;
        // Token score
        double score = 0.0;
        // starts at byte position for walking back the best scoring node
        private int startsAtBytePos = -1;
        // Its char position for correctly identifying offsets related to the original input
        private int startsAtCharPos = -1;
    }

    @FunctionalInterface
    public interface IntToIntFunction {
        int apply(int value);
    }

    /**
     * This is a simple whitespace tokenizer that generates whitespace delimited tokens from the input stream
     *
     * This is effectively the lucene WhitespaceTokenizer, slightly adjusted for our needs here.
     */
    class SimpleWhitespaceTokenizer {
        private int offset = 0, bufferIndex = 0, dataLen = 0, finalOffset = 0;
        private static final int IO_BUFFER_SIZE = 4096;
        private final CharacterUtils.CharacterBuffer ioBuffer = CharacterUtils.newCharacterBuffer(IO_BUFFER_SIZE);

        void reset() {
            bufferIndex = 0;
            offset = 0;
            dataLen = 0;
            finalOffset = 0;
            ioBuffer.reset();
        }

        @Nullable
        DelimitedToken next() throws IOException {
            int length = 0;
            int start = -1; // this variable is always initialized
            int end = -1;
            char[] buffer = termAtt.buffer();
            while (true) {
                if (bufferIndex >= dataLen) {
                    offset += dataLen;
                    CharacterUtils.fill(ioBuffer, input); // read supplementary char aware with CharacterUtils
                    if (ioBuffer.getLength() == 0) {
                        dataLen = 0; // so next offset += dataLen won't decrement offset
                        if (length > 0) {
                            break;
                        } else {
                            finalOffset = offset;
                            return null;
                        }
                    }
                    dataLen = ioBuffer.getLength();
                    bufferIndex = 0;
                }
                // use CharacterUtils here to support < 3.1 UTF-16 code unit behavior if the char based
                // methods are gone
                final int c = Character.codePointAt(ioBuffer.getBuffer(), bufferIndex, ioBuffer.getLength());
                final int charCount = Character.charCount(c);
                bufferIndex += charCount;
                if (Character.isWhitespace(c) == false) { // if it's a token char
                    if (length == 0) { // start of token
                        assert start == -1;
                        start = offset + bufferIndex - charCount;
                        end = start;
                    } else if (length >= buffer.length - 1) { // supplementary could run out of bounds?
                        // make sure a supplementary fits in the buffer
                        buffer = termAtt.resizeBuffer(2 + length);
                    }
                    end += charCount;
                    length += Character.toChars(c, buffer, length);
                } else if (length > 0) {
                    break;
                }
            }

            termAtt.setLength(length);
            assert start != -1;
            return new DelimitedToken(termAtt, start, finalOffset = end);
        }
    }
}
