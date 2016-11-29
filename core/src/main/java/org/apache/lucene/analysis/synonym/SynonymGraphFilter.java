/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.analysis.synonym;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.RollingBuffer;
import org.apache.lucene.util.fst.FST;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

// TODO: maybe we should resolve token -> wordID then run
// FST on wordIDs, for better perf?

// TODO: a more efficient approach would be Aho/Corasick's
// algorithm
// http://en.wikipedia.org/wiki/Aho%E2%80%93Corasick_string_matching_algorithm
// It improves over the current approach here
// because it does not fully re-start matching at every
// token.  For example if one pattern is "a b c x"
// and another is "b c d" and the input is "a b c d", on
// trying to parse "a b c x" but failing when you got to x,
// rather than starting over again your really should
// immediately recognize that "b c d" matches at the next
// input.  I suspect this won't matter that much in
// practice, but it's possible on some set of synonyms it
// will.  We'd have to modify Aho/Corasick to enforce our
// conflict resolving (eg greedy matching) because that algo
// finds all matches.  This really amounts to adding a .*
// closure to the FST and then determinizing it.
//
// Another possible solution is described at http://www.cis.uni-muenchen.de/people/Schulz/Pub/dictle5.ps

/**
 * Applies single- or multi-token synonyms from a {@link SynonymMap}
 * to an incoming {@link TokenStream}, producing a fully correct graph
 * output.  This is a replacement for {@link SynonymFilter}, which produces
 * incorrect graphs for multi-token synonyms.
 *
 * <b>NOTE</b>: this cannot consume an incoming graph; results will
 * be undefined.
 */
public final class SynonymGraphFilter extends TokenFilter {

    public static final String TYPE_SYNONYM = "SYNONYM";
    public static final int GRAPH_FLAG = 8;

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    private final PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);
    private final FlagsAttribute flagsAtt = addAttribute(FlagsAttribute.class);

    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

    private final SynonymMap synonyms;
    private final boolean ignoreCase;

    private final FST<BytesRef> fst;

    private final FST.BytesReader fstReader;
    private final FST.Arc<BytesRef> scratchArc;
    private final ByteArrayDataInput bytesReader = new ByteArrayDataInput();
    private final BytesRef scratchBytes = new BytesRef();
    private final CharsRefBuilder scratchChars = new CharsRefBuilder();
    private final LinkedList<BufferedOutputToken> outputBuffer = new LinkedList<>();

    private int nextNodeOut;
    private int lastNodeOut;
    private int maxLookaheadUsed;

    // For testing:
    private int captureCount;

    private boolean liveToken;

    // Start/end offset of the current match:
    private int matchStartOffset;
    private int matchEndOffset;

    // True once the input TokenStream is exhausted:
    private boolean finished;

    private int lookaheadNextRead;
    private int lookaheadNextWrite;

    private RollingBuffer<BufferedInputToken> lookahead = new RollingBuffer<BufferedInputToken>() {
        @Override
        protected BufferedInputToken newInstance() {
            return new BufferedInputToken();
        }
    };

    static class BufferedInputToken implements RollingBuffer.Resettable {
        final CharsRefBuilder term = new CharsRefBuilder();
        AttributeSource.State state;
        int startOffset = -1;
        int endOffset = -1;

        @Override
        public void reset() {
            state = null;
            term.clear();

            // Intentionally invalid to ferret out bugs:
            startOffset = -1;
            endOffset = -1;
        }
    }

    static class BufferedOutputToken {
        final String term;

        // Non-null if this was an incoming token:
        final State state;

        final int startNode;
        final int endNode;

        public BufferedOutputToken(State state, String term, int startNode, int endNode) {
            this.state = state;
            this.term = term;
            this.startNode = startNode;
            this.endNode = endNode;
        }
    }

    public SynonymGraphFilter(TokenStream input, SynonymMap synonyms, boolean ignoreCase) {
        super(input);
        this.synonyms = synonyms;
        this.fst = synonyms.fst;
        if (fst == null) {
            throw new IllegalArgumentException("fst must be non-null");
        }
        this.fstReader = fst.getBytesReader();
        scratchArc = new FST.Arc<>();
        this.ignoreCase = ignoreCase;
    }

    @Override
    public boolean incrementToken() throws IOException {
        //System.out.println("\nS: incrToken lastNodeOut=" + lastNodeOut + " nextNodeOut=" + nextNodeOut);

        assert lastNodeOut <= nextNodeOut;

        if (outputBuffer.isEmpty() == false) {
            // We still have pending outputs from a prior synonym match:
            releaseBufferedToken();
            //System.out.println("  syn: ret buffered=" + this);
            assert liveToken == false;
            return true;
        }

        // Try to parse a new synonym match at the current token:

        if (parse()) {
            // A new match was found:
            releaseBufferedToken();
            //System.out.println("  syn: after parse, ret buffered=" + this);
            assert liveToken == false;
            return true;
        }

        if (lookaheadNextRead == lookaheadNextWrite) {

            // Fast path: parse pulled one token, but it didn't match
            // the start for any synonym, so we now return it "live" w/o having
            // cloned all of its atts:
            if (finished) {
                //System.out.println("  syn: ret END");
                return false;
            }

            assert liveToken;
            liveToken = false;

            // NOTE: no need to change posInc since it's relative, i.e. whatever
            // node our output is upto will just increase by the incoming posInc.
            // We also don't need to change posLen, but only because we cannot
            // consume a graph, so the incoming token can never span a future
            // synonym match.

        } else {
            // We still have buffered lookahead tokens from a previous
            // parse attempt that required lookahead; just replay them now:
            //System.out.println("  restore buffer");
            assert lookaheadNextRead < lookaheadNextWrite : "read=" + lookaheadNextRead + " write=" + lookaheadNextWrite;
            BufferedInputToken token = lookahead.get(lookaheadNextRead);
            lookaheadNextRead++;

            restoreState(token.state);

            lookahead.freeBefore(lookaheadNextRead);

            //System.out.println("  after restore offset=" + offsetAtt.startOffset() + "-" + offsetAtt.endOffset());
            assert liveToken == false;
        }

        lastNodeOut += posIncrAtt.getPositionIncrement();
        nextNodeOut = lastNodeOut + posLenAtt.getPositionLength();

        //System.out.println("  syn: ret lookahead=" + this);

        return true;
    }

    private void releaseBufferedToken() throws IOException {
        //System.out.println("  releaseBufferedToken");

        BufferedOutputToken token = outputBuffer.pollFirst();

        if (token.state != null) {
            // This is an original input token (keepOrig=true case):
            //System.out.println("    hasState");
            restoreState(token.state);
            //System.out.println("    startOffset=" + offsetAtt.startOffset() + " endOffset=" + offsetAtt.endOffset());
        } else {
            clearAttributes();
            //System.out.println("    no state");
            termAtt.append(token.term);

            // We better have a match already:
            assert matchStartOffset != -1;

            offsetAtt.setOffset(matchStartOffset, matchEndOffset);
            //System.out.println("    startOffset=" + matchStartOffset + " endOffset=" + matchEndOffset);
            typeAtt.setType(TYPE_SYNONYM);
        }

        //System.out.println("    lastNodeOut=" + lastNodeOut);
        //System.out.println("    term=" + termAtt);

        posIncrAtt.setPositionIncrement(token.startNode - lastNodeOut);
        lastNodeOut = token.startNode;
        posLenAtt.setPositionLength(token.endNode - token.startNode);
        flagsAtt.setFlags(flagsAtt.getFlags() | GRAPH_FLAG);  // set the graph flag
    }

    /**
     * Scans the next input token(s) to see if a synonym matches.  Returns true
     * if a match was found.
     */
    private boolean parse() throws IOException {
        // System.out.println(Thread.currentThread().getName() + ": S: parse: " + System.identityHashCode(this));

        // Holds the longest match we've seen so far:
        BytesRef matchOutput = null;
        int matchInputLength = 0;

        BytesRef pendingOutput = fst.outputs.getNoOutput();
        fst.getFirstArc(scratchArc);

        assert scratchArc.output == fst.outputs.getNoOutput();

        // How many tokens in the current match
        int matchLength = 0;
        boolean doFinalCapture = false;

        int lookaheadUpto = lookaheadNextRead;
        matchStartOffset = -1;

        byToken:
        while (true) {
            //System.out.println("  cycle lookaheadUpto=" + lookaheadUpto + " maxPos=" + lookahead.getMaxPos());

            // Pull next token's chars:
            final char[] buffer;
            final int bufferLen;
            final int inputEndOffset;

            if (lookaheadUpto <= lookahead.getMaxPos()) {
                // Still in our lookahead buffer
                BufferedInputToken token = lookahead.get(lookaheadUpto);
                lookaheadUpto++;
                buffer = token.term.chars();
                bufferLen = token.term.length();
                inputEndOffset = token.endOffset;
                //System.out.println("    use buffer now max=" + lookahead.getMaxPos());
                if (matchStartOffset == -1) {
                    matchStartOffset = token.startOffset;
                }
            } else {

                // We used up our lookahead buffer of input tokens
                // -- pull next real input token:

                assert finished || liveToken == false;

                if (finished) {
                    //System.out.println("    break: finished");
                    break;
                } else if (input.incrementToken()) {
                    //System.out.println("    input.incrToken");
                    liveToken = true;
                    buffer = termAtt.buffer();
                    bufferLen = termAtt.length();
                    if (matchStartOffset == -1) {
                        matchStartOffset = offsetAtt.startOffset();
                    }
                    inputEndOffset = offsetAtt.endOffset();

                    lookaheadUpto++;
                } else {
                    // No more input tokens
                    finished = true;
                    //System.out.println("    break: now set finished");
                    break;
                }
            }

            matchLength++;
            //System.out.println("    cycle term=" + new String(buffer, 0, bufferLen));

            // Run each char in this token through the FST:
            int bufUpto = 0;
            while (bufUpto < bufferLen) {
                final int codePoint = Character.codePointAt(buffer, bufUpto, bufferLen);
                if (fst.findTargetArc(ignoreCase ? Character.toLowerCase(codePoint) : codePoint, scratchArc, scratchArc, fstReader) ==
                    null) {
                    break byToken;
                }

                // Accum the output
                pendingOutput = fst.outputs.add(pendingOutput, scratchArc.output);
                bufUpto += Character.charCount(codePoint);
            }

            assert bufUpto == bufferLen;

            // OK, entire token matched; now see if this is a final
            // state in the FST (a match):
            if (scratchArc.isFinal()) {
                matchOutput = fst.outputs.add(pendingOutput, scratchArc.nextFinalOutput);
                matchInputLength = matchLength;
                matchEndOffset = inputEndOffset;
                //System.out.println("    ** match");
            }

            // See if the FST can continue matching (ie, needs to
            // see the next input token):
            if (fst.findTargetArc(SynonymMap.WORD_SEPARATOR, scratchArc, scratchArc, fstReader) == null) {
                // No further rules can match here; we're done
                // searching for matching rules starting at the
                // current input position.
                break;
            } else {
                // More matching is possible -- accum the output (if
                // any) of the WORD_SEP arc:
                pendingOutput = fst.outputs.add(pendingOutput, scratchArc.output);
                doFinalCapture = true;
                if (liveToken) {
                    capture();
                }
            }
        }

        if (doFinalCapture && liveToken && finished == false) {
            // Must capture the final token if we captured any prior tokens:
            capture();
        }

        if (matchOutput != null) {

            if (liveToken) {
                // Single input token synonym; we must buffer it now:
                capture();
            }

            // There is a match!
            bufferOutputTokens(matchOutput, matchInputLength);
            lookaheadNextRead += matchInputLength;
            //System.out.println("  precmatch; set lookaheadNextRead=" + lookaheadNextRead + " now max=" + lookahead.getMaxPos());
            lookahead.freeBefore(lookaheadNextRead);
            //System.out.println("  match; set lookaheadNextRead=" + lookaheadNextRead + " now max=" + lookahead.getMaxPos());
            return true;
        } else {
            //System.out.println("  no match; lookaheadNextRead=" + lookaheadNextRead);
            return false;
        }

        //System.out.println("  parse done inputSkipCount=" + inputSkipCount + " nextRead=" + nextRead + " nextWrite=" + nextWrite);
    }

    /**
     * Expands the output graph into the necessary tokens, adding
     * synonyms as side paths parallel to the input tokens, and
     * buffers them in the output token buffer.
     */
    private void bufferOutputTokens(BytesRef bytes, int matchInputLength) {
        bytesReader.reset(bytes.bytes, bytes.offset, bytes.length);

        final int code = bytesReader.readVInt();
        final boolean keepOrig = (code & 0x1) == 0;
        //System.out.println("  buffer: keepOrig=" + keepOrig + " matchInputLength=" + matchInputLength);

        // How many nodes along all paths; we need this to assign the
        // node ID for the final end node where all paths merge back:
        int totalPathNodes;
        if (keepOrig) {
            assert matchInputLength > 0;
            totalPathNodes = matchInputLength - 1;
        } else {
            totalPathNodes = 0;
        }

        // How many synonyms we will insert over this match:
        final int count = code >>> 1;

        // TODO: we could encode this instead into the FST:

        // 1st pass: count how many new nodes we need
        List<List<String>> paths = new ArrayList<>();
        for (int outputIDX = 0; outputIDX < count; outputIDX++) {
            int wordID = bytesReader.readVInt();
            synonyms.words.get(wordID, scratchBytes);
            scratchChars.copyUTF8Bytes(scratchBytes);
            int lastStart = 0;

            List<String> path = new ArrayList<>();
            paths.add(path);
            int chEnd = scratchChars.length();
            for (int chUpto = 0; chUpto <= chEnd; chUpto++) {
                if (chUpto == chEnd || scratchChars.charAt(chUpto) == SynonymMap.WORD_SEPARATOR) {
                    path.add(new String(scratchChars.chars(), lastStart, chUpto - lastStart));
                    lastStart = 1 + chUpto;
                }
            }

            assert path.size() > 0;
            totalPathNodes += path.size() - 1;
        }
        //System.out.println("  totalPathNodes=" + totalPathNodes);

        // 2nd pass: buffer tokens for the graph fragment

        // NOTE: totalPathNodes will be 0 in the case where the matched
        // input is a single token and all outputs are also a single token

        // We "spawn" a side-path for each of the outputs for this matched
        // synonym, all ending back at this end node:

        int startNode = nextNodeOut;

        int endNode = startNode + totalPathNodes + 1;
        //System.out.println("  " + paths.size() + " new side-paths");

        // First, fanout all tokens departing start node for these new side paths:
        int newNodeCount = 0;
        for (List<String> path : paths) {
            int pathEndNode;
            //System.out.println("    path size=" + path.size());
            if (path.size() == 1) {
                // Single token output, so there are no intermediate nodes:
                pathEndNode = endNode;
            } else {
                pathEndNode = nextNodeOut + newNodeCount + 1;
                newNodeCount += path.size() - 1;
            }
            outputBuffer.add(new BufferedOutputToken(null, path.get(0), startNode, pathEndNode));
        }

        // We must do the original tokens last, else the offsets "go backwards":
        if (keepOrig) {
            BufferedInputToken token = lookahead.get(lookaheadNextRead);
            int inputEndNode;
            if (matchInputLength == 1) {
                // Single token matched input, so there are no intermediate nodes:
                inputEndNode = endNode;
            } else {
                inputEndNode = nextNodeOut + newNodeCount + 1;
            }

            //System.out.println("    keepOrig first token: " + token.term);

            outputBuffer.add(new BufferedOutputToken(token.state, token.term.toString(), startNode, inputEndNode));
        }

        nextNodeOut = endNode;

        // Do full side-path for each syn output:
        for (int pathID = 0; pathID < paths.size(); pathID++) {
            List<String> path = paths.get(pathID);
            if (path.size() > 1) {
                int lastNode = outputBuffer.get(pathID).endNode;
                for (int i = 1; i < path.size() - 1; i++) {
                    outputBuffer.add(new BufferedOutputToken(null, path.get(i), lastNode, lastNode + 1));
                    lastNode++;
                }
                outputBuffer.add(new BufferedOutputToken(null, path.get(path.size() - 1), lastNode, endNode));
            }
        }

        if (keepOrig && matchInputLength > 1) {
            // Do full "side path" with the original tokens:
            int lastNode = outputBuffer.get(paths.size()).endNode;
            for (int i = 1; i < matchInputLength - 1; i++) {
                BufferedInputToken token = lookahead.get(lookaheadNextRead + i);
                outputBuffer.add(new BufferedOutputToken(token.state, token.term.toString(), lastNode, lastNode + 1));
                lastNode++;
            }
            BufferedInputToken token = lookahead.get(lookaheadNextRead + matchInputLength - 1);
            outputBuffer.add(new BufferedOutputToken(token.state, token.term.toString(), lastNode, endNode));
        }

    /*
    System.out.println("  after buffer: " + outputBuffer.size() + " tokens:");
    for(BufferedOutputToken token : outputBuffer) {
      System.out.println("    tok: " + token.term + " startNode=" + token.startNode + " endNode=" + token.endNode);
    }
    */
    }

    /**
     * Buffers the current input token into lookahead buffer.
     */
    private void capture() {
        assert liveToken;
        liveToken = false;
        BufferedInputToken token = lookahead.get(lookaheadNextWrite);
        lookaheadNextWrite++;

        token.state = captureState();
        token.startOffset = offsetAtt.startOffset();
        token.endOffset = offsetAtt.endOffset();
        assert token.term.length() == 0;
        token.term.append(termAtt);

        captureCount++;
        maxLookaheadUsed = Math.max(maxLookaheadUsed, lookahead.getBufferSize());
        //System.out.println("  maxLookaheadUsed=" + maxLookaheadUsed);
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        lookahead.reset();
        lookaheadNextWrite = 0;
        lookaheadNextRead = 0;
        captureCount = 0;
        lastNodeOut = -1;
        nextNodeOut = 0;
        matchStartOffset = -1;
        matchEndOffset = -1;
        finished = false;
        liveToken = false;
        outputBuffer.clear();
        maxLookaheadUsed = 0;
        //System.out.println("S: reset");
    }

    // for testing
    int getCaptureCount() {
        return captureCount;
    }

    // for testing
    int getMaxLookaheadUsed() {
        return maxLookaheadUsed;
    }
}
