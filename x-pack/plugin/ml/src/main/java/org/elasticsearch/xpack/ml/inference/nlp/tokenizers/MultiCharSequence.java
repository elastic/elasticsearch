/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.util.CharsRef;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MultiCharSequence implements CharSequence {

    private final int[] lengths;
    private final List<CharSequence> sequenceList;

    public static MultiCharSequence from(CharSequence... sequences) {
        List<CharSequence> sequenceList = new ArrayList<>(sequences.length);
        sequenceList.addAll(Arrays.asList(sequences));
        return new MultiCharSequence(sequenceList);
    }

    public MultiCharSequence(List<CharSequence> sequenceList) {
        this.sequenceList = sequenceList;
        this.lengths = new int[sequenceList.size()];
        int i = 0;
        int length = 0;
        for (CharSequence sequence : sequenceList) {
            length += sequence.length();
            lengths[i++] = length;
        }
    }

    @Override
    public int length() {
        if (lengths.length == 0) {
            return 0;
        }
        return lengths[lengths.length - 1];
    }

    @Override
    public char charAt(int index) {
        if (lengths.length == 0) {
            throw new IndexOutOfBoundsException(index);
        }
        int sequenceIndex = Arrays.binarySearch(lengths, index + 1);
        if (sequenceIndex < 0) {
            sequenceIndex = -1 - sequenceIndex;
        }
        CharSequence sequence = sequenceList.get(sequenceIndex);
        if (sequenceIndex == 0) {
            return sequence.charAt(index);
        }
        return sequence.charAt(index - lengths[sequenceIndex - 1]);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        if (start > end) {
            throw new IndexOutOfBoundsException();
        }
        if (lengths.length == 0) {
            throw new IndexOutOfBoundsException();
        }
        if (start == 0 && end >= length()) {
            return this;
        }
        if (start == end) {
            return new CharsRef(CharsRef.EMPTY_CHARS, 0, 0);
        }

        int startIndex = Arrays.binarySearch(lengths, start);
        if (startIndex < 0) {
            startIndex = -1 - startIndex;
        }
        int endIndex = Arrays.binarySearch(lengths, end);
        if (endIndex < 0) {
            endIndex = -1 - endIndex;
        }
        if (endIndex > lengths.length - 1) {
            endIndex = lengths.length - 1;
        }
        if (startIndex == endIndex) {
            if (startIndex == 0) {
                return sequenceList.get(startIndex).subSequence(start, end);
            } else {
                return sequenceList.get(startIndex).subSequence(start - lengths[startIndex - 1], end - lengths[startIndex - 1]);
            }
        }
        List<CharSequence> sequences = new ArrayList<>((endIndex - startIndex) + 1);
        if (startIndex == 0) {
            sequences.add(sequenceList.get(startIndex).subSequence(start, sequenceList.get(startIndex).length()));
        } else {
            sequences.add(sequenceList.get(startIndex).subSequence(start - lengths[startIndex - 1], sequenceList.get(startIndex).length()));
        }
        if (endIndex - startIndex > 1) {
            sequences.addAll(sequenceList.subList(startIndex + 1, endIndex));
        }
        sequences.add(sequenceList.get(endIndex).subSequence(0, end - lengths[endIndex - 1]));
        return new MultiCharSequence(sequences);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (CharSequence sequence : sequenceList) {
            builder.append(sequence);
        }
        return builder.toString();
    }
}
