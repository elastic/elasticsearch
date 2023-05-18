/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;

import java.io.IOException;
import java.util.function.Supplier;

public class ES87TSDBDocValuesFormatTests extends BaseDocValuesFormatTestCase {

    private final Codec codec = TestUtil.alwaysDocValuesFormat(new ES87TSDBDocValuesFormat());

    @Override
    protected Codec getCodec() {
        return codec;
    }

    // NOTE: here and below we disable tests dealing with non-numeric fields
    // because ES87TSDBDocValuesFormat only deals with numeric fields.
    @Override
    public void testTwoBinaryValues() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testVariouslyCompressibleBinaryValues() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testTwoFieldsMixed() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testThreeFieldsMixed() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testThreeFieldsMixed2() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testBytes() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testBytesTwoDocumentsMerged() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testBytesMergeAwayAllValues() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedBytes() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedBytesTwoDocuments() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedBytesThreeDocuments() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedBytesTwoDocumentsMerged() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedMergeAwayAllValues() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testBytesWithNewline() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testMissingSortedBytes() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedTermsEnum() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testEmptySortedBytes() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testEmptyBytes() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testVeryLargeButLegalBytes() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testVeryLargeButLegalSortedBytes() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testCodecUsesOwnBytes() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testCodecUsesOwnSortedBytes() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testDocValuesSimple() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testRandomSortedBytes() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testBinaryFixedLengthVsStoredFields() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSparseBinaryFixedLengthVsStoredFields() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testBinaryVariableLengthVsStoredFields() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSparseBinaryVariableLengthVsStoredFields() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void doTestBinaryVariableLengthVsStoredFields(double density) throws Exception {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    protected void doTestSortedVsStoredFields(int numDocs, double density, Supplier<byte[]> bytes) throws Exception {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedFixedLengthVsStoredFields() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSparseSortedFixedLengthVsStoredFields() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedVariableLengthVsStoredFields() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSparseSortedVariableLengthVsStoredFields() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    protected void doTestSortedVsStoredFields(int numDocs, double density, int minLength, int maxLength) throws Exception {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetOneValue() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetTwoFields() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetTwoDocumentsMerged() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetTwoValues() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetTwoValuesUnordered() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetThreeValuesTwoDocs() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetTwoDocumentsLastMissing() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetTwoDocumentsLastMissingMerge() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetTwoDocumentsFirstMissing() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetTwoDocumentsFirstMissingMerge() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetMergeAwayAllValues() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetTermsEnum() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetFixedLengthVsStoredFields() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetVariableLengthVsStoredFields() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetFixedLengthSingleValuedVsStoredFields() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetVariableLengthSingleValuedVsStoredFields() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetFixedLengthFewUniqueSetsVsStoredFields() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetVariableLengthFewUniqueSetsVsStoredFields() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetVariableLengthManyValuesPerDocVsStoredFields() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetFixedLengthManyValuesPerDocVsStoredFields() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSparseGCDCompression() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testTwoBytesOneMissing() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testTwoBytesOneMissingWithMerging() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testThreeBytesOneMissingWithMerging() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testThreads() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testEmptyBinaryValueOnPageSizes() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testOneSortedNumber() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testOneSortedNumberOneMissing() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testTwoSortedNumber() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testTwoSortedNumberSameValue() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testTwoSortedNumberOneMissing() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedNumberMerge() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedNumberMergeAwayAllValues() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedEnumAdvanceIndependently() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetEnumAdvanceIndependently() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedMergeAwayAllValuesLargeSegment() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testSortedSetMergeAwayAllValuesLargeSegment() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testBinaryMergeAwayAllValuesLargeSegment() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testRandomAdvanceBinary() throws IOException {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testHighOrdsSortedSetDV() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testCheckIntegrityReadsAllBytes() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testMergeStability() {
        assumeTrue("doc values format only supports numerics", false);
    }

    @Override
    public void testRandomExceptions() {
        assumeTrue("doc values format only supports numerics", false);
    }
}
