/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.test.ESTestCase;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Unit tests for {@link FilterLevelUsefulness}. Synthetic {@link BlockMetaData} so each
 * predicate / stats / encoding combination is exercised without writing Parquet files.
 */
public class FilterLevelUsefulnessTests extends ESTestCase {

    private static final String COL = "id";
    private static final EncodingStats PURE_DICTIONARY_STATS = new EncodingStats.Builder().addDictEncoding(Encoding.PLAIN)
        .addDataEncoding(Encoding.PLAIN_DICTIONARY)
        .build();
    private static final EncodingStats PLAIN_FALLBACK_STATS = new EncodingStats.Builder().addDictEncoding(Encoding.PLAIN)
        .addDataEncoding(Encoding.PLAIN_DICTIONARY)
        .addDataEncoding(Encoding.PLAIN)
        .build();

    // ----- Eq -----

    public void testEqAmbiguousOnDictionaryColumnIsDictAndBloomUseful() {
        BlockMetaData block = dictBlock(1, 10, 0L, 100L, true);
        assertUseful(FilterApi.eq(FilterApi.intColumn(COL), 5), block, true, true);
    }

    public void testEqMatchIsNeitherUseful() {
        BlockMetaData block = dictBlock(5, 5, 0L, 100L, true);
        assertUseful(FilterApi.eq(FilterApi.intColumn(COL), 5), block, false, false);
    }

    public void testEqMissIsNeitherUseful() {
        BlockMetaData block = dictBlock(10, 20, 0L, 100L, true);
        assertUseful(FilterApi.eq(FilterApi.intColumn(COL), 5), block, false, false);
    }

    public void testEqWithoutBloomLengthIsDictOnly() {
        BlockMetaData block = dictBlock(1, 10, 0L, 100L, false);
        assertUseful(FilterApi.eq(FilterApi.intColumn(COL), 5), block, true, false);
    }

    public void testEqUnknownBloomLengthIsNotBloomUseful() {
        BlockMetaData block = dictBlock(1, 10, 0L, 100L, false);
        block.getColumns().get(0).setBloomFilterOffset(1_000L);
        assertUseful(FilterApi.eq(FilterApi.intColumn(COL), 5), block, true, false);
    }

    public void testEqOnPlainFallbackIsNotDictUseful() {
        BlockMetaData block = blockWith(1, 10, 0L, 100L, PLAIN_FALLBACK_STATS, false);
        assertFalse(ParquetFormatReader.dictionaryFilterReadsDictionary(block.getColumns().get(0)));
        assertUseful(FilterApi.eq(FilterApi.intColumn(COL), 5), block, false, false);
    }

    public void testEqNullOnMixedNullsIsNeitherUseful() {
        BlockMetaData block = dictBlock(1, 10, 5L, 100L, true);
        assertUseful(FilterApi.eq(FilterApi.intColumn(COL), (Integer) null), block, false, false);
    }

    public void testNotEqNullOnMixedNullsIsNeitherUseful() {
        BlockMetaData block = dictBlock(1, 10, 5L, 100L, true);
        assertUseful(FilterApi.notEq(FilterApi.intColumn(COL), (Integer) null), block, false, false);
    }

    public void testNotEqNullWithMissingStatsIsNeitherUseful() {
        BlockMetaData block = dictBlockMissingStats(true);
        assertUseful(FilterApi.notEq(FilterApi.intColumn(COL), (Integer) null), block, false, false);
    }

    // ----- In / NotIn -----

    public void testInAmbiguousIsDictAndBloomUseful() {
        BlockMetaData block = dictBlock(1, 10, 0L, 100L, true);
        assertUseful(FilterApi.in(FilterApi.intColumn(COL), Set.of(5, 50)), block, true, true);
    }

    public void testInMatchIsNeitherUseful() {
        BlockMetaData block = dictBlock(5, 5, 0L, 100L, true);
        assertUseful(FilterApi.in(FilterApi.intColumn(COL), Set.of(5, 50)), block, false, false);
    }

    public void testNotInAmbiguousIsDictUsefulNotBloom() {
        BlockMetaData block = dictBlock(1, 10, 0L, 100L, true);
        assertUseful(FilterApi.notIn(FilterApi.intColumn(COL), Set.of(5, 50)), block, true, false);
    }

    public void testNotInMatchIsNeitherUseful() {
        BlockMetaData block = dictBlock(50, 60, 0L, 100L, true);
        assertUseful(FilterApi.notIn(FilterApi.intColumn(COL), Set.of(5, 10)), block, false, false);
    }

    public void testInContainingNullIsNeitherUseful() {
        BlockMetaData block = dictBlock(1, 10, 5L, 100L, true);
        Set<Integer> values = new HashSet<>();
        values.add(5);
        values.add(null);
        assertUseful(FilterApi.in(FilterApi.intColumn(COL), values), block, false, false);
    }

    public void testNotInContainingNullIsNeitherUseful() {
        BlockMetaData block = dictBlock(1, 10, 5L, 100L, true);
        Set<Integer> values = new HashSet<>();
        values.add(5);
        values.add(null);
        assertUseful(FilterApi.notIn(FilterApi.intColumn(COL), values), block, false, false);
    }

    // ----- comparisons / NotEq: useful only when min/max missing -----

    public void testLtAmbiguousWithStatsIsNotDictUseful() {
        BlockMetaData block = dictBlock(1, 10, 0L, 100L, true);
        assertUseful(FilterApi.lt(FilterApi.intColumn(COL), 5), block, false, false);
    }

    public void testGtAmbiguousWithStatsIsNotDictUseful() {
        BlockMetaData block = dictBlock(1, 10, 0L, 100L, true);
        assertUseful(FilterApi.gt(FilterApi.intColumn(COL), 5), block, false, false);
    }

    public void testGtEqAmbiguousWithStatsIsNotDictUseful() {
        BlockMetaData block = dictBlock(1, 10, 0L, 100L, true);
        assertUseful(FilterApi.gtEq(FilterApi.intColumn(COL), 5), block, false, false);
    }

    public void testNotEqAmbiguousWithStatsIsNotDictUseful() {
        BlockMetaData block = dictBlock(1, 10, 0L, 100L, true);
        assertUseful(FilterApi.notEq(FilterApi.intColumn(COL), 5), block, false, false);
    }

    public void testLtMatchIsNeitherUseful() {
        BlockMetaData block = dictBlock(0, 9, 0L, 100L, true);
        assertUseful(FilterApi.lt(FilterApi.intColumn(COL), 10), block, false, false);
    }

    public void testLtWithMissingStatsIsDictUseful() {
        BlockMetaData block = dictBlockMissingStats(true);
        assertUseful(FilterApi.lt(FilterApi.intColumn(COL), 5), block, true, false);
    }

    public void testNotEqWithMissingStatsIsDictUseful() {
        BlockMetaData block = dictBlockMissingStats(true);
        assertUseful(FilterApi.notEq(FilterApi.intColumn(COL), 5), block, true, false);
    }

    public void testGtWithMissingStatsIsDictUseful() {
        BlockMetaData block = dictBlockMissingStats(true);
        assertUseful(FilterApi.gt(FilterApi.intColumn(COL), 5), block, true, false);
    }

    // ----- UserDefined -----

    public void testUserDefinedAmbiguousIsDictUsefulNotBloom() {
        BlockMetaData block = dictBlock(1, 10, 0L, 100L, true);
        assertUseful(FilterApi.userDefined(FilterApi.intColumn(COL), NeverDropUdp.class), block, true, false);
    }

    // ----- AND / OR / NOT -----

    public void testAndIsUsefulIfAnyLeafIsUseful() {
        // range MATCH (not useful) AND eq AMBIGUOUS (dict+bloom useful)
        FilterPredicate pred = FilterApi.and(FilterApi.gtEq(FilterApi.intColumn(COL), 0), FilterApi.eq(FilterApi.intColumn(COL), 5));
        BlockMetaData block = dictBlock(1, 10, 0L, 100L, true);
        assertUseful(pred, block, true, true);
    }

    public void testAndIsNotUsefulWhenNoLeafIsUseful() {
        FilterPredicate pred = FilterApi.and(FilterApi.gtEq(FilterApi.intColumn(COL), 0), FilterApi.ltEq(FilterApi.intColumn(COL), 20));
        BlockMetaData block = dictBlock(1, 10, 0L, 100L, true);
        assertUseful(pred, block, false, false);
    }

    public void testOrIsUsefulIfAnyLeafIsUseful() {
        FilterPredicate pred = FilterApi.or(FilterApi.lt(FilterApi.intColumn(COL), 5), FilterApi.eq(FilterApi.intColumn(COL), 5));
        BlockMetaData block = dictBlock(1, 10, 0L, 100L, true);
        assertUseful(pred, block, true, true);
    }

    public void testNotDelegatesToInnerLeaf() {
        BlockMetaData block = dictBlock(1, 10, 0L, 100L, true);
        assertUseful(FilterApi.not(FilterApi.eq(FilterApi.intColumn(COL), 5)), block, true, true);
        assertUseful(FilterApi.not(FilterApi.lt(FilterApi.intColumn(COL), 5)), block, false, false);
    }

    // ----- survivors / null predicate -----

    public void testFillLeavesNonSurvivorsFalse() {
        BlockMetaData useful = dictBlock(1, 10, 0L, 100L, true);
        boolean[] dict = new boolean[2];
        boolean[] bloom = new boolean[2];
        FilterLevelUsefulness.fill(
            FilterApi.eq(FilterApi.intColumn(COL), 5),
            List.of(useful, useful),
            new boolean[] { true, false },
            dict,
            bloom
        );
        assertTrue(dict[0]);
        assertTrue(bloom[0]);
        assertFalse(dict[1]);
        assertFalse(bloom[1]);
    }

    public void testNullPredicateIsNoOp() {
        boolean[] dict = new boolean[] { true };
        boolean[] bloom = new boolean[] { true };
        FilterLevelUsefulness.fill(null, List.of(dictBlock(1, 10, 0L, 100L, true)), new boolean[] { true }, dict, bloom);
        assertTrue(dict[0]);
        assertTrue(bloom[0]);
    }

    public void testMissingColumnIsNeitherUseful() {
        BlockMetaData block = dictBlock(1, 10, 0L, 100L, true);
        assertUseful(FilterApi.eq(FilterApi.intColumn("other"), 5), block, false, false);
    }

    // ----- helpers -----

    private static void assertUseful(FilterPredicate pred, BlockMetaData block, boolean dict, boolean bloom) {
        boolean[] dictOut = new boolean[1];
        boolean[] bloomOut = new boolean[1];
        FilterLevelUsefulness.fill(pred, List.of(block), new boolean[] { true }, dictOut, bloomOut);
        assertEquals("dictUseful", dict, dictOut[0]);
        assertEquals("bloomUseful", bloom, bloomOut[0]);
    }

    private static BlockMetaData dictBlock(int min, int max, long numNulls, long valueCount, boolean bloom) {
        IntStatistics stats = new IntStatistics();
        stats.setMinMax(min, max);
        stats.setNumNulls(numNulls);
        return blockWith(stats, valueCount, PURE_DICTIONARY_STATS, bloom);
    }

    private static BlockMetaData dictBlockMissingStats(boolean bloom) {
        return blockWith(new IntStatistics(), 100L, PURE_DICTIONARY_STATS, bloom);
    }

    private static BlockMetaData blockWith(int min, int max, long numNulls, long valueCount, EncodingStats encodingStats, boolean bloom) {
        IntStatistics stats = new IntStatistics();
        stats.setMinMax(min, max);
        stats.setNumNulls(numNulls);
        return blockWith(stats, valueCount, encodingStats, bloom);
    }

    private static BlockMetaData blockWith(Statistics<?> stats, long valueCount, EncodingStats encodingStats, boolean bloom) {
        PrimitiveType type = Types.required(stats.type().getPrimitiveTypeName()).named(COL);
        ColumnChunkMetaData ccm = ColumnChunkMetaData.get(
            ColumnPath.get(COL),
            type,
            CompressionCodecName.UNCOMPRESSED,
            encodingStats,
            EnumSet.of(Encoding.PLAIN, Encoding.PLAIN_DICTIONARY),
            stats,
            100L,
            1L,
            valueCount,
            200L,
            200L
        );
        if (bloom) {
            ccm.setBloomFilterOffset(1_000L);
            ccm.setBloomFilterLength(64);
        }
        BlockMetaData block = new BlockMetaData();
        block.setRowCount(valueCount);
        block.addColumn(ccm);
        return block;
    }

    public static final class NeverDropUdp extends UserDefinedPredicate<Integer> {
        @Override
        public boolean keep(Integer value) {
            return true;
        }

        @Override
        public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Integer> statistics) {
            return false;
        }

        @Override
        public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Integer> statistics) {
            return false;
        }
    }
}
