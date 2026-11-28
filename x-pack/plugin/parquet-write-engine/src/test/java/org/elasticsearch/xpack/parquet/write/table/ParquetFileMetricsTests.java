/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.table;

import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.parquet.write.file.IndexInputFile;
import org.elasticsearch.xpack.parquet.write.file.LocalParquetFiles;
import org.elasticsearch.xpack.parquet.write.file.ParquetCodecs;
import org.elasticsearch.xpack.parquet.write.file.ParquetMerger;
import org.elasticsearch.xpack.parquet.write.file.ParquetReadOptionsFactory;
import org.elasticsearch.xpack.parquet.write.file.TestLocalFiles;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class ParquetFileMetricsTests extends ESTestCase {

    private Directory directory;

    @Before
    public void openDirectory() {
        directory = new ByteBuffersDirectory();
    }

    @After
    public void closeDirectory() throws IOException {
        directory.close();
    }

    /** A merged file of rows 0 to 999 whose columns have the ids 1 (count), 2 (@timestamp) and 3 (host.name). */
    private String mergedFile() throws IOException {
        final LocalParquetFiles.LocalFile local = TestLocalFiles.write(directory, 0, 999, false);
        final MessageType union = ParquetMerger.unionSchema(directory, List.of(local));
        final MessageType withIds = ShardTable.withFieldIds(union, Map.of("count", 1, "@timestamp", 2, "host.name", 3));
        ParquetMerger.merge(directory, List.of(local), withIds, "merged.parquet", CompressionCodecName.SNAPPY, 1 << 14);
        return "merged.parquet";
    }

    public void testCountsAndBoundsFromTheFooter() throws Exception {
        final Metrics metrics = ParquetFileMetrics.read(new IndexInputFile(directory, mergedFile()));

        assertThat(metrics.recordCount(), equalTo(1000L));
        assertThat(metrics.valueCounts(), equalTo(Map.of(1, 1000L, 2, 1000L, 3, 1000L)));
        // every seventh row has no keyword: rows 0, 7, ... 994
        assertThat(metrics.nullValueCounts(), equalTo(Map.of(1, 0L, 2, 0L, 3, 143L)));
        assertThat(metrics.columnSizes().keySet(), equalTo(java.util.Set.of(1, 2, 3)));

        // a long bound is the long
        assertThat(Conversions.fromByteBuffer(Types.LongType.get(), metrics.lowerBounds().get(1)), equalTo(0L));
        assertThat(Conversions.fromByteBuffer(Types.LongType.get(), metrics.upperBounds().get(1)), equalTo(999L));
        // a timestamp bound has the precision of an Iceberg timestamp, microseconds, and the file has milliseconds
        assertThat(
            Conversions.fromByteBuffer(Types.TimestampType.withZone(), metrics.lowerBounds().get(2)),
            equalTo(TestLocalFiles.BASE_TIMESTAMP * 1000)
        );
        assertThat(
            Conversions.fromByteBuffer(Types.TimestampType.withZone(), metrics.upperBounds().get(2)),
            equalTo((TestLocalFiles.BASE_TIMESTAMP + 999_000) * 1000)
        );
        // no bound for a string
        assertThat(metrics.lowerBounds(), not(hasKey(3)));
        assertThat(metrics.upperBounds(), not(hasKey(3)));
    }

    /** On the flat class path of a test there is one copy of the Parquet classes, so Iceberg's own computation is possible to compare. */
    public void testTheCountsAreTheOnesThatIcebergComputes() throws Exception {
        final String name = mergedFile();
        final Metrics ours = ParquetFileMetrics.read(new IndexInputFile(directory, name));

        final Metrics icebergs;
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new IndexInputFile(directory, name),
                ParquetReadOptionsFactory.create(new ParquetCodecs())
            )
        ) {
            icebergs = ParquetUtil.footerMetrics(reader.getFooter(), Stream.empty(), MetricsConfig.getDefault());
        }

        assertThat(ours.recordCount(), equalTo(icebergs.recordCount()));
        assertThat(ours.valueCounts(), equalTo(icebergs.valueCounts()));
        assertThat(ours.nullValueCounts(), equalTo(icebergs.nullValueCounts()));
        assertThat(ours.lowerBounds().get(1), equalTo(icebergs.lowerBounds().get(1)));
        assertThat(ours.upperBounds().get(1), equalTo(icebergs.upperBounds().get(1)));
    }
}
