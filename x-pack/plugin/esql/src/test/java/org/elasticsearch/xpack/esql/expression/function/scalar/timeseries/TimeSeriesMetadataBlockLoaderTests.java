/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.lucene.read.DelegatingBlockLoaderFactory;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromOrdsBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.LongsBlockLoader;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataBlockLoader.Dimension;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataBlockLoader.Values;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/** Loads {@code _timeseries} through {@link TimeSeriesMetadataBlockLoader}, from the doc values of real dimensions. */
public class TimeSeriesMetadataBlockLoaderTests extends ComputeTestCase {
    private static final ByteSizeValue ORDINALS = ByteSizeValue.ofMb(1);

    /** Each document loads as the label container of its dimensions: a keyword as its bytes, an ip and a number as their text. */
    public void testLoadsTheLabelsOfEachDocument() throws IOException {
        TimeSeriesMetadataBlockLoader loader = new TimeSeriesMetadataBlockLoader(
            List.of(
                new Dimension("pod", new BytesRefsFromOrdsBlockLoader("labels.pod", ORDINALS), Values.AS_LOADED),
                new Dimension("zone", new BytesRefsFromOrdsBlockLoader("zone", ORDINALS), Values.AS_LOADED),
                new Dimension("host", new BytesRefsFromOrdsBlockLoader("host", ORDINALS), Values.IP),
                new Dimension("port", new LongsBlockLoader("port"), Values.AS_LOADED)
            )
        );
        List<BytesRef> loaded = load(loader, List.of(series("a", "10.0.0.1", 8080, "west", "east"), series("b", null, null)));
        assertThat(
            loaded,
            equalTo(
                List.of(
                    TimeSeriesMetadataEncoding.encode(
                        Map.of("pod", List.of("a"), "zone", List.of("east", "west"), "host", List.of("10.0.0.1"), "port", List.of("8080"))
                    ),
                    TimeSeriesMetadataEncoding.encode(Map.of("pod", List.of("b")))
                )
            )
        );
    }

    /** A series without dimensions loads as the label container without labels. */
    public void testLoadsTheEmptyLabelContainer() throws IOException {
        assertThat(
            load(new TimeSeriesMetadataBlockLoader(List.of()), List.of(series("a", null, null))),
            equalTo(List.of(TimeSeriesMetadataEncoding.EMPTY))
        );
    }

    /** Two dimensions PromQL names alike cannot both label a series. */
    public void testTwoDimensionsOfOneLabelFail() {
        TimeSeriesMetadataBlockLoader loader = new TimeSeriesMetadataBlockLoader(
            List.of(
                new Dimension("pod", new BytesRefsFromOrdsBlockLoader("labels.pod", ORDINALS), Values.AS_LOADED),
                new Dimension("pod", new BytesRefsFromOrdsBlockLoader("zone", ORDINALS), Values.AS_LOADED)
            )
        );
        Exception e = expectThrows(IllegalArgumentException.class, () -> load(loader, List.of(series("a", null, null, "west"))));
        assertThat(e.getMessage(), containsString("multiple dimension fields resolve to time-series label [pod]"));
    }

    private static Document series(String pod, String host, Integer port, String... zones) {
        Document document = new Document();
        document.add(new SortedSetDocValuesField("labels.pod", new BytesRef(pod)));
        if (host != null) {
            document.add(new SortedSetDocValuesField("host", new BytesRef(InetAddressPoint.encode(InetAddresses.forString(host)))));
        }
        if (port != null) {
            document.add(new SortedNumericDocValuesField("port", port));
        }
        for (String zone : zones) {
            document.add(new SortedSetDocValuesField("zone", new BytesRef(zone)));
        }
        return document;
    }

    /** The label containers {@code loader} loads for {@code documents}, read column at a time from one segment. */
    private List<BytesRef> load(TimeSeriesMetadataBlockLoader loader, List<Document> documents) throws IOException {
        BlockFactory blockFactory = blockFactory();
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                for (Document document : documents) {
                    writer.addDocument(document);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReaderContext leaf = reader.leaves().getFirst();
                int[] docs = new int[documents.size()];
                for (int i = 0; i < docs.length; i++) {
                    docs[i] = i;
                }
                var factory = new DelegatingBlockLoaderFactory(blockFactory) {
                    @Override
                    public BlockLoader.Block constantNulls(int count) {
                        return factory.newConstantNullBlock(count);
                    }
                };
                try (
                    BlockLoader.ColumnAtATimeReader columns = loader.columnAtATimeReader(leaf).apply(blockFactory.breaker());
                    Block block = (Block) columns.read(factory, TestBlock.docs(docs), 0, false)
                ) {
                    BytesRefBlock containers = (BytesRefBlock) block;
                    return IntStream.range(0, containers.getPositionCount())
                        .mapToObj(p -> BytesRef.deepCopyOf(containers.getBytesRef(p, new BytesRef())))
                        .toList();
                }
            }
        }
    }
}
