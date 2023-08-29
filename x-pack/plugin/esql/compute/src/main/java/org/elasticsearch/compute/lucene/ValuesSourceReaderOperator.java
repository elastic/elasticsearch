/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Operator that extracts doc_values from a Lucene index out of pages that have been produced by {@link LuceneSourceOperator}
 * and outputs them to a new column. The operator leverages the {@link ValuesSource} infrastructure for extracting
 * field values. This allows for a more uniform way of extracting data compared to deciding the correct doc_values
 * loader for different field types.
 */
public class ValuesSourceReaderOperator extends AbstractPageMappingOperator {
    /**
     * Creates a new extractor that uses ValuesSources load data
     * @param sources the value source, type and index readers to use for extraction
     * @param docChannel the channel containing the shard, leaf/segment and doc id
     * @param field the lucene field being loaded
     */
    public record ValuesSourceReaderOperatorFactory(List<ValueSourceInfo> sources, int docChannel, String field)
        implements
            OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new ValuesSourceReaderOperator(sources, docChannel, field);
        }

        @Override
        public String describe() {
            return "ValuesSourceReaderOperator[field = " + field + "]";
        }
    }

    private final List<ValueSourceInfo> sources;
    private final int docChannel;
    private final String field;

    private BlockDocValuesReader lastReader;
    private int lastShard = -1;
    private int lastSegment = -1;

    private final Map<String, Integer> readersBuilt = new TreeMap<>();

    /**
     * Creates a new extractor
     * @param sources the value source, type and index readers to use for extraction
     * @param docChannel the channel containing the shard, leaf/segment and doc id
     * @param field the lucene field being loaded
     */
    public ValuesSourceReaderOperator(List<ValueSourceInfo> sources, int docChannel, String field) {
        this.sources = sources;
        this.docChannel = docChannel;
        this.field = field;
    }

    @Override
    protected Page process(Page page) {
        DocVector docVector = page.<DocBlock>getBlock(docChannel).asVector();

        try {
            if (docVector.singleSegmentNonDecreasing()) {
                return page.appendBlock(loadFromSingleLeaf(docVector));
            }
            return page.appendBlock(loadFromManyLeaves(docVector));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Block loadFromSingleLeaf(DocVector docVector) throws IOException {
        setupReader(docVector.shards().getInt(0), docVector.segments().getInt(0), docVector.docs().getInt(0));
        return lastReader.readValues(docVector.docs());
    }

    private Block loadFromManyLeaves(DocVector docVector) throws IOException {
        int[] forwards = docVector.shardSegmentDocMapForwards();
        int doc = docVector.docs().getInt(forwards[0]);
        setupReader(docVector.shards().getInt(forwards[0]), docVector.segments().getInt(forwards[0]), doc);
        Block.Builder builder = lastReader.builder(forwards.length);
        lastReader.readValuesFromSingleDoc(doc, builder);
        for (int i = 1; i < forwards.length; i++) {
            int shard = docVector.shards().getInt(forwards[i]);
            int segment = docVector.segments().getInt(forwards[i]);
            doc = docVector.docs().getInt(forwards[i]);
            if (segment != lastSegment || shard != lastShard) {
                setupReader(shard, segment, doc);
            }
            lastReader.readValuesFromSingleDoc(doc, builder);
        }
        // TODO maybe it's better for downstream consumers if we perform a copy here.
        return builder.build().filter(docVector.shardSegmentDocMapBackwards());
    }

    private void setupReader(int shard, int segment, int doc) throws IOException {
        if (lastSegment == segment && lastShard == shard && BlockDocValuesReader.canReuse(lastReader, doc)) {
            return;
        }
        var info = sources.get(shard);
        LeafReaderContext leafReaderContext = info.reader().leaves().get(segment);
        lastReader = BlockDocValuesReader.createBlockReader(info.source(), info.type(), info.elementType(), leafReaderContext);
        lastShard = shard;
        lastSegment = segment;
        readersBuilt.compute(lastReader.toString(), (k, v) -> v == null ? 1 : v + 1);
    }

    @Override
    public String toString() {
        return "ValuesSourceReaderOperator[field = " + field + "]";
    }

    @Override
    protected Status status(int pagesProcessed) {
        return new Status(new TreeMap<>(readersBuilt), pagesProcessed);
    }

    public static class Status extends AbstractPageMappingOperator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "values_source_reader",
            Status::new
        );

        private final Map<String, Integer> readersBuilt;

        Status(Map<String, Integer> readersBuilt, int pagesProcessed) {
            super(pagesProcessed);
            this.readersBuilt = readersBuilt;
        }

        Status(StreamInput in) throws IOException {
            super(in);
            readersBuilt = in.readOrderedMap(StreamInput::readString, StreamInput::readVInt);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(readersBuilt, StreamOutput::writeString, StreamOutput::writeVInt);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public Map<String, Integer> readersBuilt() {
            return readersBuilt;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject("readers_built");
            for (Map.Entry<String, Integer> e : readersBuilt.entrySet()) {
                builder.field(e.getKey(), e.getValue());
            }
            builder.endObject();
            builder.field("pages_processed", pagesProcessed());
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return pagesProcessed() == status.pagesProcessed() && readersBuilt.equals(status.readersBuilt);
        }

        @Override
        public int hashCode() {
            return Objects.hash(readersBuilt, pagesProcessed());
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
