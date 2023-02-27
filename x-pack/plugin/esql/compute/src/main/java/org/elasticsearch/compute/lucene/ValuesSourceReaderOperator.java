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
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
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
@Experimental
public class ValuesSourceReaderOperator implements Operator {

    private final List<ValueSourceInfo> sources;
    private final int docChannel;

    private BlockDocValuesReader lastReader;
    private int lastShard = -1;
    private int lastSegment = -1;

    private Page lastPage;

    private final Map<String, Integer> readersBuilt = new TreeMap<>();
    private int pagesProcessed;

    boolean finished;

    /**
     * Creates a new extractor that uses ValuesSources load data
     * @param sources the value source, type and index readers to use for extraction
     * @param docChannel the channel containing the shard, leaf/segment and doc id
     * @param field the lucene field to use
     */
    public record ValuesSourceReaderOperatorFactory(List<ValueSourceInfo> sources, int docChannel, String field)
        implements
            OperatorFactory {
        @Override
        public Operator get() {
            return new ValuesSourceReaderOperator(sources, docChannel);
        }

        @Override
        public String describe() {
            return "ValuesSourceReaderOperator(field = " + field + ")";
        }
    }

    /**
     * Creates a new extractor
     * @param sources the value source, type and index readers to use for extraction
     * @param docChannel the channel containing the shard, leaf/segment and doc id
     */
    public ValuesSourceReaderOperator(List<ValueSourceInfo> sources, int docChannel) {
        this.sources = sources;
        this.docChannel = docChannel;
    }

    @Override
    public Page getOutput() {
        Page l = lastPage;
        lastPage = null;
        return l;
    }

    @Override
    public boolean isFinished() {
        return finished && lastPage == null;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean needsInput() {
        return lastPage == null;
    }

    @Override
    public void addInput(Page page) {
        DocVector docVector = page.<DocBlock>getBlock(docChannel).asVector();
        IntVector shardOrd = docVector.shards();
        IntVector leafOrd = docVector.segments();
        IntVector docs = docVector.docs();
        if (leafOrd.isConstant() == false) {
            throw new IllegalArgumentException("Expected constant block, got: " + leafOrd);
        }
        if (shardOrd.isConstant() == false) {
            throw new IllegalArgumentException("Expected constant block, got: " + shardOrd);
        }
        if (docs.isNonDecreasing() == false) {
            throw new IllegalArgumentException("Expected non decreasing block, got: " + docs);
        }

        if (docs.getPositionCount() > 0) {
            int segment = leafOrd.getInt(0);
            int shard = shardOrd.getInt(0);
            int firstDoc = docs.getInt(0);
            try {
                if (lastShard != shard || lastSegment != segment || BlockDocValuesReader.canReuse(lastReader, firstDoc) == false) {
                    var info = sources.get(shard);
                    LeafReaderContext leafReaderContext = info.reader().leaves().get(segment);

                    lastReader = BlockDocValuesReader.createBlockReader(info.source(), info.type(), info.elementType(), leafReaderContext);
                    lastShard = shard;
                    lastSegment = segment;
                    readersBuilt.compute(lastReader.toString(), (k, v) -> v == null ? 1 : v + 1);
                }
                Block block = lastReader.readValues(docs);
                pagesProcessed++;
                lastPage = page.appendBlock(block);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void close() {

    }

    @Override
    public String toString() {
        return "ValuesSourceReaderOperator";
    }

    @Override
    public Status status() {
        return new Status(new TreeMap<>(readersBuilt), pagesProcessed);
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "values_source_reader",
            Status::new
        );

        private final Map<String, Integer> readersBuilt;
        private final int pagesProcessed;

        Status(Map<String, Integer> readersBuilt, int pagesProcessed) {
            this.readersBuilt = readersBuilt;
            this.pagesProcessed = pagesProcessed;
        }

        Status(StreamInput in) throws IOException {
            readersBuilt = in.readOrderedMap(StreamInput::readString, StreamInput::readVInt);
            pagesProcessed = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(readersBuilt, StreamOutput::writeString, StreamOutput::writeVInt);
            out.writeVInt(pagesProcessed);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public Map<String, Integer> readersBuilt() {
            return readersBuilt;
        }

        public int pagesProcessed() {
            return pagesProcessed;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject("readers_built");
            for (Map.Entry<String, Integer> e : readersBuilt.entrySet()) {
                builder.field(e.getKey(), e.getValue());
            }
            builder.endObject();
            builder.field("pages_processed", pagesProcessed);
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return pagesProcessed == status.pagesProcessed && readersBuilt.equals(status.readersBuilt);
        }

        @Override
        public int hashCode() {
            return Objects.hash(readersBuilt, pagesProcessed);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
