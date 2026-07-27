/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.AbstractPageMappingToIteratorOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class ValuesSourceReaderOperatorStatus extends AbstractPageMappingToIteratorOperator.Status {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Operator.Status.class,
        "values_source_reader",
        ValuesSourceReaderOperatorStatus::readFrom
    );
    private static final TransportVersion CONVERTERS_USED = TransportVersion.fromName("esql_vsr_converters_used");

    private static final TransportVersion ESQL_DOCUMENTS_FOUND_AND_VALUES_LOADED = TransportVersion.fromName(
        "esql_documents_found_and_values_loaded"
    );
    private static final TransportVersion ESQL_SPLIT_ON_BIG_VALUES = TransportVersion.fromName("esql_split_on_big_values");
    private static final TransportVersion ESQL_LUCENE_OPERATOR_BYTES_READ = TransportVersion.fromName("esql_lucene_operator_bytes_read");
    private static final TransportVersion ESQL_VSR_SOURCE_LOAD_PROFILE = TransportVersion.fromName("esql_vsr_source_load_profile");

    private final Map<String, Integer> readersBuilt;
    private final Map<String, Integer> convertersUsed;
    private final long valuesLoaded;
    private final long bytesRead;
    /** Number of documents where at least one source-backed field was read. */
    private final long sourceDocsLoaded;
    /** Number of source-backed field read attempts (roughly docs x source-backed fields). */
    private final long sourceFieldReads;
    /** Sum of last materialized source payload size per row-stride document read. */
    private final long sourceBytesLoaded;

    public ValuesSourceReaderOperatorStatus(
        Map<String, Integer> readersBuilt,
        Map<String, Integer> convertersUsed,
        long processNanos,
        int pagesReceived,
        int pagesEmitted,
        long rowsReceived,
        long rowsEmitted,
        long valuesLoaded,
        long bytesRead,
        long sourceDocsLoaded,
        long sourceFieldReads,
        long sourceBytesLoaded
    ) {
        super(processNanos, pagesReceived, pagesEmitted, rowsReceived, rowsEmitted);
        this.readersBuilt = readersBuilt;
        this.convertersUsed = convertersUsed;
        this.valuesLoaded = valuesLoaded;
        this.bytesRead = bytesRead;
        this.sourceDocsLoaded = sourceDocsLoaded;
        this.sourceFieldReads = sourceFieldReads;
        this.sourceBytesLoaded = sourceBytesLoaded;
    }

    static ValuesSourceReaderOperatorStatus readFrom(StreamInput in) throws IOException {
        long processNanos;
        int pagesReceived;
        int pagesEmitted;
        long rowsReceived;
        long rowsEmitted;
        if (supportsSplitOnBigValues(in.getTransportVersion())) {
            AbstractPageMappingToIteratorOperator.Status status = new AbstractPageMappingToIteratorOperator.Status(in);
            processNanos = status.processNanos();
            pagesReceived = status.pagesReceived();
            pagesEmitted = status.pagesEmitted();
            rowsReceived = status.rowsReceived();
            rowsEmitted = status.rowsEmitted();
        } else {
            AbstractPageMappingOperator.Status status = new AbstractPageMappingOperator.Status(in);
            processNanos = status.processNanos();
            pagesReceived = status.pagesProcessed();
            pagesEmitted = status.pagesProcessed();
            rowsReceived = status.rowsReceived();
            rowsEmitted = status.rowsEmitted();
        }
        Map<String, Integer> readersBuilt = in.readOrderedMap(StreamInput::readString, StreamInput::readVInt);
        Map<String, Integer> convertersUsed = in.getTransportVersion().supports(CONVERTERS_USED)
            ? in.readOrderedMap(StreamInput::readString, StreamInput::readVInt)
            : Map.of();
        long valuesLoaded = supportsValuesLoaded(in.getTransportVersion()) ? in.readVLong() : 0;
        long bytesRead = supportsBytesRead(in.getTransportVersion()) ? in.readVLong() : 0;
        long sourceDocsLoaded = supportsSourceLoadProfile(in.getTransportVersion()) ? in.readVLong() : 0;
        long sourceFieldReads = supportsSourceLoadProfile(in.getTransportVersion()) ? in.readVLong() : 0;
        long sourceBytesLoaded = supportsSourceLoadProfile(in.getTransportVersion()) ? in.readVLong() : 0;
        return new ValuesSourceReaderOperatorStatus(
            readersBuilt,
            convertersUsed,
            processNanos,
            pagesReceived,
            pagesEmitted,
            rowsReceived,
            rowsEmitted,
            valuesLoaded,
            bytesRead,
            sourceDocsLoaded,
            sourceFieldReads,
            sourceBytesLoaded
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (supportsSplitOnBigValues(out.getTransportVersion())) {
            super.writeTo(out);
        } else {
            /*
             * Before we knew how to split pages when reading large values
             * our status just contained one int per page - just like AbstractPageMappingOperator.Status.
             */
            new AbstractPageMappingOperator.Status(processNanos(), pagesEmitted(), rowsReceived(), rowsEmitted()).writeTo(out);
        }
        out.writeMap(readersBuilt, StreamOutput::writeVInt);
        if (out.getTransportVersion().supports(CONVERTERS_USED)) {
            out.writeMap(convertersUsed, StreamOutput::writeVInt);
        }
        if (supportsValuesLoaded(out.getTransportVersion())) {
            out.writeVLong(valuesLoaded);
        }
        if (supportsBytesRead(out.getTransportVersion())) {
            out.writeVLong(bytesRead);
        }
        if (supportsSourceLoadProfile(out.getTransportVersion())) {
            out.writeVLong(sourceDocsLoaded);
            out.writeVLong(sourceFieldReads);
            out.writeVLong(sourceBytesLoaded);
        }
    }

    private static boolean supportsBytesRead(TransportVersion version) {
        return version.supports(ESQL_LUCENE_OPERATOR_BYTES_READ);
    }

    private static boolean supportsSplitOnBigValues(TransportVersion version) {
        return version.supports(ESQL_SPLIT_ON_BIG_VALUES);
    }

    private static boolean supportsValuesLoaded(TransportVersion version) {
        return version.supports(ESQL_DOCUMENTS_FOUND_AND_VALUES_LOADED);
    }

    private static boolean supportsSourceLoadProfile(TransportVersion version) {
        return version.supports(ESQL_VSR_SOURCE_LOAD_PROFILE);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Map<String, Integer> readersBuilt() {
        return readersBuilt;
    }

    public Map<String, Integer> convertersUsed() {
        return convertersUsed;
    }

    @Override
    public long valuesLoaded() {
        return valuesLoaded;
    }

    @Override
    public long bytesRead() {
        return bytesRead;
    }

    public long sourceDocsLoaded() {
        return sourceDocsLoaded;
    }

    public long sourceFieldReads() {
        return sourceFieldReads;
    }

    public long sourceBytesLoaded() {
        return sourceBytesLoaded;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject("readers_built");
        for (Map.Entry<String, Integer> e : readersBuilt.entrySet()) {
            builder.field(e.getKey(), e.getValue());
        }
        builder.endObject();
        if (convertersUsed.isEmpty() == false) {
            builder.startObject("converters_used");
            for (Map.Entry<String, Integer> e : convertersUsed.entrySet()) {
                builder.field(e.getKey(), e.getValue());
            }
            builder.endObject();
        }
        builder.field("values_loaded", valuesLoaded);
        builder.field("bytes_read", bytesRead);
        // Breadth: how many docs needed source-backed extraction at least once.
        builder.field("source_docs_loaded", sourceDocsLoaded);
        // Frequency: how often source-backed fields were read across docs.
        builder.field("source_field_reads", sourceFieldReads);
        // Volume: cumulative source payload bytes materialized while reading.
        builder.field("source_bytes_loaded", sourceBytesLoaded);
        innerToXContent(builder);
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o) == false) return false;
        ValuesSourceReaderOperatorStatus status = (ValuesSourceReaderOperatorStatus) o;
        return readersBuilt.equals(status.readersBuilt)
            && convertersUsed.equals(status.convertersUsed)
            && valuesLoaded == status.valuesLoaded
            && bytesRead == status.bytesRead
            && sourceDocsLoaded == status.sourceDocsLoaded
            && sourceFieldReads == status.sourceFieldReads
            && sourceBytesLoaded == status.sourceBytesLoaded;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), readersBuilt, valuesLoaded, bytesRead, sourceDocsLoaded, sourceFieldReads, sourceBytesLoaded);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
