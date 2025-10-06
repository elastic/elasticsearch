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

    private static final TransportVersion ESQL_DOCUMENTS_FOUND_AND_VALUES_LOADED = TransportVersion.fromName(
        "esql_documents_found_and_values_loaded"
    );
    private static final TransportVersion ESQL_SPLIT_ON_BIG_VALUES = TransportVersion.fromName("esql_split_on_big_values");

    private final Map<String, Integer> readersBuilt;
    private final long valuesLoaded;

    public ValuesSourceReaderOperatorStatus(
        Map<String, Integer> readersBuilt,
        long processNanos,
        int pagesReceived,
        int pagesEmitted,
        long rowsReceived,
        long rowsEmitted,
        long valuesLoaded
    ) {
        super(processNanos, pagesReceived, pagesEmitted, rowsReceived, rowsEmitted);
        this.readersBuilt = readersBuilt;
        this.valuesLoaded = valuesLoaded;
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
        long valuesLoaded = supportsValuesLoaded(in.getTransportVersion()) ? in.readVLong() : 0;
        return new ValuesSourceReaderOperatorStatus(
            readersBuilt,
            processNanos,
            pagesReceived,
            pagesEmitted,
            rowsReceived,
            rowsEmitted,
            valuesLoaded
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
        if (supportsValuesLoaded(out.getTransportVersion())) {
            out.writeVLong(valuesLoaded);
        }
    }

    private static boolean supportsSplitOnBigValues(TransportVersion version) {
        return version.supports(ESQL_SPLIT_ON_BIG_VALUES);
    }

    private static boolean supportsValuesLoaded(TransportVersion version) {
        return version.supports(ESQL_DOCUMENTS_FOUND_AND_VALUES_LOADED);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Map<String, Integer> readersBuilt() {
        return readersBuilt;
    }

    @Override
    public long valuesLoaded() {
        return valuesLoaded;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject("readers_built");
        for (Map.Entry<String, Integer> e : readersBuilt.entrySet()) {
            builder.field(e.getKey(), e.getValue());
        }
        builder.endObject();
        builder.field("values_loaded", valuesLoaded);
        innerToXContent(builder);
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o) == false) return false;
        ValuesSourceReaderOperatorStatus status = (ValuesSourceReaderOperatorStatus) o;
        return readersBuilt.equals(status.readersBuilt) && valuesLoaded == status.valuesLoaded;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), readersBuilt, valuesLoaded);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
