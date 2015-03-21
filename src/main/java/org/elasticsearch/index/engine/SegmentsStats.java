/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

public class SegmentsStats implements Streamable, ToXContent {

    private long count;
    private long memoryInBytes;
    private long termsMemoryInBytes;
    private long storedFieldsMemoryInBytes;
    private long termVectorsMemoryInBytes;
    private long normsMemoryInBytes;
    private long docValuesMemoryInBytes;
    private long indexWriterMemoryInBytes;
    private long indexWriterMaxMemoryInBytes;
    private long versionMapMemoryInBytes;
    private long bitsetMemoryInBytes;

    public SegmentsStats() {}

    public void add(long count, long memoryInBytes) {
        this.count += count;
        this.memoryInBytes += memoryInBytes;
    }
    
    public void addTermsMemoryInBytes(long termsMemoryInBytes) {
        this.termsMemoryInBytes += termsMemoryInBytes;
    }

    public void addStoredFieldsMemoryInBytes(long storedFieldsMemoryInBytes) {
        this.storedFieldsMemoryInBytes += storedFieldsMemoryInBytes;
    }

    public void addTermVectorsMemoryInBytes(long termVectorsMemoryInBytes) {
        this.termVectorsMemoryInBytes += termVectorsMemoryInBytes;
    }

    public void addNormsMemoryInBytes(long normsMemoryInBytes) {
        this.normsMemoryInBytes += normsMemoryInBytes;
    }

    public void addDocValuesMemoryInBytes(long docValuesMemoryInBytes) {
        this.docValuesMemoryInBytes += docValuesMemoryInBytes;
    }

    public void addIndexWriterMemoryInBytes(long indexWriterMemoryInBytes) {
        this.indexWriterMemoryInBytes += indexWriterMemoryInBytes;
    }

    public void addIndexWriterMaxMemoryInBytes(long indexWriterMaxMemoryInBytes) {
        this.indexWriterMaxMemoryInBytes += indexWriterMaxMemoryInBytes;
    }

    public void addVersionMapMemoryInBytes(long versionMapMemoryInBytes) {
        this.versionMapMemoryInBytes += versionMapMemoryInBytes;
    }

    public void addBitsetMemoryInBytes(long bitsetMemoryInBytes) {
        this.bitsetMemoryInBytes += bitsetMemoryInBytes;
    }

    public void add(SegmentsStats mergeStats) {
        if (mergeStats == null) {
            return;
        }
        add(mergeStats.count, mergeStats.memoryInBytes);
        addTermsMemoryInBytes(mergeStats.termsMemoryInBytes);
        addStoredFieldsMemoryInBytes(mergeStats.storedFieldsMemoryInBytes);
        addTermVectorsMemoryInBytes(mergeStats.termVectorsMemoryInBytes);
        addNormsMemoryInBytes(mergeStats.normsMemoryInBytes);
        addDocValuesMemoryInBytes(mergeStats.docValuesMemoryInBytes);
        addIndexWriterMemoryInBytes(mergeStats.indexWriterMemoryInBytes);
        addIndexWriterMaxMemoryInBytes(mergeStats.indexWriterMaxMemoryInBytes);
        addVersionMapMemoryInBytes(mergeStats.versionMapMemoryInBytes);
        addBitsetMemoryInBytes(mergeStats.bitsetMemoryInBytes);
    }

    /**
     * The number of segments.
     */
    public long getCount() {
        return this.count;
    }

    /**
     * Estimation of the memory usage used by a segment.
     */
    public long getMemoryInBytes() {
        return this.memoryInBytes;
    }

    public ByteSizeValue getMemory() {
        return new ByteSizeValue(memoryInBytes);
    }

    /**
     * Estimation of the terms dictionary memory usage by a segment.
     */
    public long getTermsMemoryInBytes() {
        return this.termsMemoryInBytes;
    }

    public ByteSizeValue getTermsMemory() {
        return new ByteSizeValue(termsMemoryInBytes);
    }

    /**
     * Estimation of the stored fields memory usage by a segment.
     */
    public long getStoredFieldsMemoryInBytes() {
        return this.storedFieldsMemoryInBytes;
    }

    public ByteSizeValue getStoredFieldsMemory() {
        return new ByteSizeValue(storedFieldsMemoryInBytes);
    }

    /**
     * Estimation of the term vectors memory usage by a segment.
     */
    public long getTermVectorsMemoryInBytes() {
        return this.termVectorsMemoryInBytes;
    }

    public ByteSizeValue getTermVectorsMemory() {
        return new ByteSizeValue(termVectorsMemoryInBytes);
    }

    /**
     * Estimation of the norms memory usage by a segment.
     */
    public long getNormsMemoryInBytes() {
        return this.normsMemoryInBytes;
    }

    public ByteSizeValue getNormsMemory() {
        return new ByteSizeValue(normsMemoryInBytes);
    }

    /**
     * Estimation of the doc values memory usage by a segment.
     */
    public long getDocValuesMemoryInBytes() {
        return this.docValuesMemoryInBytes;
    }

    public ByteSizeValue getDocValuesMemory() {
        return new ByteSizeValue(docValuesMemoryInBytes);
    }

    /**
     * Estimation of the memory usage by index writer
     */
    public long getIndexWriterMemoryInBytes() {
        return this.indexWriterMemoryInBytes;
    }

    public ByteSizeValue getIndexWriterMemory() {
        return new ByteSizeValue(indexWriterMemoryInBytes);
    }

    /**
     * Maximum memory index writer may use before it must write buffered documents to a new segment.
     */
    public long getIndexWriterMaxMemoryInBytes() {
        return this.indexWriterMaxMemoryInBytes;
    }

    public ByteSizeValue getIndexWriterMaxMemory() {
        return new ByteSizeValue(indexWriterMaxMemoryInBytes);
    }

    /**
     * Estimation of the memory usage by version map
     */
    public long getVersionMapMemoryInBytes() {
        return this.versionMapMemoryInBytes;
    }

    public ByteSizeValue getVersionMapMemory() {
        return new ByteSizeValue(versionMapMemoryInBytes);
    }

    /**
     * Estimation of how much the cached bit sets are taking. (which nested and p/c rely on)
     */
    public long getBitsetMemoryInBytes() {
        return bitsetMemoryInBytes;
    }

    public ByteSizeValue getBitsetMemory() {
        return new ByteSizeValue(bitsetMemoryInBytes);
    }

    public static SegmentsStats readSegmentsStats(StreamInput in) throws IOException {
        SegmentsStats stats = new SegmentsStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SEGMENTS);
        builder.field(Fields.COUNT, count);
        builder.byteSizeField(Fields.MEMORY_IN_BYTES, Fields.MEMORY, memoryInBytes);
        builder.byteSizeField(Fields.TERMS_MEMORY_IN_BYTES, Fields.TERMS_MEMORY, termsMemoryInBytes);
        builder.byteSizeField(Fields.STORED_FIELDS_MEMORY_IN_BYTES, Fields.STORED_FIELDS_MEMORY, storedFieldsMemoryInBytes);
        builder.byteSizeField(Fields.TERM_VECTORS_MEMORY_IN_BYTES, Fields.TERM_VECTORS_MEMORY, termVectorsMemoryInBytes);
        builder.byteSizeField(Fields.NORMS_MEMORY_IN_BYTES, Fields.NORMS_MEMORY, normsMemoryInBytes);
        builder.byteSizeField(Fields.DOC_VALUES_MEMORY_IN_BYTES, Fields.DOC_VALUES_MEMORY, docValuesMemoryInBytes);
        builder.byteSizeField(Fields.INDEX_WRITER_MEMORY_IN_BYTES, Fields.INDEX_WRITER_MEMORY, indexWriterMemoryInBytes);
        builder.byteSizeField(Fields.INDEX_WRITER_MAX_MEMORY_IN_BYTES, Fields.INDEX_WRITER_MAX_MEMORY, indexWriterMaxMemoryInBytes);
        builder.byteSizeField(Fields.VERSION_MAP_MEMORY_IN_BYTES, Fields.VERSION_MAP_MEMORY, versionMapMemoryInBytes);
        builder.byteSizeField(Fields.FIXED_BIT_SET_MEMORY_IN_BYTES, Fields.FIXED_BIT_SET, bitsetMemoryInBytes);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString SEGMENTS = new XContentBuilderString("segments");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
        static final XContentBuilderString MEMORY = new XContentBuilderString("memory");
        static final XContentBuilderString MEMORY_IN_BYTES = new XContentBuilderString("memory_in_bytes");
        static final XContentBuilderString TERMS_MEMORY = new XContentBuilderString("terms_memory");
        static final XContentBuilderString TERMS_MEMORY_IN_BYTES = new XContentBuilderString("terms_memory_in_bytes");
        static final XContentBuilderString STORED_FIELDS_MEMORY = new XContentBuilderString("stored_fields_memory");
        static final XContentBuilderString STORED_FIELDS_MEMORY_IN_BYTES = new XContentBuilderString("stored_fields_memory_in_bytes");
        static final XContentBuilderString TERM_VECTORS_MEMORY = new XContentBuilderString("term_vectors_memory");
        static final XContentBuilderString TERM_VECTORS_MEMORY_IN_BYTES = new XContentBuilderString("term_vectors_memory_in_bytes");
        static final XContentBuilderString NORMS_MEMORY = new XContentBuilderString("norms_memory");
        static final XContentBuilderString NORMS_MEMORY_IN_BYTES = new XContentBuilderString("norms_memory_in_bytes");
        static final XContentBuilderString DOC_VALUES_MEMORY = new XContentBuilderString("doc_values_memory");
        static final XContentBuilderString DOC_VALUES_MEMORY_IN_BYTES = new XContentBuilderString("doc_values_memory_in_bytes");
        static final XContentBuilderString INDEX_WRITER_MEMORY = new XContentBuilderString("index_writer_memory");
        static final XContentBuilderString INDEX_WRITER_MEMORY_IN_BYTES = new XContentBuilderString("index_writer_memory_in_bytes");
        static final XContentBuilderString INDEX_WRITER_MAX_MEMORY = new XContentBuilderString("index_writer_max_memory");
        static final XContentBuilderString INDEX_WRITER_MAX_MEMORY_IN_BYTES = new XContentBuilderString("index_writer_max_memory_in_bytes");
        static final XContentBuilderString VERSION_MAP_MEMORY = new XContentBuilderString("version_map_memory");
        static final XContentBuilderString VERSION_MAP_MEMORY_IN_BYTES = new XContentBuilderString("version_map_memory_in_bytes");
        static final XContentBuilderString FIXED_BIT_SET = new XContentBuilderString("fixed_bit_set");
        static final XContentBuilderString FIXED_BIT_SET_MEMORY_IN_BYTES = new XContentBuilderString("fixed_bit_set_memory_in_bytes");
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        count = in.readVLong();
        memoryInBytes = in.readLong();
        termsMemoryInBytes = in.readLong();
        storedFieldsMemoryInBytes = in.readLong();
        termVectorsMemoryInBytes = in.readLong();
        normsMemoryInBytes = in.readLong();
        docValuesMemoryInBytes = in.readLong();
        indexWriterMemoryInBytes = in.readLong();
        versionMapMemoryInBytes = in.readLong();
        indexWriterMaxMemoryInBytes = in.readLong();
        bitsetMemoryInBytes = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
        out.writeLong(memoryInBytes);
        out.writeLong(termsMemoryInBytes);
        out.writeLong(storedFieldsMemoryInBytes);
        out.writeLong(termVectorsMemoryInBytes);
        out.writeLong(normsMemoryInBytes);
        out.writeLong(docValuesMemoryInBytes);
        out.writeLong(indexWriterMemoryInBytes);
        out.writeLong(versionMapMemoryInBytes);
        out.writeLong(indexWriterMaxMemoryInBytes);
        out.writeLong(bitsetMemoryInBytes);
    }
}
