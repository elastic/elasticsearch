/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class Segment implements Writeable {

    private String name;
    private long generation;
    public boolean committed;
    public boolean search;
    public long sizeInBytes = -1;
    public int docCount = -1;
    public int delDocCount = -1;
    public org.apache.lucene.util.Version version = null;
    public Boolean compound = null;
    public String mergeId;
    public Sort segmentSort;
    public Map<String, String> attributes;

    public Segment(StreamInput in) throws IOException {
        name = in.readString();
        generation = Long.parseLong(name.substring(1), Character.MAX_RADIX);
        committed = in.readBoolean();
        search = in.readBoolean();
        docCount = in.readInt();
        delDocCount = in.readInt();
        sizeInBytes = in.readLong();
        version = Lucene.parseVersionLenient(in.readOptionalString(), null);
        compound = in.readOptionalBoolean();
        mergeId = in.readOptionalString();
        if (in.getTransportVersion().before(TransportVersion.V_8_0_0)) {
            in.readLong(); // memoryInBytes
        }
        if (in.readBoolean()) {
            readRamTree(in);
        }
        segmentSort = readSegmentSort(in);
        if (in.readBoolean()) {
            attributes = in.readMap(StreamInput::readString);
        } else {
            attributes = null;
        }
    }

    public Segment(String name) {
        this.name = name;
        this.generation = Long.parseLong(name.substring(1), Character.MAX_RADIX);
    }

    public String getName() {
        return this.name;
    }

    public long getGeneration() {
        return this.generation;
    }

    public boolean isCommitted() {
        return this.committed;
    }

    public boolean isSearch() {
        return this.search;
    }

    public int getNumDocs() {
        return this.docCount;
    }

    public int getDeletedDocs() {
        return this.delDocCount;
    }

    public ByteSizeValue getSize() {
        return ByteSizeValue.ofBytes(sizeInBytes);
    }

    public org.apache.lucene.util.Version getVersion() {
        return version;
    }

    @Nullable
    public Boolean isCompound() {
        return compound;
    }

    /**
     * If set, a string representing that the segment is part of a merge, with the value representing the
     * group of segments that represent this merge.
     */
    @Nullable
    public String getMergeId() {
        return this.mergeId;
    }

    /**
     * Return the sort order of this segment, or null if the segment has no sort.
     */
    public Sort getSegmentSort() {
        return segmentSort;
    }

    /**
     * Return segment attributes.
     * @see org.apache.lucene.index.SegmentInfo#getAttributes()
     */
    public Map<String, String> getAttributes() {
        return attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Segment segment = (Segment) o;

        return Objects.equals(name, segment.name);

    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeBoolean(committed);
        out.writeBoolean(search);
        out.writeInt(docCount);
        out.writeInt(delDocCount);
        out.writeLong(sizeInBytes);
        out.writeOptionalString(version.toString());
        out.writeOptionalBoolean(compound);
        out.writeOptionalString(mergeId);
        if (out.getTransportVersion().before(TransportVersion.V_8_0_0)) {
            out.writeLong(0); // memoryInBytes
        }

        out.writeBoolean(false);
        writeSegmentSort(out, segmentSort);
        boolean hasAttributes = attributes != null;
        out.writeBoolean(hasAttributes);
        if (hasAttributes) {
            out.writeMap(attributes, StreamOutput::writeString, StreamOutput::writeString);
        }
    }

    private static final byte SORT_STRING_SET = 0;
    private static final byte SORT_INT = 1;
    private static final byte SORT_FLOAT = 2;
    private static final byte SORT_DOUBLE = 3;
    private static final byte SORT_LONG = 4;
    private static final byte SORT_STRING_SINGLE = 5;

    private static Sort readSegmentSort(StreamInput in) throws IOException {
        int size = in.readVInt();
        if (size == 0) {
            return null;
        }
        SortField[] fields = new SortField[size];
        for (int i = 0; i < size; i++) {
            String field = in.readString();
            byte type = in.readByte();
            if (type == SORT_STRING_SET) {
                Boolean missingFirst = in.readOptionalBoolean();
                boolean max = in.readBoolean();
                boolean reverse = in.readBoolean();
                fields[i] = new SortedSetSortField(field, reverse, max ? SortedSetSelector.Type.MAX : SortedSetSelector.Type.MIN);
                if (missingFirst != null) {
                    fields[i].setMissingValue(missingFirst ? SortedSetSortField.STRING_FIRST : SortedSetSortField.STRING_LAST);
                }
            } else if (type == SORT_STRING_SINGLE) {
                Boolean missingFirst = in.readOptionalBoolean();
                boolean reverse = in.readBoolean();
                fields[i] = new SortField(field, SortField.Type.STRING, reverse);
                if (missingFirst != null) {
                    fields[i].setMissingValue(missingFirst ? SortedSetSortField.STRING_FIRST : SortedSetSortField.STRING_LAST);
                }
            } else {
                Object missing = in.readGenericValue();
                boolean max = in.readBoolean();
                boolean reverse = in.readBoolean();
                final SortField.Type numericType = switch (type) {
                    case SORT_INT -> SortField.Type.INT;
                    case SORT_FLOAT -> SortField.Type.FLOAT;
                    case SORT_DOUBLE -> SortField.Type.DOUBLE;
                    case SORT_LONG -> SortField.Type.LONG;
                    default -> throw new IOException("invalid index sort type:[" + type + "] for numeric field:[" + field + "]");
                };
                fields[i] = new SortedNumericSortField(
                    field,
                    numericType,
                    reverse,
                    max ? SortedNumericSelector.Type.MAX : SortedNumericSelector.Type.MIN
                );
                if (missing != null) {
                    fields[i].setMissingValue(missing);
                }
            }
        }
        return new Sort(fields);
    }

    private static void writeSegmentSort(StreamOutput out, Sort sort) throws IOException {
        if (sort == null) {
            out.writeVInt(0);
            return;
        }
        out.writeArray((o, field) -> {
            o.writeString(field.getField());
            if (field instanceof SortedSetSortField) {
                o.writeByte(SORT_STRING_SET);
                o.writeOptionalBoolean(field.getMissingValue() == null ? null : field.getMissingValue() == SortField.STRING_FIRST);
                o.writeBoolean(((SortedSetSortField) field).getSelector() == SortedSetSelector.Type.MAX);
                o.writeBoolean(field.getReverse());
            } else if (field instanceof SortedNumericSortField) {
                switch (((SortedNumericSortField) field).getNumericType()) {
                    case INT -> o.writeByte(SORT_INT);
                    case FLOAT -> o.writeByte(SORT_FLOAT);
                    case DOUBLE -> o.writeByte(SORT_DOUBLE);
                    case LONG -> o.writeByte(SORT_LONG);
                    default -> throw new IOException("invalid index sort field:" + field);
                }
                o.writeGenericValue(field.getMissingValue());
                o.writeBoolean(((SortedNumericSortField) field).getSelector() == SortedNumericSelector.Type.MAX);
                o.writeBoolean(field.getReverse());
            } else if (field.getType().equals(SortField.Type.STRING)) {
                if (o.getTransportVersion().before(TransportVersion.V_8_5_0)) {
                    // The closest supported version before 8.5.0 was SortedSet fields, so we mimic that
                    o.writeByte(SORT_STRING_SET);
                    o.writeOptionalBoolean(field.getMissingValue() == null ? null : field.getMissingValue() == SortField.STRING_FIRST);
                    o.writeBoolean(true);
                    o.writeBoolean(field.getReverse());
                } else {
                    o.writeByte(SORT_STRING_SINGLE);
                    o.writeOptionalBoolean(field.getMissingValue() == null ? null : field.getMissingValue() == SortField.STRING_FIRST);
                    o.writeBoolean(field.getReverse());
                }
            } else {
                throw new IOException("invalid index sort field:" + field);
            }
        }, sort.getSort());
    }

    private static void readRamTree(StreamInput in) throws IOException {
        in.readString();
        in.readVLong();
        int numChildren = in.readVInt();
        for (int i = 0; i < numChildren; i++) {
            readRamTree(in);
        }
    }

    @Override
    public String toString() {
        return "Segment{"
            + "name='"
            + name
            + '\''
            + ", generation="
            + generation
            + ", committed="
            + committed
            + ", search="
            + search
            + ", sizeInBytes="
            + sizeInBytes
            + ", docCount="
            + docCount
            + ", delDocCount="
            + delDocCount
            + ", version='"
            + version
            + '\''
            + ", compound="
            + compound
            + ", mergeId='"
            + mergeId
            + '\''
            + (segmentSort != null ? ", sort=" + segmentSort : "")
            + ", attributes="
            + attributes
            + '}';
    }
}
