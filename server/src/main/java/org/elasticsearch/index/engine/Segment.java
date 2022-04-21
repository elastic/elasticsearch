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
import org.elasticsearch.Version;
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
        if (in.getVersion().before(Version.V_8_0_0)) {
            in.readLong(); // memoryInBytes
        }
        if (in.readBoolean()) {
            readRamTree(in);
        }
        segmentSort = readSegmentSort(in);
        if (in.readBoolean()) {
            attributes = in.readMap(StreamInput::readString, StreamInput::readString);
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
        return new ByteSizeValue(sizeInBytes);
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
        if (out.getVersion().before(Version.V_8_0_0)) {
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

    private static Sort readSegmentSort(StreamInput in) throws IOException {
        int size = in.readVInt();
        if (size == 0) {
            return null;
        }
        SortField[] fields = new SortField[size];
        for (int i = 0; i < size; i++) {
            String field = in.readString();
            byte type = in.readByte();
            if (type == 0) {
                Boolean missingFirst = in.readOptionalBoolean();
                boolean max = in.readBoolean();
                boolean reverse = in.readBoolean();
                fields[i] = new SortedSetSortField(field, reverse, max ? SortedSetSelector.Type.MAX : SortedSetSelector.Type.MIN);
                if (missingFirst != null) {
                    fields[i].setMissingValue(missingFirst ? SortedSetSortField.STRING_FIRST : SortedSetSortField.STRING_LAST);
                }
            } else {
                Object missing = in.readGenericValue();
                boolean max = in.readBoolean();
                boolean reverse = in.readBoolean();
                final SortField.Type numericType = switch (type) {
                    case 1 -> SortField.Type.INT;
                    case 2 -> SortField.Type.FLOAT;
                    case 3 -> SortField.Type.DOUBLE;
                    case 4 -> SortField.Type.LONG;
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
        out.writeVInt(sort.getSort().length);
        for (SortField field : sort.getSort()) {
            out.writeString(field.getField());
            if (field instanceof SortedSetSortField) {
                out.writeByte((byte) 0);
                out.writeOptionalBoolean(field.getMissingValue() == null ? null : field.getMissingValue() == SortField.STRING_FIRST);
                out.writeBoolean(((SortedSetSortField) field).getSelector() == SortedSetSelector.Type.MAX);
                out.writeBoolean(field.getReverse());
            } else if (field instanceof SortedNumericSortField) {
                switch (((SortedNumericSortField) field).getNumericType()) {
                    case INT -> out.writeByte((byte) 1);
                    case FLOAT -> out.writeByte((byte) 2);
                    case DOUBLE -> out.writeByte((byte) 3);
                    case LONG -> out.writeByte((byte) 4);
                    default -> throw new IOException("invalid index sort field:" + field);
                }
                out.writeGenericValue(field.getMissingValue());
                out.writeBoolean(((SortedNumericSortField) field).getSelector() == SortedNumericSelector.Type.MAX);
                out.writeBoolean(field.getReverse());
            } else {
                throw new IOException("invalid index sort field:" + field);
            }
        }
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
