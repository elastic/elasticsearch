/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class FieldCapabilitiesIndexResponse extends ActionResponse implements Writeable {
    private final String indexName;
    private final Collection<IndexFieldCapabilities> fields;
    private final boolean canMatch;
    private final transient Version originVersion;

    FieldCapabilitiesIndexResponse(String indexName, Collection<IndexFieldCapabilities> fields, boolean canMatch) {
        this.indexName = indexName;
        this.fields = fields;
        this.canMatch = canMatch;
        this.originVersion = Version.CURRENT;
    }

    FieldCapabilitiesIndexResponse(StreamInput in, IndexFieldCapabilities.Deduplicator fieldDeduplicator) throws IOException {
        super(in);
        this.indexName = in.readString();
        this.fields = readFields(in, fieldDeduplicator);
        this.canMatch = in.getVersion().onOrAfter(Version.V_7_9_0) ? in.readBoolean() : true;
        this.originVersion = in.getVersion();
    }

    /**
     * Get the index name
     */
    public String getIndexName() {
        return indexName;
    }

    public boolean canMatch() {
        return canMatch;
    }

    /**
     * Get the field capabilities
     */
    public Collection<IndexFieldCapabilities> getFields() {
        return fields;
    }

    private static Collection<IndexFieldCapabilities> readFields(StreamInput in, IndexFieldCapabilities.Deduplicator fieldDeduplicator)
        throws IOException {
        // Previously, we serialize fields as a map from field name to field-caps
        final int size = in.readVInt();
        if (size == 0) {
            return Collections.emptyList();
        }
        final List<IndexFieldCapabilities> fields = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            final String fieldName = in.readString(); // the fieldName will be discarded - it's used in assertions only
            final IndexFieldCapabilities fieldCaps = new IndexFieldCapabilities(in);
            assert fieldName.equals(fieldCaps.getName()) : fieldName + " != " + fieldCaps.getName();
            fields.add(fieldDeduplicator.deduplicate(fieldCaps));
        }
        return fields;
    }

    private static void writeFields(StreamOutput out, Collection<IndexFieldCapabilities> fields) throws IOException {
        // Previously, we serialize fields as a map from field name to field-caps
        out.writeVInt(fields.size());
        for (IndexFieldCapabilities field : fields) {
            out.writeString(field.getName());
            field.writeTo(out);
        }
    }

    Version getOriginVersion() {
        return originVersion;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
        writeFields(out, fields);
        if (out.getVersion().onOrAfter(Version.V_7_9_0)) {
            out.writeBoolean(canMatch);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldCapabilitiesIndexResponse that = (FieldCapabilitiesIndexResponse) o;
        return canMatch == that.canMatch && Objects.equals(indexName, that.indexName) && Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, fields, canMatch);
    }
}
