/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.ObjectIntMap;
import com.carrotsearch.hppc.cursors.ObjectIntCursor;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.FilterStreamInput;
import org.elasticsearch.common.io.stream.FilterStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Response for shard level operation in {@link TransportFieldCapabilitiesAction}.
 */
public class FieldCapabilitiesIndexResponse extends ActionResponse implements Writeable {
    private final String indexName;
    private final Map<String, IndexFieldCapabilities> responseMap;
    private final boolean canMatch;
    private final transient Version originVersion;

    FieldCapabilitiesIndexResponse(String indexName, Map<String, IndexFieldCapabilities> responseMap, boolean canMatch) {
        this.indexName = indexName;
        this.responseMap = responseMap;
        this.canMatch = canMatch;
        this.originVersion = Version.CURRENT;
    }

    FieldCapabilitiesIndexResponse(StreamInput in) throws IOException {
        super(in);
        this.indexName = in.readString();
        this.responseMap = in.readMap(StreamInput::readString, IndexFieldCapabilities::new);
        this.canMatch = in.readBoolean();
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
     * Get the field capabilities map
     */
    public Map<String, IndexFieldCapabilities> get() {
        return responseMap;
    }

    /**
     *
     * Get the field capabilities for the provided {@code field}
     */
    public IndexFieldCapabilities getField(String field) {
        return responseMap.get(field);
    }

    Version getOriginVersion() {
        return originVersion;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
        out.writeMap(responseMap, StreamOutput::writeString, (valueOut, fc) -> fc.writeTo(valueOut));
        out.writeBoolean(canMatch);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldCapabilitiesIndexResponse that = (FieldCapabilitiesIndexResponse) o;
        return canMatch == that.canMatch &&
            Objects.equals(indexName, that.indexName) &&
            Objects.equals(responseMap, that.responseMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, responseMap, canMatch);
    }

    private static void addStringToDict(ObjectIntMap<String> dictionary, String s) {
        dictionary.putOrAdd(s, dictionary.size(), 0);
    }

    private static ObjectIntMap<String> collectStringLiterals(List<FieldCapabilitiesIndexResponse> responses) {
        final ObjectIntMap<String> dict = new ObjectIntHashMap<>();
        for (FieldCapabilitiesIndexResponse response : responses) {
            addStringToDict(dict, response.getIndexName());
            for (Map.Entry<String, IndexFieldCapabilities> fieldEntry : response.responseMap.entrySet()) {
                addStringToDict(dict, fieldEntry.getKey());
                final IndexFieldCapabilities fieldCap = fieldEntry.getValue();
                addStringToDict(dict, fieldCap.getName());
                addStringToDict(dict, fieldCap.getType());
                for (Map.Entry<String, String> e : fieldCap.meta().entrySet()) {
                    addStringToDict(dict, e.getKey());
                    addStringToDict(dict, e.getValue());
                }
            }
        }
        return dict;
    }

    static void writeResponses(StreamOutput output, List<FieldCapabilitiesIndexResponse> responses) throws IOException {
        if (output.getVersion().onOrAfter(Version.V_8_0_0)) {
            final boolean withOrdinals = responses.size() > 1;
            output.writeBoolean(withOrdinals);
            if (withOrdinals) {
                final ObjectIntMap<String> dictionary = collectStringLiterals(responses);
                final String[] inverseTable = new String[dictionary.size()];
                for (ObjectIntCursor<String> cursor : dictionary) {
                    inverseTable[cursor.value] = cursor.key;
                }
                output.writeStringArray(inverseTable);
                output = new StreamOutputWithOrdinals(output, dictionary);
            }
        }
        output.writeList(responses);
    }

    static List<FieldCapabilitiesIndexResponse> readResponses(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            final boolean withOrdinals = in.readBoolean();
            if (withOrdinals) {
                final String[] lookupTable = in.readStringArray();
                in = new StreamInputWithOrdinals(in, lookupTable);
            }
        }
        return in.readList(FieldCapabilitiesIndexResponse::new);
    }

    private static class StreamOutputWithOrdinals extends FilterStreamOutput {
        private final ObjectIntMap<String> dictionary;
        StreamOutputWithOrdinals(StreamOutput out, ObjectIntMap<String> dictionary) {
            super(out);
            this.dictionary = dictionary;
        }

        @Override
        public void writeString(String str) throws IOException {
            final int index = dictionary.getOrDefault(str, -1);
            if (index == -1) {
                assert false : "string value [" + str + " wasn't added to the dictionary";
                throw new IllegalStateException("String value [" + str + "] was added to the dictionary");
            }
            super.writeVInt(index);
        }
    }

    private static class StreamInputWithOrdinals extends FilterStreamInput {
        private final String[] lookupTable;

        StreamInputWithOrdinals(StreamInput in, String[] lookupTable) {
            super(in);
            this.lookupTable = lookupTable;
        }

        @Override
        public String readString() throws IOException {
            final int index = readVInt();
            if (index < 0 || index >= lookupTable.length) {
                assert false : "index " + index + " table length = " + lookupTable.length;
                throw new IndexOutOfBoundsException(index);
            }
            return lookupTable[index];
        }
    }
}
