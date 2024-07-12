/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.DynamicMap;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Definition of Script for DerivedField.
 * It will be used to execute scripts defined against derived fields of any type
 *
 * @opensearch.internal
 */
public abstract class DerivedFieldScript {

    public static final String[] PARAMETERS = {};
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("derived_field", Factory.class);
    private static final int MAX_BYTE_SIZE = 1024 * 1024; // Maximum allowed byte size (1 MB)

    private static final Map<String, Function<Object, Object>> PARAMS_FUNCTIONS = Map.of(
        "doc",
        value -> value,
        "_source",
        value -> ((SourceLookup) value).loadSourceIfNeeded()
    );

    /**
     * The generic runtime parameters for the script.
     */
    private final Map<String, Object> params;

    /**
     * A leaf lookup for the bound segment this script will operate on.
     */
    private final LeafSearchLookup leafLookup;

    /**
     * The field values emitted from the script.
     */
    private List<Object> emittedValues;

    private int totalByteSize;

    public DerivedFieldScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) {
        Map<String, Object> parameters = new HashMap<>(params);
        this.leafLookup = lookup.getLeafSearchLookup(leafContext);
        parameters.putAll(leafLookup.asMap());
        this.params = new DynamicMap(parameters, PARAMS_FUNCTIONS);
        this.emittedValues = new ArrayList<>();
        this.totalByteSize = 0;
    }

    /**
     * Return the parameters for this script.
     */
    public Map<String, Object> getParams() {
        return params;
    }

    /**
     * The doc lookup for the Lucene segment this script was created for.
     */
    public Map<String, ScriptDocValues<?>> getDoc() {
        return leafLookup.doc();
    }

    /**
     * Return the emitted values from the script execution.
     */
    public List<Object> getEmittedValues() {
        return emittedValues;
    }

    /**
     * Set the current document to run the script on next.
     * Clears the emittedValues as well since they should be scoped per document.
     */
    public void setDocument(int docid) {
        this.emittedValues = new ArrayList<>();
        this.totalByteSize = 0;
        leafLookup.setDocument(docid);
    }

    public void addEmittedValue(Object o) {
        int byteSize = getObjectByteSize(o);
        int newTotalByteSize = totalByteSize + byteSize;
        if (newTotalByteSize <= MAX_BYTE_SIZE) {
            emittedValues.add(o);
            totalByteSize = newTotalByteSize;
        } else {
            throw new IllegalStateException("Exceeded maximum allowed byte size for emitted values");
        }
    }

    private int getObjectByteSize(Object obj) {
        if (obj instanceof String) {
            return ((String) obj).getBytes(StandardCharsets.UTF_8).length;
        } else if (obj instanceof Integer) {
            return Integer.BYTES;
        } else if (obj instanceof Long) {
            return Long.BYTES;
        } else if (obj instanceof Double) {
            return Double.BYTES;
        } else if (obj instanceof Boolean) {
            return Byte.BYTES; // Assuming 1 byte for boolean
        } else if (obj instanceof Tuple) {
            // Assuming each element in the tuple is a double for GeoPoint case
            return Double.BYTES * 2;
        } else if (obj == null) {
            return 0;
        } else {
            throw new IllegalArgumentException("Unsupported object type passed in emit() - " + obj);
        }
    }

    public void execute() {}

    /**
     * A factory to construct {@link DerivedFieldScript} instances.
     *
     * @opensearch.internal
     */
    public interface LeafFactory {
        DerivedFieldScript newInstance(LeafReaderContext ctx) throws IOException;
    }

    /**
     * A factory to construct stateful {@link DerivedFieldScript} factories for a specific index.
     * @opensearch.internal
     */
    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup);
    }
}
