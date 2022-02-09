/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.api.Debug;
import org.elasticsearch.painless.lookup.PainlessClass;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.script.ScriptException;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Collections.singletonList;

/**
 * Thrown by {@link Debug#explain(Object)} to explain an object. Subclass of {@linkplain Error} so it cannot be caught by painless
 * scripts.
 */
public class PainlessExplainError extends Error {
    private final Object objectToExplain;

    public PainlessExplainError(Object objectToExplain) {
        this.objectToExplain = objectToExplain;
    }

    Object getObjectToExplain() {
        return objectToExplain;
    }

    /**
     * Headers to be added to the {@link ScriptException} for structured rendering.
     */
    public Map<String, List<String>> getHeaders(PainlessLookup painlessLookup) {
        Map<String, List<String>> headers = new TreeMap<>();
        String toString = "null";
        String javaClassName = null;
        String painlessClassName = null;
        if (objectToExplain != null) {
            toString = objectToExplain.toString();
            javaClassName = objectToExplain.getClass().getName();
            PainlessClass struct = painlessLookup.lookupPainlessClass(objectToExplain.getClass());
            if (struct != null) {
                painlessClassName = PainlessLookupUtility.typeToCanonicalTypeName(objectToExplain.getClass());
            }
        }

        headers.put("es.to_string", singletonList(toString));
        if (painlessClassName != null) {
            headers.put("es.painless_class", singletonList(painlessClassName));
        }
        if (javaClassName != null) {
            headers.put("es.java_class", singletonList(javaClassName));
        }
        return headers;
    }
}
