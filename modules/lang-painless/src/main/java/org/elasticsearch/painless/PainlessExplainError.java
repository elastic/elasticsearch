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
