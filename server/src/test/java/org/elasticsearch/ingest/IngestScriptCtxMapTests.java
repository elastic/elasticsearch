/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class IngestScriptCtxMapTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testExposesIngestMetadataLookup() {
        Map<String, Object> source = new HashMap<>(Map.of("_ingest", "source-value", "field", "value"));
        IngestDocMetadata metadata = new IngestDocMetadata(new HashMap<>(Map.of("_version", 5L)), null);
        Map<String, Object> ingestMetadata = new HashMap<>(Map.of("_value", "ingest-value", "timestamp", "now"));
        IngestScriptCtxMap scriptCtx = new IngestScriptCtxMap(source, metadata, ingestMetadata);
        IngestCtxMap normalCtx = new IngestCtxMap(source, metadata);

        assertThat(normalCtx.get("_ingest"), equalTo("source-value"));
        assertThat(scriptCtx.get("_ingest"), sameInstance(scriptCtx.get("_ingest")));
        assertThat(scriptCtx.containsKey("_ingest"), equalTo(true));
        assertThat(scriptCtx.getOrDefault("_ingest", "missing"), sameInstance(scriptCtx.get("_ingest")));
        assertThat(scriptCtx.get("field"), equalTo("value"));
        assertThat(scriptCtx.get("_version"), equalTo(5L));
        assertThat(scriptCtx.size(), equalTo(normalCtx.size()));

        Map<String, Object> scriptIngest = (Map<String, Object>) scriptCtx.get("_ingest");
        assertThat(scriptIngest.get("_value"), equalTo("ingest-value"));
        assertThat(scriptIngest.get("timestamp"), equalTo("now"));
        scriptIngest.put("_value", "changed");
        assertThat(ingestMetadata.get("_value"), equalTo("changed"));

        scriptIngest.put("foo", "bar");
        assertThat(ingestMetadata.get("foo"), equalTo("bar"));
    }

    public void testDoesNotOverridePutContainsKeyEtc() {
        Map<String, Object> source = new HashMap<>();
        IngestScriptCtxMap scriptCtx = new IngestScriptCtxMap(
            source,
            new IngestDocMetadata(new HashMap<>(Map.of("_version", 5L)), null),
            new HashMap<>(Map.of("_value", "ingest-value"))
        );

        assertThat(scriptCtx.containsKey("_ingest"), equalTo(false));
        scriptCtx.put("_ingest", "source-value");
        assertThat(source.get("_ingest"), equalTo("source-value"));
        assertThat(scriptCtx.containsKey("_ingest"), equalTo(true));
        assertThat(((Map<?, ?>) scriptCtx.get("_ingest")).get("_value"), equalTo("ingest-value"));

        scriptCtx.remove("_ingest");
        assertThat(source.containsKey("_ingest"), equalTo(false));
        assertThat(((Map<?, ?>) scriptCtx.get("_ingest")).get("_value"), equalTo("ingest-value"));
    }

}
