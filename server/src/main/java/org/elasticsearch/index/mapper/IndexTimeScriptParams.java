/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.Collections;
import java.util.Map;

public class IndexTimeScriptParams {

    private final BytesReference source;
    private final ParseContext.Document document;

    private SourceLookup sourceLookup;

    public IndexTimeScriptParams(BytesReference source, ParseContext.Document document) {
        this.source = source;
        this.document = document;
    }

    public SourceLookup source() {
        if (sourceLookup == null) {
            sourceLookup = new SourceLookup();
            sourceLookup.setSource(source);
        }
        return sourceLookup;
    }

    public Map<String, ScriptDocValues<?>> doc() {
        return Collections.emptyMap();  // TODO
    }
}
