/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.get;

import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

public record GetAndFetchContext(Engine.Get engineGet, String[] gFields, FetchSourceContext fetchSourceContext) {
    public String id() {
        return engineGet().id();
    }
}
