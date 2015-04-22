/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.watcher.support.Script;
import org.elasticsearch.watcher.transform.chain.ChainTransform;
import org.elasticsearch.watcher.transform.script.ScriptTransform;
import org.elasticsearch.watcher.transform.search.SearchTransform;

/**
 *
 */
public final class TransformBuilders {

    private TransformBuilders() {
    }

    public static SearchTransform.Builder searchTransform(SearchRequest request) {
        return SearchTransform.builder(request);
    }

    public static SearchTransform.Builder searchTransform(SearchRequestBuilder request) {
        return searchTransform(request.request());
    }

    public static ScriptTransform.Builder scriptTransform(String script) {
        return scriptTransform(new Script(script));
    }

    public static ScriptTransform.Builder scriptTransform(Script script) {
        return ScriptTransform.builder(script);
    }

    public static ChainTransform.Builder chainTransform(Transform.Builder... transforms) {
        return ChainTransform.builder().add(transforms);
    }

    public static ChainTransform.Builder chainTransform(Transform... transforms) {
        return ChainTransform.builder(transforms);
    }

}
