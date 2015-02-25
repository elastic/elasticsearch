/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transform;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.alerts.support.Script;
import org.elasticsearch.alerts.transform.ChainTransform;
import org.elasticsearch.alerts.transform.ScriptTransform;
import org.elasticsearch.alerts.transform.SearchTransform;
import org.elasticsearch.alerts.transform.Transform;

/**
 *
 */
public final class TransformBuilders {

    private TransformBuilders() {
    }

    public static SearchTransform.SourceBuilder searchTransform(SearchRequest request) {
        return new SearchTransform.SourceBuilder(request);
    }

    public static ScriptTransform.SourceBuilder scriptTransform(String script) {
        return new ScriptTransform.SourceBuilder(script);
    }

    public static ScriptTransform.SourceBuilder scriptTransform(Script script) {
        return new ScriptTransform.SourceBuilder(script);
    }

    public static ChainTransform.SourceBuilder chainTransform(Transform.SourceBuilder... transforms) {
        return new ChainTransform.SourceBuilder(transforms);
    }

}
