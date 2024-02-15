/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.scriptrank;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.script.ScriptContext;

import java.util.List;
import java.util.Map;

public abstract class RankScript {

    public static final String[] PARAMETERS = {"ctx"};

    private final Map<String, Object> params;

    public RankScript(Map<String, Object> params) {
        this.params = params;
    }

    public abstract List<ScoreDoc> execute(Map<String, Object> ctx);

    public Map<String, Object> getParams() {
        return params;
    }

    public interface Factory {
        RankScript newInstance(Map<String, Object> params);
    }

    public static ScriptContext<Factory> CONTEXT = new ScriptContext<>(
        "rank",
        Factory.class,
        200,
        TimeValue.timeValueMillis(0),
        false,
        true
    );
}
