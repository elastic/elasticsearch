/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.similarity;

import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.Version;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SimilarityScript;
import org.elasticsearch.script.SimilarityWeightScript;

/** Provider of scripted similarities. */
final class ScriptedSimilarityProvider implements TriFunction<Settings, Version, ScriptService, Similarity> {

    @Override
    public Similarity apply(Settings settings, Version indexCreatedVersion, ScriptService scriptService) {
        boolean discountOverlaps = settings.getAsBoolean(SimilarityProviders.DISCOUNT_OVERLAPS, true);
        Settings scriptSettings = settings.getAsSettings("script");
        Script script = Script.parse(scriptSettings);
        SimilarityScript.Factory scriptFactory = scriptService.compile(script, SimilarityScript.CONTEXT);
        Settings weightScriptSettings = settings.getAsSettings("weight_script");
        Script weightScript = null;
        SimilarityWeightScript.Factory weightScriptFactory = null;
        if (weightScriptSettings.isEmpty() == false) {
            weightScript = Script.parse(weightScriptSettings);
            weightScriptFactory = scriptService.compile(weightScript, SimilarityWeightScript.CONTEXT);
        }
        return new ScriptedSimilarity(
            weightScript == null ? null : weightScript.toString(),
            weightScriptFactory == null ? null : weightScriptFactory::newInstance,
            script.toString(),
            scriptFactory::newInstance,
            discountOverlaps
        );
    }

}
