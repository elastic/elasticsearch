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

package org.elasticsearch.index.similarity;

import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;

import java.util.Collections;

public class ScriptedSimilarityProvider extends AbstractSimilarityProvider {

    private final ScriptedSimilarity scriptedSimilarity;

    public ScriptedSimilarityProvider(String name, Settings settings, Settings indexSettings, ScriptService scriptService) {
        super(name);
        boolean discountOverlaps = settings.getAsBoolean("discount_overlaps", true);
        String lang = settings.get("lang", Script.DEFAULT_SCRIPT_LANG);
        String source = settings.get("source");
        if (source == null) {
            throw new IllegalArgumentException("Scripted similarities only support inline scripts, but [source] has no value");
        }
        String stored = settings.get("stored");
        if (stored != null) {
            throw new IllegalArgumentException("Scripted similarities only support inline scripts, but [stored] was provided");
        }
        String id = settings.get("id");
        if (id != null) {
            throw new IllegalArgumentException("Scripted similarities only support inline scripts, but [id] was provided");
        }
        if (settings.getGroups("params").isEmpty() == false) {
            throw new IllegalArgumentException("Scripted similarities do not support [params]");
        }
        Script script = new Script(ScriptType.INLINE, lang, source, Collections.emptyMap());
        ExecutableScript.Factory scriptFactory = scriptService.compile(script, ExecutableScript.SIMILARITY_CONTEXT);
        scriptedSimilarity = new ScriptedSimilarity(script.toString(),
                () -> scriptFactory.newInstance(Collections.emptyMap()), discountOverlaps);
    }

    @Override
    public Similarity get() {
        return scriptedSimilarity;
    }

}
