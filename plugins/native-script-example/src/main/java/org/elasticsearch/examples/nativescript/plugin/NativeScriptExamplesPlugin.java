/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.examples.nativescript.plugin;

import org.elasticsearch.examples.nativescript.script.IsPrimeSearchScript;
import org.elasticsearch.examples.nativescript.script.LanguageModelScoreScript;
import org.elasticsearch.examples.nativescript.script.CosineSimilarityScoreScript;
import org.elasticsearch.examples.nativescript.script.PhraseScoreScript;
import org.elasticsearch.examples.nativescript.script.TFIDFScoreScript;
import org.elasticsearch.examples.nativescript.script.PopularityScoreScriptFactory;
import org.elasticsearch.examples.nativescript.script.RandomSortScriptFactory;
import org.elasticsearch.examples.nativescript.script.stockaggs.CombineScriptFactory;
import org.elasticsearch.examples.nativescript.script.stockaggs.InitScriptFactory;
import org.elasticsearch.examples.nativescript.script.stockaggs.MapScriptFactory;
import org.elasticsearch.examples.nativescript.script.stockaggs.ReduceScriptFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.NativeScriptFactory;

import java.util.Arrays;
import java.util.List;

/**
 * This class is instantiated when Elasticsearch loads the plugin for the
 * first time.
 */
public class NativeScriptExamplesPlugin extends Plugin implements ScriptPlugin {

    @Override
    public List<NativeScriptFactory> getNativeScripts() {
        return Arrays.asList(new IsPrimeSearchScript.Factory(), new RandomSortScriptFactory(),
            new PopularityScoreScriptFactory(), new TFIDFScoreScript.Factory(), new CosineSimilarityScoreScript.Factory(),
            new PhraseScoreScript.Factory(), new LanguageModelScoreScript.Factory(),
            // Scripted Metric Aggregations Scripts
            new InitScriptFactory(), new MapScriptFactory(), new CombineScriptFactory(), new ReduceScriptFactory());
    }
}
