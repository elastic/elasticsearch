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
import org.elasticsearch.examples.nativescript.script.LookupScript;
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
import org.elasticsearch.script.ScriptModule;

/**
 * This class is instantiated when Elasticsearch loads the plugin for the
 * first time. If you change the name of this plugin, make sure to update
 * src/main/resources/es-plugin.properties file that points to this class.
 */
public class NativeScriptExamplesPlugin extends Plugin {

    /**
     * The name of the plugin.
     * <p>
     * This name will be used by elasticsearch in the log file to refer to this plugin.
     *
     * @return plugin name.
     */
    @Override
    public String name() {
        return "native-script-example";
    }

    /**
     * The description of the plugin.
     *
     * @return plugin description
     */
    @Override
    public String description() {
        return "Native script examples";
    }

    public void onModule(ScriptModule module) {
        // Register each script that we defined in this plugin
        module.registerScript("is_prime", IsPrimeSearchScript.Factory.class);
        module.registerScript("lookup", LookupScript.Factory.class);
        module.registerScript("random", RandomSortScriptFactory.class);
        module.registerScript("popularity", PopularityScoreScriptFactory.class);
        module.registerScript(TFIDFScoreScript.SCRIPT_NAME, TFIDFScoreScript.Factory.class);
        module.registerScript(CosineSimilarityScoreScript.SCRIPT_NAME, CosineSimilarityScoreScript.Factory.class);
        module.registerScript(PhraseScoreScript.SCRIPT_NAME, PhraseScoreScript.Factory.class);
        module.registerScript(LanguageModelScoreScript.SCRIPT_NAME, LanguageModelScoreScript.Factory.class);
        // Scripted Metric Aggregation Scripts
        module.registerScript("stockaggs_init", InitScriptFactory.class);
        module.registerScript("stockaggs_map", MapScriptFactory.class);
        module.registerScript("stockaggs_combine", CombineScriptFactory.class);
        module.registerScript("stockaggs_reduce", ReduceScriptFactory.class);
    }
}
