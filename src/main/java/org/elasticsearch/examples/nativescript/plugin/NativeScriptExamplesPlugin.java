package org.elasticsearch.examples.nativescript.plugin;

import org.elasticsearch.examples.nativescript.script.IsPrimeSearchScript;
import org.elasticsearch.examples.nativescript.script.LanguageModelScoreScript;
import org.elasticsearch.examples.nativescript.script.LookupScript;
import org.elasticsearch.examples.nativescript.script.CosineSimilarityScoreScript;
import org.elasticsearch.examples.nativescript.script.PhraseScoreScript;
import org.elasticsearch.examples.nativescript.script.TFIDFScoreScript;
import org.elasticsearch.examples.nativescript.script.PopularityScoreScriptFactory;
import org.elasticsearch.examples.nativescript.script.RandomSortScriptFactory;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.script.ScriptModule;

/**
 * This class is instantiated when Elasticsearch loads the plugin for the
 * first time. If you change the name of this plugin, make sure to update
 * src/main/resources/es-plugin.properties file that points to this class.
 */
public class NativeScriptExamplesPlugin extends AbstractPlugin {

    /**
     * The name of the plugin.
     * <p/>
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
    }
}
