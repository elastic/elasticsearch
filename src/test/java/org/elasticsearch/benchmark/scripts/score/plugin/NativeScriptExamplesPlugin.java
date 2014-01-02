package org.elasticsearch.benchmark.scripts.score.plugin;

import org.elasticsearch.benchmark.scripts.score.script.*;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.script.ScriptModule;

public class NativeScriptExamplesPlugin extends AbstractPlugin {


    @Override
    public String name() {
        return "native-script-example";
    }

    @Override
    public String description() {
        return "Native script examples";
    }

    public void onModule(ScriptModule module) {
        module.registerScript(NativeNaiveTFIDFScoreScript.NATIVE_NAIVE_TFIDF_SCRIPT_SCORE, NativeNaiveTFIDFScoreScript.Factory.class);
        module.registerScript(NativeConstantForLoopScoreScript.NATIVE_CONSTANT_FOR_LOOP_SCRIPT_SCORE, NativeConstantForLoopScoreScript.Factory.class);
        module.registerScript(NativeConstantScoreScript.NATIVE_CONSTANT_SCRIPT_SCORE, NativeConstantScoreScript.Factory.class);
        module.registerScript(NativePayloadSumScoreScript.NATIVE_PAYLOAD_SUM_SCRIPT_SCORE, NativePayloadSumScoreScript.Factory.class);
        module.registerScript(NativePayloadSumNoRecordScoreScript.NATIVE_PAYLOAD_SUM_NO_RECORD_SCRIPT_SCORE, NativePayloadSumNoRecordScoreScript.Factory.class);
    }
}
