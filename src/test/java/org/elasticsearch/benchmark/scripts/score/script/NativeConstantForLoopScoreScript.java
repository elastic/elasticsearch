package org.elasticsearch.benchmark.scripts.score.script;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

import java.util.Map;

public class NativeConstantForLoopScoreScript extends AbstractSearchScript {

    public static final String NATIVE_CONSTANT_FOR_LOOP_SCRIPT_SCORE = "native_constant_for_loop_script_score";
    
    public static class Factory implements NativeScriptFactory {

        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new NativeConstantForLoopScoreScript(params);
        }
    }

    private NativeConstantForLoopScoreScript(Map<String, Object> params) {

    }

    @Override
    public Object run() {
        float score = 0;
        for (int i = 0; i < 10; i++) {
            score += Math.log(2);
        }
        return score;
    }

}
