package org.elasticsearch.benchmark.scripts.score.script;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;

import java.util.Map;

public class NativeConstantScoreScript extends AbstractSearchScript {

    public static final String NATIVE_CONSTANT_SCRIPT_SCORE = "native_constant_script_score";
    
    public static class Factory implements NativeScriptFactory {

        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new NativeConstantScoreScript();
        }
    }

    private NativeConstantScoreScript() {
    }

    @Override
    public Object run() {
        return 2;
    }

}
