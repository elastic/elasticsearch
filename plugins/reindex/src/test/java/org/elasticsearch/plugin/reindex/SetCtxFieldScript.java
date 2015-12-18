package org.elasticsearch.plugin.reindex;

import java.util.Map;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.AbstractExecutableScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.ScriptModule;

/**
 * Script used to test scripts in reindex and update-by-query.
 */
public class SetCtxFieldScript extends AbstractExecutableScript {
    private final String key;
    private final Object value;
    private Map<String, Object> ctx;

    public SetCtxFieldScript(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setNextVar(String name, Object value) {
        if (name.equals("ctx")) {
            ctx = (Map<String, Object>) value;
            return;
        }
        throw new IllegalArgumentException("Unexpected variable [" + name + "]");
    }

    @Override
    public Object run() {
        ctx.put(key, value);
        return null;
    }

    public static class RegistrationPlugin extends Plugin {
        @Override
        public String name() {
            return "set-ctx-field-script";
        }

        @Override
        public String description() {
            return "test plugin";
        }

        public void onModule(ScriptModule scripts) {
            scripts.registerScript("set-ctx-field", Factory.class);
        }
    }

    public static class Factory implements NativeScriptFactory {
        @Override
        public ExecutableScript newScript(Map<String, Object> params) {
            if (params.size() != 1) {
                throw new IllegalArgumentException("Expected only a single param!");
            }
            Map.Entry<String, Object> entry = params.entrySet().iterator().next();
            return new SetCtxFieldScript(entry.getKey(), entry.getValue());
        }

        @Override
        public boolean needsScores() {
            return false;
        }
    }
}
