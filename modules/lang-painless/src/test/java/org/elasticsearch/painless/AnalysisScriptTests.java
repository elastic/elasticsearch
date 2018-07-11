package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.script.AnalysisScript;
import org.elasticsearch.script.ScriptContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AnalysisScriptTests extends ScriptTestCase {

    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        contexts.put(AnalysisScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        return contexts;
    }

    public void testAnalysisScript() {
        AnalysisScript.Factory factory = scriptEngine.compile("test", "return \"one\".contentEquals(term.term)",
            AnalysisScript.CONTEXT, Collections.emptyMap());

        AnalysisScript script = factory.newInstance();
        AnalysisScript.Term term = new AnalysisScript.Term();
        term.term = "one";
        assertTrue(script.execute(term));
        term.term = "two";
        assertFalse(script.execute(term));
    }
}
