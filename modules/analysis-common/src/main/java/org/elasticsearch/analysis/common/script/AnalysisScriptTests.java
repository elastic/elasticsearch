package org.elasticsearch.analysis.common.script;

import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.script.AnalysisPredicateScript;
import org.elasticsearch.script.ScriptContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AnalysisScriptTests extends ScriptTestCase {

    @Override
    protected Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        contexts.put(AnalysisPredicateScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        return contexts;
    }

    public void testAnalysisScript() {
        AnalysisPredicateScript.Factory factory = scriptEngine.compile("test", "return \"one\".contentEquals(term.term)",
            AnalysisPredicateScript.CONTEXT, Collections.emptyMap());

        AnalysisPredicateScript script = factory.newInstance();
        AnalysisPredicateScript.Term term = new AnalysisPredicateScript.Term();
        term.term = "one";
        Assert.assertTrue(script.execute(term));
        term.term = "two";
        Assert.assertFalse(script.execute(term));
    }
}
