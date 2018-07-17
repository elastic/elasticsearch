package org.elasticsearch.analysis.common;

import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.AnalysisPredicateScript;
import org.elasticsearch.script.ScriptContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AnalysisPainlessExtension implements PainlessExtension {

    private static final Whitelist WHITELIST =
        WhitelistLoader.loadFromResourceFiles(AnalysisPainlessExtension.class, "painless_whitelist.txt");

    @Override
    public Map<ScriptContext<?>, List<Whitelist>> getContextWhitelists() {
        return Collections.singletonMap(AnalysisPredicateScript.CONTEXT, Collections.singletonList(WHITELIST));
    }
}
