package org.elasticsearch.script.expression;

import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;

import org.apache.lucene.expressions.js.JavascriptCompiler;

import java.text.ParseException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides the infrastructure for Lucene expressions as a scripting language for Elasticsearch.
 */
public class ExpressionScriptEngineService extends AbstractComponent implements ScriptEngineService {

    private final AtomicLong counter = new AtomicLong();
    private final ClassLoader classLoader; // TODO: should use this instead of the implicit this.getClass().getClassLoader()?
    private final MapperService mapper;

    @Inject
    public ExpressionScriptEngineService(Settings settings, MapperService m) {
        super(settings);
        classLoader = settings.getClassLoader();
        mapper = m;
    }

    @Override
    public String[] types() {
        return new String[]{"expression"};
    }

    @Override
    public String[] extensions() {
        return new String[]{"expr"};
    }

    @Override
    public boolean sandboxed() {
        return true;
    }

    @Override
    public Object compile(String script) {
        try {
            // NOTE: validation is delayed to allow runtime vars
            return JavascriptCompiler.compile(script);
        } catch (ParseException e) {
            throw new ExpressionScriptCompilationException("Failed to parse expression: " + script, e);
        }
    }

    @Override
    public SearchScript search(Object compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars) {
        Expression expr = (Expression)compiledScript;

        // TODO: fill in vars into bindings, do validation
        return new ExpressionScript((Expression)compiledScript, new SimpleBindings());
    }

    @Override
    public ExecutableScript executable(Object compiledScript, @Nullable Map<String, Object> vars) {
        // cannot use expressions for updates (yet)
        throw new UnsupportedOperationException();
    }

    @Override
    public Object execute(Object compiledScript, Map<String, Object> vars) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object unwrap(Object value) {
        return value;
    }

    @Override
    public void close() {}
}
