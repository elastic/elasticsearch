package org.elasticsearch.script.expression;

import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
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

    @Inject
    public ExpressionScriptEngineService(Settings settings) {
        super(settings);
        classLoader = settings.getClassLoader();
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
            // NOTE: validation is delayed to allow runtime vars, and we don't have access to per index stuff here
            return JavascriptCompiler.compile(script);
        } catch (ParseException e) {
            throw new ExpressionScriptCompilationException("Failed to parse expression: " + script, e);
        }
    }

    @Override
    public SearchScript search(Object compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars) {
        Expression expr = (Expression)compiledScript;
        MapperService mapper = lookup.doc().mapperService();
        ExpressionScriptBindings bindings = new ExpressionScriptBindings();

        for (String variable : expr.variables) {
            if (variable.equals("_score")) {
                // noop: our bindings inherently know how to deal with score
            } else if (vars != null && vars.containsKey(variable)) {
                Object value = vars.get(variable);
                if (value instanceof Double) {
                    bindings.addConstant(variable, ((Double)value).doubleValue());
                } else if (value instanceof Long) {
                    bindings.addConstant(variable, ((Long)value).doubleValue());
                } else if (value instanceof Integer) {
                    bindings.addConstant(variable, ((Integer)value).doubleValue());
                } else {
                    throw new ExpressionScriptExecutionException("Parameter [" + variable + "] must be a numeric type (double or long)");
                }
            } else {
                FieldMapper<?> field = mapper.smartNameFieldMapper(variable);
                if (field.isNumeric() == false) {
                    // TODO: more context (which expression?)
                    throw new ExpressionScriptExecutionException("Field [" + variable + "] used in expression must be numeric");
                }
                IndexFieldData<?> fieldData = lookup.doc().fieldDataService.getForField((NumberFieldMapper)field);
                bindings.addField(variable, fieldData);
            }
        }

        return new ExpressionScript((Expression)compiledScript, bindings);
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
