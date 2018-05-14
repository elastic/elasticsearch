/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.script;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class ScriptTemplate {

    public static final ScriptTemplate EMPTY = new ScriptTemplate(StringUtils.EMPTY);

    private final String template;
    private final Params params;
    // used for sorting based on scripts
    private final DataType outputType;
    private final List<ScriptFunction> functions;

    public ScriptTemplate(String template) {
        this(template, Params.EMPTY, DataType.KEYWORD);
    }

    public ScriptTemplate(String template, Params params, DataType outputType) {
        this(template, params, emptyList(), outputType);
    }

    public ScriptTemplate(String template, Params params, ScriptFunction function, DataType outputType) {
        this(template, params, function == null ? null : singletonList(function), outputType);
    }

    public ScriptTemplate(String template, Params params, List<ScriptFunction> functions, DataType outputType) {
        this.template = template;
        this.params = params;
        this.outputType = outputType;
        this.functions = functions == null ? emptyList() : functions;
    }

    public String template() {
        return template;
    }

    public Params params() {
        return params;
    }

    public List<ScriptFunction> functions() {
        return functions;
    }

    public List<String> aggRefs() {
        return params.asAggRefs();
    }

    public Map<String, String> aggPaths() {
        return params.asAggPaths();
    }

    public DataType outputType() {
        return outputType;
    }

    public Script toPainless() {
        return new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, bindTemplate(), params.asParams());
    }

    private String bindTemplate() {
        List<String> binding = params.asCodeNames();

        String header = null;
        // sort functions alphabetically
        if (!functions.isEmpty()) {
            List<ScriptFunction> ordered = new ArrayList<>(functions);
            ordered.sort(Comparator.comparing(f -> f.name));
            StringJoiner sj = new StringJoiner("\n", "", "\n");
            for (ScriptFunction scriptFunction : ordered) {
                sj.add(scriptFunction.definition);
            }
            header = sj.toString();
        }

        String combined = header != null ? header + template : template;

        return binding.isEmpty() ? combined : format(Locale.ROOT, combined, binding.toArray());
    }

    @Override
    public int hashCode() {
        return Objects.hash(template, params, functions, outputType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ScriptTemplate other = (ScriptTemplate) obj;
        return Objects.equals(template, other.template)
                && Objects.equals(params, other.params)
                && Objects.equals(functions, other.functions)
                && Objects.equals(outputType, other.outputType);
    }

    @Override
    public String toString() {
        return bindTemplate();
    }

    public static String formatTemplate(String template) {
        return template.replace("{}", "params.%s");
    }
}