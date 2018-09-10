/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.gen.script;

import org.elasticsearch.xpack.sql.expression.function.scalar.whitelist.InternalSqlScriptUtils;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

public final class Scripts {

    private Scripts() {}

    private static final Map<Pattern, String> FORMATTING_PATTERNS;

    static {
        Map<String, String> patterns = new LinkedHashMap<>();
        patterns.put("doc[{}].value", "{sql}.docValue(doc,{})");
        patterns.put("{sql}", InternalSqlScriptUtils.class.getSimpleName());
        patterns.put("{}", "params.%s");

        FORMATTING_PATTERNS = new LinkedHashMap<>();

        for (Entry<String, String> entry : patterns.entrySet()) {
            FORMATTING_PATTERNS.put(Pattern.compile(entry.getKey(), Pattern.LITERAL), entry.getValue());
        }
    }

    /**
     * Expands common tokens inside the script:
     * 
     * <pre>
     * {sql} -&gt; InternalSqlScriptUtils
     * doc[{}].value -&gt; InternalSqlScriptUtils.docValue(doc, {})
     * {}    -&gt; params.%s
     * </pre>
     */
    public static String formatTemplate(String template) {
        for (Entry<Pattern, String> entry : FORMATTING_PATTERNS.entrySet()) {
            template = entry.getKey().matcher(template).replaceAll(entry.getValue());
        }
        return template;
    }

    public static ScriptTemplate nullSafeFilter(ScriptTemplate script) {
        return new ScriptTemplate(formatTemplate(
                format(Locale.ROOT, "{sql}.nullSafeFilter(%s)", script.template())),
                script.params(),
                DataType.BOOLEAN);
    }

    public static ScriptTemplate nullSafeSort(ScriptTemplate script) {
        String methodName = script.outputType().isNumeric() ? "nullSafeSortNumeric" : "nullSafeSortString";
        return new ScriptTemplate(formatTemplate(
                format(Locale.ROOT, "{sql}.%s(%s)", methodName, script.template())),
                script.params(),
                script.outputType());
    }

    public static ScriptTemplate and(ScriptTemplate left, ScriptTemplate right) {
        return binaryMethod("and", left, right, DataType.BOOLEAN);
    }

    public static ScriptTemplate or(ScriptTemplate left, ScriptTemplate right) {
        return binaryMethod("or", left, right, DataType.BOOLEAN);
    }
    
    public static ScriptTemplate binaryMethod(String methodName, ScriptTemplate leftScript, ScriptTemplate rightScript,
            DataType dataType) {
        return new ScriptTemplate(format(Locale.ROOT, formatTemplate("{sql}.%s(%s,%s)"),
                methodName,
                leftScript.template(),
                rightScript.template()),
                paramsBuilder()
                    .script(leftScript.params())
                    .script(rightScript.params())
                    .build(),
                dataType);
    }
}
