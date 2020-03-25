/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.expression.gen.script;

import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.AbstractMap.SimpleEntry;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

public final class Scripts {

    public static final String DOC_VALUE = "doc[{}].value";
    public static final String QL_SCRIPTS = "{ql}";
    public static final String EQL_SCRIPTS = "{eql}";
    public static final String SQL_SCRIPTS = "{sql}";
    public static final String PARAM = "{}";
    public static final String INTERNAL_QL_SCRIPT_UTILS = "InternalQlScriptUtils";
    public static final String INTERNAL_EQL_SCRIPT_UTILS = "InternalEqlScriptUtils";
    public static final String INTERNAL_SQL_SCRIPT_UTILS = "InternalSqlScriptUtils";

    private Scripts() {}

    static final Map<Pattern, String> FORMATTING_PATTERNS = unmodifiableMap(Stream.of(
            new SimpleEntry<>(DOC_VALUE, QL_SCRIPTS + ".docValue(doc,{})"),
            new SimpleEntry<>(QL_SCRIPTS, INTERNAL_QL_SCRIPT_UTILS),
            new SimpleEntry<>(EQL_SCRIPTS, INTERNAL_EQL_SCRIPT_UTILS),
            new SimpleEntry<>(SQL_SCRIPTS, INTERNAL_SQL_SCRIPT_UTILS),
            new SimpleEntry<>(PARAM, "params.%s"))
            .collect(toMap(e -> Pattern.compile(e.getKey(), Pattern.LITERAL), Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new)));

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
                format(Locale.ROOT, "{ql}.nullSafeFilter(%s)", script.template())),
                script.params(),
                DataTypes.BOOLEAN);
    }

    public static ScriptTemplate nullSafeSort(ScriptTemplate script) {
        String methodName = script.outputType().isNumeric() ? "nullSafeSortNumeric" : "nullSafeSortString";
        return new ScriptTemplate(formatTemplate(
                format(Locale.ROOT, "{ql}.%s(%s)", methodName, script.template())),
                script.params(),
                script.outputType());
    }

    public static ScriptTemplate and(ScriptTemplate left, ScriptTemplate right) {
        return binaryMethod("{ql}", "and", left, right, DataTypes.BOOLEAN);
    }

    public static ScriptTemplate or(ScriptTemplate left, ScriptTemplate right) {
        return binaryMethod("{ql}", "or", left, right, DataTypes.BOOLEAN);
    }
    
    public static ScriptTemplate binaryMethod(String prefix, String methodName, ScriptTemplate leftScript, ScriptTemplate rightScript,
            DataType dataType) {
        return new ScriptTemplate(format(Locale.ROOT, formatTemplate("%s.%s(%s,%s)"),
                formatTemplate(prefix),
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
