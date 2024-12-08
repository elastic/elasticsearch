/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.gen.script;

import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.Check;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
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
    static final String DOC_VALUE_PARAMS_REGEX =
        "InternalQlScriptUtils\\.docValue\\(doc,(params\\.%s)\\)|(params\\.%s)|InternalQlScriptUtils\\.not\\(";
    public static final String INTERNAL_QL_SCRIPT_UTILS = "InternalQlScriptUtils";
    public static final String INTERNAL_EQL_SCRIPT_UTILS = "InternalEqlScriptUtils";
    public static final String INTERNAL_SQL_SCRIPT_UTILS = "InternalSqlScriptUtils";

    private static final int PKG_LENGTH = "org.elasticsearch.xpack.".length();

    private Scripts() {}

    static final Map<Pattern, String> FORMATTING_PATTERNS = unmodifiableMap(
        Stream.of(
            new SimpleEntry<>(DOC_VALUE, QL_SCRIPTS + ".docValue(doc,{})"),
            new SimpleEntry<>(QL_SCRIPTS, INTERNAL_QL_SCRIPT_UTILS),
            new SimpleEntry<>(EQL_SCRIPTS, INTERNAL_EQL_SCRIPT_UTILS),
            new SimpleEntry<>(SQL_SCRIPTS, INTERNAL_SQL_SCRIPT_UTILS),
            new SimpleEntry<>(PARAM, "params.%s")
        ).collect(toMap(e -> Pattern.compile(e.getKey(), Pattern.LITERAL), Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new))
    );
    static final Pattern qlDocValuePattern = Pattern.compile(DOC_VALUE_PARAMS_REGEX);

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
        return new ScriptTemplate(
            formatTemplate(format(Locale.ROOT, "{ql}.nullSafeFilter(%s)", script.template())),
            script.params(),
            DataTypes.BOOLEAN
        );
    }

    public static ScriptTemplate nullSafeSort(ScriptTemplate script) {
        String methodName = script.outputType().isNumeric() ? "nullSafeSortNumeric" : "nullSafeSortString";
        return new ScriptTemplate(
            formatTemplate(format(Locale.ROOT, "{ql}.%s(%s)", methodName, script.template())),
            script.params(),
            script.outputType()
        );
    }

    public static ScriptTemplate and(ScriptTemplate left, ScriptTemplate right) {
        return binaryMethod("{ql}", "and", left, right, DataTypes.BOOLEAN);
    }

    public static ScriptTemplate or(ScriptTemplate left, ScriptTemplate right) {
        return binaryMethod("{ql}", "or", left, right, DataTypes.BOOLEAN);
    }

    public static ScriptTemplate binaryMethod(
        String prefix,
        String methodName,
        ScriptTemplate leftScript,
        ScriptTemplate rightScript,
        DataType dataType
    ) {
        return new ScriptTemplate(
            format(
                Locale.ROOT,
                formatTemplate("%s.%s(%s,%s)"),
                formatTemplate(prefix),
                methodName,
                leftScript.template(),
                rightScript.template()
            ),
            paramsBuilder().script(leftScript.params()).script(rightScript.params()).build(),
            dataType
        );
    }

    public static String classPackageAsPrefix(Class<?> function) {
        String prefix = function.getPackageName().substring(PKG_LENGTH);
        int index = prefix.indexOf('.');
        Check.isTrue(index > 0, "invalid package {}", prefix);
        return "{" + prefix.substring(0, index) + "}";
    }

    /**
     * This method replaces any .docValue(doc,params.%s) call with a "Xn" variable.
     * Each variable is then used in a {@code java.util.function.Predicate} to iterate over the doc_values in a Painless script.
     * Multiple .docValue(doc,params.%s) calls for the same field will use multiple .docValue calls, meaning
     * a different value of the field will be used for each usage in the script.
     *
     * For example, a query of the form fieldA - fieldB > 0 that gets translated into the following Painless script
     * {@code InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.gt(InternalQlScriptUtils.sub(
     * InternalQlScriptUtils.docValue(doc,params.v0),InternalQlScriptUtils.docValue(doc,params.v1)),params.v2))}
     * will become, after this method rewrite
     * {@code InternalEqlScriptUtils.multiValueDocValues(doc,params.v0,X1 -> InternalEqlScriptUtils.multiValueDocValues(doc,params.v1,
     * X2 -> InternalQlScriptUtils.nullSafeFilter(InternalQlScriptUtils.gt(InternalQlScriptUtils.sub(X1,X2),params.v2))))}
     */
    public static ScriptTemplate multiValueDocValuesRewrite(ScriptTemplate script) {
        return docValuesRewrite(script, false);
    }

    private static ScriptTemplate docValuesRewrite(ScriptTemplate script, boolean useSameValueInScript) {
        int index = 0; // counts how many params there are to be able to get their value from script's params
        Map<String, Object> params = script.params().asParams();
        List<Object> fieldVars = new ArrayList<>();
        List<Object> otherVars = new ArrayList<>();
        StringBuilder newTemplate = new StringBuilder();
        int negated = 0;

        String[] tokens = splitWithMatches(script.template(), qlDocValuePattern);
        for (String token : tokens) {
            // A docValue call will be replaced with "X" followed by a counter
            // The scripts that come into this method can contain multiple docValue calls for the same field
            // This method will use only one variable for one docValue call
            if ("InternalQlScriptUtils.docValue(doc,params.%s)".equals(token)) {
                Object fieldName = params.get("v" + index);

                if (useSameValueInScript) {
                    // if the field is already in our list, don't add it one more time
                    if (fieldVars.contains(fieldName) == false) {
                        fieldVars.add(fieldName);
                    }
                    newTemplate.append("X" + fieldVars.indexOf(fieldName));
                } else {
                    fieldVars.add(fieldName);
                    newTemplate.append("X" + (fieldVars.size() - 1));
                }
                // increase the params position
                index++;
            } else if ("InternalQlScriptUtils.not(".equals(token)) {
                negated++;
            } else if ("params.%s".equals(token)) {
                newTemplate.append(token);
                // gather the other type of params (which are not docValues calls) so that at the end we rebuild the list of params
                otherVars.add(params.get("v" + index));
                index++;
            } else {
                newTemplate.append(token);
            }
            for (int i = 0; i < negated - 1; i++) {
                // remove this many closing parantheses as "InternalQlScriptUtils.not(" matches found, minus one
                newTemplate.deleteCharAt(newTemplate.length() - 1);
            }
        }

        // iterate over the fields in reverse order and add a multiValueDocValues call for each
        for (int i = fieldVars.size() - 1; i >= 0; i--) {
            newTemplate.insert(0, "InternalEqlScriptUtils.multiValueDocValues(doc,params.%s,X" + i + " -> ");
            newTemplate.append(")");
        }
        if (negated > 0) {
            newTemplate.insert(0, "InternalQlScriptUtils.not(");
        }

        ParamsBuilder newParams = paramsBuilder();
        // field variables are first
        fieldVars.forEach(v -> newParams.variable(v));
        // the rest of variables come after
        otherVars.forEach(v -> newParams.variable(v));

        return new ScriptTemplate(newTemplate.toString(), newParams.build(), DataTypes.BOOLEAN);
    }

    /*
     * Split a string given a regular expression into tokens. The list of tokens includes both the
     * segments that matched the regex and also the segments that didn't.
     * "fooxbarxbaz" split using the "x" regex will build an array like ["foo","x","bar","x","baz"]
     */
    static String[] splitWithMatches(String input, Pattern pattern) {
        int index = 0;
        ArrayList<String> matchList = new ArrayList<>();
        Matcher m = pattern.matcher(input);

        while (m.find()) {
            if (index != m.start()) {
                matchList.add(input.subSequence(index, m.start()).toString()); // add the segment before the match
            }
            if (m.start() != m.end()) {
                matchList.add(input.subSequence(m.start(), m.end()).toString()); // add the match itself
            }
            index = m.end();
        }

        // if no match was found, return this
        if (index == 0) {
            return new String[] { input };
        }

        // add remaining segment and avoid an empty element in matches list
        if (index < input.length()) {
            matchList.add(input.subSequence(index, input.length()).toString());
        }

        // construct result
        return matchList.toArray(new String[matchList.size()]);
    }
}
