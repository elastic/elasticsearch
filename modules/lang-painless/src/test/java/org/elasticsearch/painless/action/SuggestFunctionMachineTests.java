/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.action;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.elasticsearch.painless.ScriptTestCase;
import org.elasticsearch.painless.action.PainlessExecuteAction.PainlessTestScript;
import org.elasticsearch.painless.action.PainlessSuggest.FunctionMachine.FunctionData;
import org.elasticsearch.painless.action.PainlessSuggest.FunctionMachine.FunctionState;
import org.elasticsearch.painless.antlr.EnhancedSuggestLexer;
import org.elasticsearch.painless.antlr.SuggestLexer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SuggestFunctionMachineTests extends ScriptTestCase {

    private FunctionState getFunctionState(String source) {
        ANTLRInputStream stream = new ANTLRInputStream(source);
        SuggestLexer lexer = new EnhancedSuggestLexer(stream, scriptEngine.getContextsToLookups().get(PainlessTestScript.CONTEXT));
        lexer.removeErrorListeners();

        FunctionState fs = new FunctionState(lexer.getAllTokens());
        PainlessSuggest.FunctionMachine.walk(fs);
        return fs;
    }

    private void checkFunction(FunctionData fd, int bodyStartToken, int bodyEndToken,
            String functionName, String returnType, List<String> parameterTypes, List<String> parameterNames) {

        assertEquals(bodyStartToken, fd.bodyStartToken);
        assertEquals(bodyEndToken, fd.bodyEndToken);
        assertEquals(functionName, fd.functionName);
        assertEquals(returnType, fd.returnType);
        assertEquals(parameterTypes, fd.parameterTypes);
        assertEquals(parameterNames, fd.parameterNames);
    }

    public void testNone() {
        FunctionState fs = getFunctionState("return 0;");
        assertEquals(0, fs.functions.size());
    }

    public void testSingle() {
        FunctionState fs = getFunctionState("int myfunction() {return 1;} myfunction();");
        assertEquals(1, fs.functions.size());
        checkFunction(fs.functions.get(0), 5, 7, "myfunction", "int", Collections.emptyList(), Collections.emptyList());
    }

    public void testSingleIncomplete() {
        FunctionState fs = getFunctionState("int myfunction() {return ");
        assertEquals(1, fs.functions.size());
        checkFunction(fs.functions.get(0), 5, -1, "myfunction", "int", Collections.emptyList(), Collections.emptyList());
    }

    public void testMultiple() {
        FunctionState fs = getFunctionState("int myfunction() {return 1;} void combine(Map x, Map y) {x.addAll(y)} combine([:], [:])");
        assertEquals(2, fs.functions.size());
        checkFunction(fs.functions.get(0), 5, 7, "myfunction", "int", Collections.emptyList(), Collections.emptyList());
        checkFunction(fs.functions.get(1), 19, 24, "combine", "void", Arrays.asList("Map", "Map"), Arrays.asList("x", "y"));
    }

    public void testMultipleIncomplete() {
        FunctionState fs = getFunctionState("int myfunction() {return 1;} void combine(Map x, Map y) {");
        assertEquals(2, fs.functions.size());
        checkFunction(fs.functions.get(0), 5, 7, "myfunction", "int", Collections.emptyList(), Collections.emptyList());
        checkFunction(fs.functions.get(1), 19, -1, "combine", "void", Arrays.asList("Map", "Map"), Arrays.asList("x", "y"));
    }

    public void testMultipleWithSyntaxErrors() {
        FunctionState fs = getFunctionState("int myfunction() return 1;} void combine(Map x, Map y) {");
        assertEquals(2, fs.functions.size());
        checkFunction(fs.functions.get(0), -1, -1, "myfunction", "int", Collections.emptyList(), Collections.emptyList());
        checkFunction(fs.functions.get(1), 18, -1, "combine", "void", Arrays.asList("Map", "Map"), Arrays.asList("x", "y"));
    }

    public void testSingleParameter() {
        FunctionState fs = getFunctionState("int myfunction(int xtest) {return xtest;} myfunction(1);");
        assertEquals(1, fs.functions.size());
        checkFunction(fs.functions.get(0), 7, 9, "myfunction", "int",
                Collections.singletonList("int"), Collections.singletonList("xtest"));
    }

    public void testSingleParameterIncomplete() {
        FunctionState fs = getFunctionState("int myfunction(int xtest) { xtest + ");
        assertEquals(1, fs.functions.size());
        checkFunction(fs.functions.get(0), 7, -1, "myfunction", "int",
                Collections.singletonList("int"), Collections.singletonList("xtest"));
    }

    public void testSingleParameterWithSyntaxErrors() {
        FunctionState fs = getFunctionState("int myfunction(int xtest) { ;;,;; ; ; ");
        assertEquals(1, fs.functions.size());
        checkFunction(fs.functions.get(0), 7, -1, "myfunction", "int",
                Collections.singletonList("int"), Collections.singletonList("xtest"));
    }
}
