/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.action;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.Token;
import org.elasticsearch.painless.ScriptClassInfo;
import org.elasticsearch.painless.antlr.EnhancedSuggestLexer;
import org.elasticsearch.painless.antlr.SuggestLexer;
import org.elasticsearch.painless.lookup.PainlessClass;
import org.elasticsearch.painless.lookup.PainlessClassBinding;
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessField;
import org.elasticsearch.painless.lookup.PainlessInstanceBinding;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * PainlessSuggest generates suggestions for partially completed scripts
 * based on context and source. The parsing for suggestions is
 * extremely lenient and never issues an error outright. The parser attempts
 * to recover given extraneous or unknown input. If an error is unrecoverable,
 * no suggestions are given. Also note that no suggestions are given if
 * a type is resolved as def as there is no way to know what def represents.
 */
public class PainlessSuggest {

    public static List<Suggestion> suggest(PainlessLookup lookup, ScriptClassInfo info, String source) {
        ANTLRInputStream stream = new ANTLRInputStream(source);
        SuggestLexer lexer = new EnhancedSuggestLexer(stream, lookup);
        lexer.removeErrorListeners();

        return new PainlessSuggest(lookup, info, lexer.getAllTokens()).suggest();
    }

    protected final PainlessLookup lookup;
    protected final ScriptClassInfo info;
    protected final List<? extends Token> tokens;

    protected PainlessSuggest(PainlessLookup lookup, ScriptClassInfo info, List<? extends Token> tokens) {
        this.lookup = Objects.requireNonNull(lookup);
        this.info = Objects.requireNonNull(info);
        this.tokens = Collections.unmodifiableList(Objects.requireNonNull(tokens));
    }

    protected List<Suggestion> suggest() {
        // collect data on user-defined functions
        FunctionMachine.FunctionState fs = new FunctionMachine.FunctionState(this.tokens);
        FunctionMachine.walk(fs);

        // setup which function we are in based on the end of the token stream
        // to allow for appropriate top-level parameters to be added to the suggestions
        FunctionMachine.FunctionData fd = fs.functions.isEmpty() ? null : fs.functions.get(fs.functions.size() - 1);

        // set our starting token at the beginning of the stream
        // and this does not change unless there are user-defined functions
        int start = 0;

        if (fd != null) {
            if (fd.bodyEndToken == -1) {
                // cursor ended in a function, so set the starting token for the other
                // state machines to the beginning of the function
                start = fd.bodyStartToken;
            } else {
                // cursor ended in the execute body, so set the starting token to outside
                // the last
                start = fd.bodyEndToken + 2;
            }
        }

        // cut the tokens to just our method body up to the cursor
        List<? extends Token> tokens = this.tokens.subList(start, this.tokens.size());

        // collect data on lambdas
        LambdaMachine.LambdaState ls = new LambdaMachine.LambdaState(tokens);
        LambdaMachine.walk(ls);

        Map<Integer, LambdaMachine.LambdaData> mld = new HashMap<>();

        // set up our lambda at the starting token for use in the block machine
        for (LambdaMachine.LambdaData ld : ls.lambdas) {
            mld.put(ld.headerStartToken, ld);
        }

        // collect data on declarations within the scope the cursor is in
        BlockMachine.BlockState bs = new BlockMachine.BlockState(tokens, mld);
        BlockMachine.walk(bs);

        // collect a list of type resolvable pieces along with a piece to generate
        // suggestion from
        AccessMachine.AccessState as = new AccessMachine.AccessState(tokens);
        AccessMachine.walk(as);

        List<Suggestion> suggestions = new ArrayList<>();

        if (as.target == -2) {
            // the access machine had a result that allows for suggestion generation
            Map<String, String> functions = new HashMap<>();

            // collect user-defined functions that may be provided as suggestions
            for (FunctionMachine.FunctionData function : fs.functions) {
                functions.put(function.functionName + "/" + function.parameterTypes.size(), function.returnType);
            }

            // collect variables that may be provided as suggestions
            Map<String, String> variables = bs.scope.getVariables();

            // add top-level parameters as variables that may be provided as suggestions
            if (fd == null || fd.bodyEndToken != -1) {
                // adds execute method parameters
                for (ScriptClassInfo.MethodArgument argument : info.getExecuteArguments()) {
                    variables.put(argument.getName(), PainlessLookupUtility.typeToCanonicalTypeName(argument.getClazz()));
                }

                for (int parameter = 0; parameter < info.getGetMethods().size(); ++parameter) {
                    String type = PainlessLookupUtility.typeToCanonicalTypeName(info.getGetReturns().get(parameter));
                    org.objectweb.asm.commons.Method method = info.getGetMethods().get(parameter);
                    String name = method.getName().substring(3);
                    name = Character.toLowerCase(name.charAt(0)) + name.substring(1);
                    variables.put(name, type);
                }
            } else {
                // adds user-defined function parameters
                if (fd.parameterNames.size() == fd.parameterTypes.size()) {
                    for (int parameter = 0; parameter < fd.parameterNames.size(); ++parameter) {
                        variables.put(fd.parameterNames.get(parameter), fd.parameterTypes.get(parameter));
                    }
                }
            }

            suggestions = collect(functions, variables, as.segment);
        }

        return suggestions;
    }

    protected List<Suggestion> collect(Map<String, String> functions, Map<String, String> variables, AccessMachine.AccessData segment) {

        Class<?> resolved = null;

        // resolve the types for each segment
        while (segment.child != null && (segment.modifiers & AccessMachine.AccessData.RESOLVE) == AccessMachine.AccessData.RESOLVE) {

            if ((segment.modifiers & AccessMachine.AccessData.ID) == AccessMachine.AccessData.ID) {
                // resolve a type for a variable
                String type = variables.get(segment.text);
                resolved = type == null ? null : lookup.canonicalTypeNameToType(type);
            } else if ((segment.modifiers & AccessMachine.AccessData.TYPE) == AccessMachine.AccessData.TYPE) {
                // resolve a type for a type
                resolved = segment.text == null ? null : lookup.canonicalTypeNameToType(segment.text);
            } else if ((segment.modifiers & AccessMachine.AccessData.CALL) == AccessMachine.AccessData.CALL) {
                // resolve a type for a call
                String type = functions.get(segment.text + "/" + segment.arity);

                if (type != null) {
                    resolved = lookup.canonicalTypeNameToType(type);
                } else {
                    PainlessMethod pm = lookup.lookupImportedPainlessMethod(segment.text, segment.arity);

                    if (pm != null) {
                        resolved = pm.returnType;
                    } else {
                        PainlessClassBinding pcb = lookup.lookupPainlessClassBinding(segment.text, segment.arity);

                        if (pcb != null) {
                            resolved = pcb.returnType;
                        } else {
                            PainlessInstanceBinding pib = lookup.lookupPainlessInstanceBinding(segment.text, segment.arity);
                            resolved = pib == null ? null : pib.returnType;
                        }
                    }
                }
            } else if ((segment.modifiers & AccessMachine.AccessData.METHOD) == AccessMachine.AccessData.METHOD) {
                // resolve a type for a method
                PainlessMethod pm = lookup.lookupPainlessMethod(
                        resolved,
                        (segment.modifiers & AccessMachine.AccessData.STATIC) == AccessMachine.AccessData.STATIC,
                        segment.text,
                        segment.arity);
                resolved = pm == null ? null : pm.returnType;
            } else if ((segment.modifiers & AccessMachine.AccessData.FIELD) == AccessMachine.AccessData.FIELD) {
                // resolve a type for a field
                PainlessField pf = lookup.lookupPainlessField(
                        resolved, (segment.modifiers & AccessMachine.AccessData.STATIC) == AccessMachine.AccessData.STATIC, segment.text);
                resolved = pf == null ? null : pf.typeParameter;
            } else if ((segment.modifiers & AccessMachine.AccessData.CONSTRUCTOR) == AccessMachine.AccessData.CONSTRUCTOR) {
                // resolve the type for a constructor
                resolved = lookup.canonicalTypeNameToType(segment.text);
            } else if ((segment.modifiers & AccessMachine.AccessData.INDEX) == AccessMachine.AccessData.INDEX) {
                // resolve the type for an index
                if (resolved != null) {
                    if (resolved.isArray()) {
                        resolved = resolved.getComponentType();
                    } else if (List.class.isAssignableFrom(resolved) || Map.class.isAssignableFrom(resolved)) {
                        resolved = def.class;
                    }
                }
            } else if ((segment.modifiers & AccessMachine.AccessData.NUMBER) == AccessMachine.AccessData.NUMBER) {
                // resolve the type for a number
                resolved = def.class;
            }

            if (resolved == null || resolved == def.class) {
                // the def type does not have suggestions
                return new ArrayList<>();
            } else if (resolved.isPrimitive()) {
                // suggest based on a boxed type for a primitive
                resolved = PainlessLookupUtility.typeToBoxedType(resolved);
            }

            // resolve the next segment
            segment = segment.child;
        }

        List<Suggestion> suggestions = new ArrayList<>();

        // generate suggestions for the final segment
        if (segment.child == null && (segment.modifiers & AccessMachine.AccessData.SUGGEST) == AccessMachine.AccessData.SUGGEST) {

            // generate suggestions for variables
            if ((segment.modifiers & AccessMachine.AccessData.ID) == AccessMachine.AccessData.ID) {
                for (String name : variables.keySet()) {
                    if (name.startsWith(segment.text)) {
                        suggestions.add(new Suggestion(Suggestion.VARIABLE, name));
                    }
                }
            }

            // generate suggestions for types
            if ((segment.modifiers & AccessMachine.AccessData.TYPE) == AccessMachine.AccessData.TYPE) {
                for (String type : lookup.getCanonicalClassNames()) {
                    if (type.startsWith(segment.text)) {
                        suggestions.add(new Suggestion(Suggestion.TYPE, type));
                    }
                }
            }

            // generate suggestions for calls
            if ((segment.modifiers & AccessMachine.AccessData.CALL) == AccessMachine.AccessData.CALL) {
                for (String call : functions.keySet()) {
                    if (call.startsWith(segment.text)) {
                        suggestions.add(new Suggestion(Suggestion.USER, call));
                    }
                }

                for (String call : lookup.getImportedPainlessMethodsKeys()) {
                    if (call.startsWith(segment.text)) {
                        suggestions.add(new Suggestion(Suggestion.CALL, call));
                    }
                }

                for (String call : lookup.getPainlessClassBindingsKeys()) {
                    if (call.startsWith(segment.text)) {
                        suggestions.add(new Suggestion(Suggestion.CALL, call));
                    }
                }

                for (String call : lookup.getPainlessInstanceBindingsKeys()) {
                    if (call.startsWith(segment.text)) {
                        suggestions.add(new Suggestion(Suggestion.CALL, call));
                    }
                }
            }

            // generate suggestions for methods
            if ((segment.modifiers & AccessMachine.AccessData.METHOD) == AccessMachine.AccessData.METHOD) {
                if (resolved != null) {
                    PainlessClass pc = lookup.lookupPainlessClass(resolved);

                    if (pc != null) {
                        if ((segment.modifiers & AccessMachine.AccessData.STATIC) == AccessMachine.AccessData.STATIC) {
                            for (String method : pc.staticMethods.keySet()) {
                                if (segment.text == null || method.startsWith(segment.text)) {
                                    suggestions.add(new Suggestion(Suggestion.METHOD, method));
                                }
                            }
                        } else {
                            for (String method : pc.methods.keySet()) {
                                if (segment.text == null || method.startsWith(segment.text)) {
                                    suggestions.add(new Suggestion(Suggestion.METHOD, method));
                                }
                            }
                        }
                    }
                }
            }

            // generate suggestions for fields
            if ((segment.modifiers & AccessMachine.AccessData.FIELD) == AccessMachine.AccessData.FIELD) {
                if (resolved != null) {
                    PainlessClass pc = lookup.lookupPainlessClass(resolved);

                    if (pc != null) {
                        if ((segment.modifiers & AccessMachine.AccessData.STATIC) == AccessMachine.AccessData.STATIC) {
                            for (String field : pc.staticFields.keySet()) {
                                if (segment.text == null || field.startsWith(segment.text)) {
                                    suggestions.add(new Suggestion(Suggestion.FIELD, field));
                                }
                            }
                        } else {
                            for (String field : pc.fields.keySet()) {
                                if (segment.text == null || field.startsWith(segment.text)) {
                                    suggestions.add(new Suggestion(Suggestion.FIELD, field));
                                }
                            }
                        }
                    }
                }
            }
        // generate suggestions for the parameters of a specific constructor/call/method
        } else if ((segment.modifiers & AccessMachine.AccessData.PARAMETERS) == AccessMachine.AccessData.PARAMETERS) {

            // generate parameters for a specific constructor
            if ((segment.modifiers & AccessMachine.AccessData.CONSTRUCTOR) == AccessMachine.AccessData.CONSTRUCTOR) {
                PainlessClass pc = lookup.lookupPainlessClass(lookup.canonicalTypeNameToType(segment.text));

                if (pc != null) {
                    for (PainlessConstructor pcon : pc.constructors.values()) {
                        StringBuilder parameters = new StringBuilder(" ");
                        for (Class<?> type : pcon.typeParameters) {
                            parameters.append(PainlessLookupUtility.typeToCanonicalTypeName(type));
                            parameters.append(" ");
                        }
                        suggestions.add(new Suggestion(Suggestion.PARAMETERS, parameters.toString()));
                    }
                }
            }

            // generate parameters for a specific method
            if ((segment.modifiers & AccessMachine.AccessData.METHOD) == AccessMachine.AccessData.METHOD) {
                if (resolved != null) {
                    PainlessClass pc = lookup.lookupPainlessClass(resolved);

                    if (pc != null) {
                        if ((segment.modifiers & AccessMachine.AccessData.STATIC) == AccessMachine.AccessData.STATIC) {
                            for (PainlessMethod pm : pc.staticMethods.values()) {
                                if (pm.javaMethod.getName().equals(segment.text)) {
                                    StringBuilder parameters = new StringBuilder(" ");
                                    for (Class<?> type : pm.typeParameters) {
                                        parameters.append(PainlessLookupUtility.typeToCanonicalTypeName(type));
                                        parameters.append(" ");
                                    }
                                    suggestions.add(new Suggestion("parameters", parameters.toString()));
                                }
                            }
                        } else {
                            for (PainlessMethod pm : pc.methods.values()) {
                                if (pm.javaMethod.getName().equals(segment.text)) {
                                    StringBuilder parameters = new StringBuilder(" ");
                                    for (Class<?> type : pm.typeParameters) {
                                        parameters.append(PainlessLookupUtility.typeToCanonicalTypeName(type));
                                        parameters.append(" ");
                                    }
                                    suggestions.add(new Suggestion("parameters", parameters.toString()));
                                }
                            }
                        }
                    }
                }
            }
        }

        return suggestions;
    }

    /**
     * Suggestion contains the type of suggestion along
     * with the suggested text.
     */
    public static class Suggestion {

        public final static String VARIABLE = "variable";
        public final static String TYPE = "type";
        public final static String USER = "user";
        public final static String CALL = "call";
        public final static String METHOD = "method";
        public final static String FIELD = "field";
        public final static String PARAMETERS = "parameters";

        protected final String type;
        protected final String text;

        public Suggestion(String type, String text) {
            this.type = type;
            this.text = text;
        }

        public String getType() {
            return type;
        }

        public String getText() {
            return text;
        }

        @Override
        public String toString() {
            return "Suggestion{" +
                    "type='" + type + '\'' +
                    ", text='" + text + '\'' +
                    '}';
        }
    }

    /**
     * TokenState keeps track of the current token. The state machines
     * each increment a single token at a time and make decisions by
     * storing state appropriately.
     */
    protected static class TokenState {

        protected final List<? extends Token> tokens;

        protected int current = 0;

        protected TokenState(List<? extends Token> tokens) {
            this.tokens = Collections.unmodifiableList(tokens);
        }
    }

    /**
     * FunctionMachine collects information about user-defined functions.
     * This is required for suggestions relating to static-style calls,
     * and to determine what top-level variables are available.
     */
    protected static class FunctionMachine {

        protected static class FunctionData {

            protected String returnType = "";
            protected String functionName = "";
            protected final List<String> parameterTypes = new ArrayList<>();
            protected final List<String> parameterNames = new ArrayList<>();

            protected int bodyStartToken = -1;
            protected int bodyEndToken = -1;

            @Override
            public String toString() {
                return "FunctionState{" +
                        "returnType='" + returnType + '\'' +
                        ", functionName='" + functionName + '\'' +
                        ", parameterTypes=" + parameterTypes +
                        ", parameterNames=" + parameterNames +
                        ", bodyStartToken=" + bodyStartToken +
                        ", bodyEndToken=" + bodyEndToken +
                        '}';
            }
        }

        protected static class FunctionState extends TokenState {

            protected int target = 0;

            protected String returnType;
            protected String functionName;
            protected String parameterType;
            protected FunctionData functionData;

            protected final List<FunctionData> functions = new ArrayList<>();

            protected int brackets;

            protected FunctionState(List<? extends Token> tokens) {
                super(tokens);
            }
        }

        protected static final List<Function<FunctionState, Integer>> fstates;

        static {
            fstates = new ArrayList<>();
            // 0 - possible start of function
            fstates.add(fs -> {
                Token token = fs.tokens.get(fs.current);
                if (token.getType() == SuggestLexer.ATYPE || token.getType() == SuggestLexer.TYPE) {
                    // VALID: possible return type for function
                    fs.returnType = token.getText();
                    return 1;
                }
                // VALID: not a function
                return 0;
            });
            // 1 - possible function name
            fstates.add(fs -> {
                Token token = fs.tokens.get(fs.current);
                if (token.getType() == SuggestLexer.ID) {
                    // VALID: possible function name
                    fs.functionName = token.getText();
                    return 2;
                }
                // VALID: not a function
                return 0;
            });
            // 2 - start of parameters
            fstates.add(fs -> {
                Token token = fs.tokens.get(fs.current);
                if (token.getType() == SuggestLexer.LP) {
                    // VALID: found a function, record return type and function name
                    fs.functionData = new FunctionData();
                    fs.functionData.returnType = fs.returnType;
                    fs.functionData.functionName = fs.functionName;
                    fs.functions.add(fs.functionData);
                    return 3;
                }
                // VALID: not a function
                return 0;
            });
            // 3 - start of a parameter or end of parameters
            fstates.add(fs -> {
                Token token = fs.tokens.get(fs.current);
                if (token.getType() == SuggestLexer.ATYPE || token.getType() == SuggestLexer.TYPE) {
                    // VALID: found a parameter type
                    fs.parameterType = token.getText();
                    return 6;
                } else if (token.getType() == SuggestLexer.RP) {
                    // VALID: end of function header
                    return 4;
                } else if (token.getType() == SuggestLexer.LBRACK) {
                    // ERROR (process): missing right parenthesis, but found start of function body
                    fs.brackets = 1;
                    fs.functionData.bodyStartToken = fs.current + 1;
                    return 5;
                }
                // ERROR (ignore): unexpected token, keep looking for a sentinel
                return 3;
            });
            // 4 - start of function body
            fstates.add(fs -> {
                Token token = fs.tokens.get(fs.current);
                if (token.getType() == SuggestLexer.LBRACK) {
                    // VALID: found start of function body
                    fs.brackets = 1;
                    fs.functionData.bodyStartToken = fs.current + 1;
                    return 5;
                }
                // ERROR (ignore): unexpected token, keep looking for a sentinel
                return 4;
            });
            // 5 - possible end of function body
            fstates.add(fs -> {
                Token token = fs.tokens.get(fs.current);
                if (token.getType() == SuggestLexer.LBRACK) {
                    // VALID: increase scope
                    ++fs.brackets;
                } else if (token.getType() == SuggestLexer.RBRACK) {
                    // VALID: decrease scope
                    --fs.brackets;
                    if (fs.brackets == 0) {
                        // VALID: end of function body
                        fs.functionData.bodyEndToken = fs.current - 1;
                        return 0;
                    }
                }
                // VALID: keep looking for end of function body
                return 5;
            });
            // 6 - parameter name
            fstates.add(fs -> {
                Token token = fs.tokens.get(fs.current);
                if (token.getType() == SuggestLexer.ID) {
                    // VALID: found a parameter name, record parameter type and name
                    fs.functionData.parameterTypes.add(fs.parameterType);
                    fs.functionData.parameterNames.add(token.getText());
                    return 7;
                } else if (token.getType() == SuggestLexer.RP) {
                    // ERROR (process): missing parameter name, but found end of function header
                    return 4;
                } else if (token.getType() == SuggestLexer.LBRACK) {
                    // ERROR (process): missing parameter name, but found start of function body
                    return 5;
                }
                // ERROR (ignore): unexpected token, keep looking for a sentinel
                return 6;
            });
            // 7 - start of another parameter or end of parameters
            fstates.add(fs -> {
                Token token = fs.tokens.get(fs.current);
                if (token.getType() == SuggestLexer.COMMA) {
                    // VALID: found comma, look for another parameter
                    return 8;
                } else if (token.getType() == SuggestLexer.RP) {
                    // VALID: end of function header
                    return 4;
                } else if (token.getType() == SuggestLexer.LBRACK) {
                    // ERROR (process): missing comma or right parenthesis, but found start of function body
                    return 5;
                }
                // ERROR (ignore): unexpected token, keep looking for a sentinel
                return 7;
            });
            // 8 - start of another parameter
            fstates.add(fs -> {
                Token token = fs.tokens.get(fs.current);
                if (token.getType() == SuggestLexer.ATYPE || token.getType() == SuggestLexer.TYPE) {
                    // VALID: found a parameter type
                    fs.parameterType = token.getText();
                    return 6;
                } else if (token.getType() == SuggestLexer.RP) {
                    // ERROR (process): missing parameter type, but found end of function header
                    return 4;
                } else if (token.getType() == SuggestLexer.LBRACK) {
                    // ERROR (process): missing parameter type, but found start of function body
                    return 5;
                }
                // ERROR (ignore): unexpected token, keep looking for a sentinel
                return 8;
            });
        }

        protected static void walk(FunctionState fs) {
            while (fs.current < fs.tokens.size()) {
                Function<FunctionState, Integer> state = fstates.get(fs.target);
                fs.target = state.apply(fs);
                ++fs.current;
            }
        }

        private FunctionMachine() {
            // do not instantiate
        }
    }

    /**
     * LambdaMachine collects information about lambdas by doing a pass through
     * each token in reverse order. This allows for easier collection of
     * parameter types and parameter names by using the arrow '->' token as a
     * sentinel. Skipping the parameters is possible in BlockMachine using the
     * data collected here.
     */
    protected static class LambdaMachine {

        protected static class LambdaData {

            protected final List<String> parameterTypes = new ArrayList<>();
            protected final List<String> parameterNames = new ArrayList<>();

            protected int headerStartToken = -1;
            protected int headerEndToken = -1;

            @Override
            public String toString() {
                return "LambdaData{" +
                        "parameterTypes=" + parameterTypes +
                        ", parameterNames=" + parameterNames +
                        ", headerStartToken=" + headerStartToken +
                        ", headerEndToken=" + headerEndToken +
                        '}';
            }
        }

        protected static class LambdaState extends TokenState {

            protected int target = 0;

            protected LambdaData lambdaData;

            protected final List<LambdaMachine.LambdaData> lambdas = new ArrayList<>();

            protected LambdaState(List<? extends Token> tokens) {
                super(tokens);
            }
        }

        protected static final List<Function<LambdaMachine.LambdaState, Integer>> lstates;

        static {
            lstates = new ArrayList<>();

            // 0 - possible start of a lambda header
            lstates.add(ls -> {
                Token token = ls.tokens.get(ls.current);
                if (token.getType() == SuggestLexer.ARROW) {
                    // VALID: found start of a lambda header
                    ls.lambdaData = new LambdaData();
                    ls.lambdas.add(ls.lambdaData);
                    ls.lambdaData.headerStartToken = ls.current;
                    ls.lambdaData.headerEndToken = ls.current;
                    return 1;
                }
                // VALID: not a start of a lambda header
                return 0;
            });
            // 1 - parameter name or start of parameters
            lstates.add(ls -> {
                Token token = ls.tokens.get(ls.current);
                if (token.getType() == SuggestLexer.ID) {
                    // VALID: found a singular parameter name
                    ls.lambdaData.headerStartToken = ls.current;
                    ls.lambdaData.parameterTypes.add("def");
                    ls.lambdaData.parameterNames.add(token.getText());
                } else if (token.getType() == SuggestLexer.ARROW) {
                    // ERROR (process): unexpected token, found a possible new start of a lambda header
                    ls.lambdaData = new LambdaData();
                    ls.lambdas.add(ls.lambdaData);
                    ls.lambdaData.headerStartToken = ls.current;
                    ls.lambdaData.headerEndToken = ls.current;
                    return 1;
                } else if (token.getType() == SuggestLexer.RP) {
                    // VALID: found a right parenthesis
                    return 2;
                }
                // ERROR (ignore): unexpected token, start looking for a new lambda header
                return 0;
            });
            // 2 - parameter name or end of parameters
            lstates.add(ls -> {
                Token token = ls.tokens.get(ls.current);
                if (token.getType() == SuggestLexer.LP) {
                    // VALID: found a left parenthesis, end of lambda header
                    ls.lambdaData.headerStartToken = ls.current;
                    return 0;
                } else if (token.getType() == SuggestLexer.ID) {
                    // VALID: found a parameter name
                    ls.lambdaData.headerStartToken = ls.current;
                    ls.lambdaData.parameterTypes.add("def");
                    ls.lambdaData.parameterNames.add(token.getText());
                    return 3;
                } else if (token.getType() == SuggestLexer.ARROW) {
                    // ERROR (process): unexpected token, found a possible new start of a lambda header
                    ls.lambdaData = new LambdaData();
                    ls.lambdas.add(ls.lambdaData);
                    ls.lambdaData.headerStartToken = ls.current;
                    ls.lambdaData.headerEndToken = ls.current;
                    return 1;
                }
                // ERROR (ignore): unexpected token, start looking for a new lambda header
                return 0;
            });
            // 3 - parameter type or end of parameters
            lstates.add(ls -> {
                Token token = ls.tokens.get(ls.current);
                if (token.getType() == SuggestLexer.LP) {
                    // VALID: found a left parenthesis, end of lambda header
                    ls.lambdaData.headerStartToken = ls.current;
                    return 0;
                } else if (token.getType() == SuggestLexer.ATYPE || token.getType() == SuggestLexer.TYPE) {
                    // VALID: found a type to complete a parameter declaration
                    ls.lambdaData.headerStartToken = ls.current;
                    ls.lambdaData.parameterTypes.set(ls.lambdaData.parameterTypes.size() - 1, token.getText());
                    return 4;
                } else if (token.getType() == SuggestLexer.COMMA) {
                    // VALID: found a comma to complete a parameter declaration and specify another
                    return 2;
                } else if (token.getType() == SuggestLexer.ARROW) {
                    // ERROR (process): unexpected token, found a possible new start of a lambda header
                    ls.lambdaData = new LambdaData();
                    ls.lambdas.add(ls.lambdaData);
                    ls.lambdaData.headerStartToken = ls.current;
                    ls.lambdaData.headerEndToken = ls.current;
                    return 1;
                }
                // ERROR (ignore): unexpected token, start looking for a new lambda header
                return 0;
            });
            // 4
            lstates.add(ls -> {
                Token token = ls.tokens.get(ls.current);
                if (token.getType() == SuggestLexer.LP) {
                    // VALID: found a left parenthesis, end of lambda header
                    ls.lambdaData.headerStartToken = ls.current;
                    return 0;
                } if (token.getType() == SuggestLexer.COMMA) {
                    // VALID: found a comma to complete a parameter declaration and specify another
                    return 2;
                } else if (token.getType() == SuggestLexer.ARROW) {
                    // ERROR (process): unexpected token, found a possible new start of a lambda header
                    ls.lambdaData = new LambdaData();
                    ls.lambdas.add(ls.lambdaData);
                    ls.lambdaData.headerStartToken = ls.current;
                    ls.lambdaData.headerEndToken = ls.current;
                    return 1;
                }
                // ERROR (ignore): unexpected token, start looking for a new lambda header
                return 0;
            });
        }

        protected static void walk(LambdaMachine.LambdaState ls) {
            ls.current = ls.tokens.size() - 1;

            while (ls.current >= 0) {
                Function<LambdaMachine.LambdaState, Integer> state = lstates.get(ls.target);
                ls.target = state.apply(ls);
                --ls.current;
            }
        }

        private LambdaMachine() {
            // do not instantiate
        }
    }

    /**
     * BlockMachine collects data about declarations within each
     * scope. This allows for suggestions based on variables that are
     * available within the appropriate scope. BlockMachine maintains
     * two separate pieces of state that combine to give an accurate
     * representation of current variables in a scope. BlockMachine must
     * maintain the appropriate scope through a stack with a potentially
     * different delimiter for a scope's closure. BlockMachine must
     * also use a state machine to track declarations and adds
     * new variables to the current scope. While these are both within
     * BlockMachine, the state transitions are handled separately.
     */
    protected static class BlockMachine {

        protected static class BlockData {

            protected final BlockData parent;

            protected int type;
            protected int sentinel;
            protected boolean pop = false;
            protected int parens = 0;
            protected int braces = 0;

            protected int decltarget = 0;
            protected String decltype = null;
            protected int declparens = 0;
            protected int declbraces = 0;

            protected final Map<String, String> variables = new HashMap<>();

            protected BlockData(BlockData parent, int type, int sentinel) {
                this.parent = parent;
                this.type = type;
                this.sentinel = sentinel;
            }

            protected Map<String, String> getVariables() {
                Map<String, String> rtn = new HashMap<>(variables);

                if (parent != null) {
                    rtn.putAll(parent.getVariables());
                }

                return rtn;
            }

            @Override
            public String toString() {
                StringBuilder builder = new StringBuilder();
                builder.append("[");
                builder.append(type == -1 ? "EOF" : SuggestLexer.ruleNames[type - 1]);
                builder.append(" : ");
                builder.append(sentinel == -1 ? "EOF" : SuggestLexer.ruleNames[sentinel - 1]);
                builder.append(" : ");
                builder.append(variables);
                builder.append("]");

                if (parent != null) {
                    builder.append(" ");
                    builder.append(parent.toString());
                }

                return builder.toString();
            }
        }

        protected static class BlockState extends TokenState {

            protected final Map<Integer, LambdaMachine.LambdaData> mld;

            protected BlockData scope = new BlockData(null, SuggestLexer.EOF, SuggestLexer.EOF);

            protected BlockState(List<? extends Token> tokens, Map<Integer, LambdaMachine.LambdaData> mld) {
                super(tokens);
                this.mld = mld;
            }
        }

        protected static final List<Function<BlockState, Integer>> declstates;

        static {
            declstates = new ArrayList<>();

            // 0 - possible start of a declaration
            declstates.add(bs -> {
                Token token = bs.tokens.get(bs.current);
                if (token.getType() == SuggestLexer.ATYPE || token.getType() == SuggestLexer.TYPE) {
                    // VALID: found a type, possible start of a declaration
                    bs.scope.decltype = token.getText();
                    return 1;
                } else if (token.getType() == SuggestLexer.IN) {
                    // VALID: found a for each loop, add the variable to the scope if previous is a name
                    Token prev = bs.current > 0 ? bs.tokens.get(bs.current - 1) : null;

                    if (prev != null && prev.getType() == SuggestLexer.ID) {
                        bs.scope.variables.put(prev.getText(), "def");
                    }
                }
                // VALID: no declaration found, continue looking for sentinel
                return 0;
            });
            // 1 - possible declaration name
            declstates.add(bs -> {
                Token token = bs.tokens.get(bs.current);
                if (token.getType() == SuggestLexer.ATYPE || token.getType() == SuggestLexer.TYPE) {
                    // ERROR (process): found a type, possible start of a declaration
                    bs.scope.decltype = token.getText();
                    return 1;
                } else if (token.getType() == SuggestLexer.ID) {
                    // VALID: found a declaration name, add the variable to the scope
                    bs.scope.variables.put(token.getText(), bs.scope.decltype);
                    return 2;
                }
                // VALID: no declaration found, continue looking for sentinel
                return 0;
            });
            // 2 - possible multiple names in a declaration
            declstates.add(bs -> {
                Token token = bs.tokens.get(bs.current);
                if (token.getType() == SuggestLexer.COMMA) {
                    // VALID: found a comma, look for another declaration name
                    return 1;
                } else if (token.getType() == SuggestLexer.ASSIGN) {
                    // VALID: found an assignment
                    return 3;
                } else if (token.getType() == SuggestLexer.ATYPE || token.getType() == SuggestLexer.TYPE) {
                    // ERROR (process): found a type, possible start of a declaration
                    bs.scope.decltype = token.getText();
                    return 1;
                }
                // VALID: declaration completed, look for next possible declaration
                return 0;
            });
            // 3 - skip an assignment expression
            declstates.add(bs -> {
                Token token = bs.tokens.get(bs.current);
                if (token.getType() == SuggestLexer.COMMA && bs.scope.declparens == 0 && bs.scope.declbraces == 0) {
                    return 1;
                } else if (token.getType() == SuggestLexer.LP) {
                    ++bs.scope.declparens;
                    return 3;
                } else if (token.getType() == SuggestLexer.RP) {
                    --bs.scope.declparens;
                    return 3;
                } else if (token.getType() == SuggestLexer.LBRACE) {
                    ++bs.scope.declbraces;
                    return 3;
                } else if (token.getType() == SuggestLexer.RBRACE) {
                    --bs.scope.declbraces;
                } else if (token.getType() == SuggestLexer.SEMICOLON) {
                    return 0;
                }
                return 3;
            });
        }

        protected static void scope(BlockState bs) {
            int token = bs.tokens.get(bs.current).getType();
            int prev = bs.current > 0 ? bs.tokens.get(bs.current - 1).getType() : SuggestLexer.EOF;

            if (bs.scope.pop) {
                // NOTE: current scope is marked for closure

                if (token == SuggestLexer.CATCH && (bs.scope.type == SuggestLexer.TRY || bs.scope.type == SuggestLexer.CATCH)) {
                    // CLOSE: found a catch, close the sibling try or catch scope
                    bs.scope = bs.scope.parent;
                } else if (token == SuggestLexer.ELSE) {
                    while (bs.scope.type != SuggestLexer.IF && bs.scope.sentinel == SuggestLexer.SEMICOLON) {
                        // CLOSE: close scopes until we reach the sibling if scope for this specific else
                        bs.scope = bs.scope.parent;
                    }

                    if (bs.scope.type == SuggestLexer.IF) {
                        // CLOSE: close the sibling if scope
                        bs.scope = bs.scope.parent;
                    }
                } else {
                    // CLOSE: close the child scope
                    bs.scope = bs.scope.parent;

                    while (bs.scope.sentinel == SuggestLexer.SEMICOLON) {
                        // CLOSE: a scope without brackets may require that multiple child scopes close based on a single delimiter
                        bs.scope = bs.scope.parent;
                    }
                }
            }

            LambdaMachine.LambdaData ld = bs.mld.get(bs.current);

            if (ld != null) {
                // OPEN: open a lambda scope and skip the previously processed header
                bs.scope = new BlockData(bs.scope, SuggestLexer.ARROW, SuggestLexer.EOF);

                for (int param = 0; param < ld.parameterTypes.size(); ++param) {
                    // NOTE: add lambda parameters to the scope
                    bs.scope.variables.put(ld.parameterNames.get(param), ld.parameterTypes.get(param));
                }

                // NOTE: skip to the end of the pre-processed lambda header
                bs.current = ld.headerEndToken;
                token = SuggestLexer.ARROW;
            } else if (token == SuggestLexer.WHILE || token == SuggestLexer.IF || token == SuggestLexer.ELSE) {
                if (prev == SuggestLexer.ELSE && token == SuggestLexer.IF) {
                    // SWITCH: found an else if statement, so just switch the scope type
                    bs.scope.type = SuggestLexer.IF;
                } else {
                    // OPEN: open a new scope for a while/if/else statement with a semicolon delimiter
                    bs.scope = new BlockData(bs.scope, token, SuggestLexer.SEMICOLON);
                }
            } else if (token == SuggestLexer.FOR) {
                // OPEN: open a new scope for a for loop with a right parenthesis delimiter
                bs.scope = new BlockData(bs.scope, token, SuggestLexer.RP);
            } else if (token == SuggestLexer.DO || token == SuggestLexer.TRY || token == SuggestLexer.CATCH) {
                // OPEN: open a new scope for a do/try/catch statement with a right bracket delimiter
                bs.scope = new BlockData(bs.scope, token, SuggestLexer.RBRACK);
            } else if (token == SuggestLexer.LBRACK) {
                if (bs.scope.sentinel == SuggestLexer.SEMICOLON || bs.scope.sentinel == SuggestLexer.RP) {
                    // NOTE: found a left bracket so switch semicolon or right parenthesis delimiters to
                    bs.scope.sentinel = SuggestLexer.RBRACK;
                }
            } else if (token == SuggestLexer.LP) {
                // NOTE: track our parenthesis depth
                ++bs.scope.parens;
            } else if (token == SuggestLexer.RP) {
                // NOTE: track our parenthesis depth
                bs.scope.parens = Math.max(0, bs.scope.parens - 1);

                if (bs.scope.sentinel == SuggestLexer.RP && bs.scope.parens == 0) {
                    // SWITCH: switch a right parenthesis delimiter to a semicolon delimiter
                    bs.scope.sentinel = SuggestLexer.SEMICOLON;
                }
            } else if (token == SuggestLexer.LBRACE) {
                // NOTE: track our brace depth
                ++bs.scope.braces;
            } else if (token == SuggestLexer.RBRACE) {
                // NOTE: track our brace depth
                bs.scope.braces = Math.max(0, bs.scope.braces - 1);
            }

            if (bs.scope.type == SuggestLexer.ARROW) {
                if (token == SuggestLexer.COMMA || token == SuggestLexer.RP && bs.scope.parens == 0 && bs.scope.braces == 0) {
                    // CLOSE: close the current lambda scope
                    bs.scope = bs.scope.parent;
                }
            } else if (token == bs.scope.sentinel) {
                if (bs.scope.type == SuggestLexer.DO) {
                    // SWITCH: update a closed do scope to a while scope
                    bs.scope.type = SuggestLexer.WHILE;
                    bs.scope.sentinel = SuggestLexer.SEMICOLON;
                } else {
                    // NOTE: mark the current scope for closure
                    bs.scope.pop = true;
                }
            }
        }

        protected static void walk(BlockState bs) {
            while (bs.current < bs.tokens.size()) {
                scope(bs);
                Function<BlockState, Integer> declstate = declstates.get(bs.scope.decltarget);
                bs.scope.decltarget = declstate.apply(bs);
                ++bs.current;
            }
        }

        private BlockMachine() {
            // do not instantiate
        }
    }

    /**
     * AccessMachine builds a chain of type resolvable pieces processing
     * each token from the end in reverse order until it reaches a
     * piece that is not resolvable. Upon completion, the AccessData
     * chain is used to resolve types and offer suggestions based on
     * data collected from the other machines. Sometimes no suggestions
     * are available depending on whether the types exist or if
     * the result is a def type.
     */
    protected static class AccessMachine {

        protected static class AccessData {

            protected static final int RESOLVE = 1 << 0;     // piece to resolve type from
            protected static final int SUGGEST = 1 << 1;     // piece to generate suggestions from
            protected static final int ID = 1 << 2;          // piece that is a simple id
            protected static final int TYPE = 1 << 3;        // piece that is a known type
            protected static final int CALL = 1 << 4;        // piece that is a loose call
            protected static final int STATIC = 1 << 5;      // piece that is static
            protected static final int METHOD = 1 << 6;      // piece that a method call
            protected static final int FIELD = 1 << 7;       // piece that is a field access
            protected static final int CONSTRUCTOR = 1 << 8; // piece that is a constructor
            protected static final int INDEX = 1 << 9;       // piece that is an index
            protected static final int NUMBER = 1 << 10;     // piece that a number
            protected static final int PARAMETERS = 1 << 12; // piece that is an open parenthesis

            protected AccessData child;
            protected int modifiers;
            protected String text;
            protected int arity;

            protected AccessData(AccessData child, int modifiers, String text, int arity) {
                this.child = child;
                this.modifiers = modifiers;
                this.text = text;
                this.arity = arity;
            }

            @Override
            public String toString() {
                String mods = "[ ";

                if ((modifiers & RESOLVE) == RESOLVE) {
                    mods += "RESOLVE ";
                }
                if ((modifiers & SUGGEST) == SUGGEST) {
                    mods += "SUGGEST ";
                }
                if ((modifiers & ID) == ID) {
                    mods += "ID ";
                }
                if ((modifiers & TYPE) == TYPE) {
                    mods += "TYPE ";
                }
                if ((modifiers & CALL) == CALL) {
                    mods += "CALL ";
                }
                if ((modifiers & STATIC) == STATIC) {
                    mods += "STATIC ";
                }
                if ((modifiers & METHOD) == METHOD) {
                    mods += "METHOD ";
                }
                if ((modifiers & FIELD) == FIELD) {
                    mods += "FIELD ";
                }
                if ((modifiers & CONSTRUCTOR) == CONSTRUCTOR) {
                    mods += "CONSTRUCTOR ";
                }
                if ((modifiers & INDEX) == INDEX) {
                    mods += "INDEX ";
                }
                if ((modifiers & NUMBER) == NUMBER) {
                    mods += "NUMBER ";
                }
                if ((modifiers & PARAMETERS) == PARAMETERS) {
                    mods += "PARAMETERS ";
                }

                mods += "]";

                return "Segment{" +
                        "child=" + child +
                        ", modifiers=" + mods +
                        ", text='" + text + '\'' +
                        ", arity=" + arity +
                        '}';
            }
        }

        protected static class AccessState extends TokenState {

            protected int target = 0;
            AccessData segment;

            int brackets = 0;
            int parens = 0;
            int braces = 0;

            protected AccessState(List<? extends Token> tokens) {
                super(tokens);
            }
        }

        protected static final List<Function<AccessState, Integer>> astates;

        static {
            astates = new ArrayList<>();

            // 0 - possible piece to generate suggestions from
            astates.add(as -> {
                Token token = as.tokens.get(as.current);
                if (token.getType() == SuggestLexer.ID || token.getType() == SuggestLexer.TYPE) {
                    // VALID: found a name to generate suggestions from
                    as.segment = new AccessData(
                            null, AccessData.SUGGEST | AccessData.TYPE | AccessData.ID | AccessData.CALL, token.getText(), -1);
                    return as.current == 0 ? -2 : 1;
                } else if (token.getType() == SuggestLexer.LP) {
                    // VALID: found a possible call/method to generate suggestions for
                    as.segment = new AccessData(null, AccessData.PARAMETERS, null, -1);
                    return 2;
                } else if (token.getType() == SuggestLexer.DOT || token.getType() == SuggestLexer.NSDOT) {
                    // VALID: found a dot to generate suggestions for
                    as.segment = new AccessData(null, AccessData.SUGGEST | AccessData.FIELD | AccessData.METHOD, null, -1);
                    return 4;
                } else if (token.getType() == SuggestLexer.DOTID) {
                    // VALID: found a name to generate suggestions from
                    as.segment = new AccessData(null, AccessData.SUGGEST | AccessData.FIELD | AccessData.METHOD, token.getText(), -1);
                    return 3;
                }
                // VALID: cannot generate suggestions
                return -1;
            });
            // 1 - check for declaration or constructor
            astates.add(as -> {
                Token token = as.tokens.get(as.current);
                if (token.getType() == SuggestLexer.TYPE || token.getType() == SuggestLexer.ATYPE) {
                    // VALID: found a declaration, do not generate suggestions
                    return -1;
                } else if (token.getType() == SuggestLexer.NEW) {
                    // VALID: found an instantiation, generate suggestions
                    as.segment = new AccessData(null, AccessData.SUGGEST | AccessData.TYPE, token.getText(), -1);
                }
                // VALID: generate suggestions
                return -2;
            });
            // 2 - check for valid open parenthesis
            astates.add(as -> {
                Token token = as.tokens.get(as.current);
                if (token.getType() == SuggestLexer.TYPE) {
                    // VALID: found a constructor
                    as.segment.modifiers |= AccessData.CONSTRUCTOR;
                    as.segment.text = token.getText();
                    return 9;
                } else if (token.getType() == SuggestLexer.ID) {
                    // VALID: found a call, generate suggestions
                    as.segment.modifiers |= AccessData.CALL;
                    as.segment.text = token.getText();
                    return -2;
                } else if (token.getType() == SuggestLexer.DOTID) {
                    // VALID: found a method
                    as.segment.modifiers |= AccessData.METHOD;
                    as.segment.text = token.getText();
                    return 3;
                }
                // VALID: cannot generate suggestions
                return -1;
            });
            // 3 - check for valid method/field
            astates.add(as -> {
                Token token = as.tokens.get(as.current);
                if (token.getType() == SuggestLexer.DOT || token.getType() == SuggestLexer.NSDOT) {
                    // VALID: found a dot
                    return 4;
                }

                // ERROR (ignore): cannot generate suggestions
                return -1;
            });
            // 4 - look for a resolvable piece from a dot
            astates.add(as -> {
                Token token = as.tokens.get(as.current);
                if (token.getType() == SuggestLexer.ID) {
                    // VALID: found a variable, generate suggestions
                    as.segment = new AccessData(as.segment, AccessData.RESOLVE | AccessData.ID, token.getText(), -1);
                    return -2;
                } else if (token.getType() == SuggestLexer.TYPE || token.getType() == SuggestLexer.ATYPE) {
                    // VALID: found a type, generate suggestions
                    as.segment.modifiers |= AccessData.STATIC;
                    as.segment = new AccessData(as.segment, AccessData.RESOLVE | AccessData.TYPE, token.getText(), -1);
                    return -2;
                } else if (token.getType() == SuggestLexer.DOTID) {
                    // VALID: found a method/field
                    as.segment = new AccessData(as.segment, AccessData.RESOLVE | AccessData.FIELD, token.getText(), -1);
                    return 3;
                } else if (token.getType() == SuggestLexer.DOTINTEGER) {
                    // VALID: found a list access
                    as.segment = new AccessData(as.segment, AccessData.RESOLVE | AccessData.NUMBER, token.getText(), -1);
                    return 3;
                } else if (token.getType() == SuggestLexer.RBRACE) {
                    // VALID: found an index access
                    as.segment = new AccessData(as.segment, AccessData.RESOLVE | AccessData.INDEX, null, -1);
                    return 5;
                } else if (token.getType() == SuggestLexer.RP) {
                    // VALID: found a possible call/method
                    as.segment = new AccessData(as.segment, AccessData.RESOLVE, null, 0);
                    return 7;
                }
                // VALID: cannot generate suggestions
                return -1;
            });
            // 5 - look for a brace access completion
            astates.add(as -> {
                Token token = as.tokens.get(as.current);
                if (token.getType() == SuggestLexer.LBRACE) {
                    if (as.brackets == 0 && as.parens == 0 && as.braces == 0) {
                        // VALID: found a left brace
                        return 6;
                    } else {
                        --as.braces;
                    }
                } else if (token.getType() == SuggestLexer.RBRACE) {
                    ++as.braces;
                } else if (token.getType() == SuggestLexer.LP) {
                    --as.parens;
                } else if (token.getType() == SuggestLexer.RP) {
                    ++as.parens;
                } else if (token.getType() == SuggestLexer.LBRACK) {
                    --as.brackets;
                } else if (token.getType() == SuggestLexer.RBRACK) {
                    ++as.brackets;
                }

                if (as.brackets < 0 || as.parens < 0 || as.braces < 0) {
                    // ERROR (ignore): cannot track brackets/parens/braces, cannot generate suggestions
                    return -1;
                }

                // VALID: continue looking for a sentinel to complete the brace access
                return 5;
            });
            // 6 - look for a resolvable piece from a brace
            astates.add(as -> {
                Token token = as.tokens.get(as.current);
                if (token.getType() == SuggestLexer.ID) {
                    // VALID: found a variable, generate suggestions
                    as.segment = new AccessData(as.segment, AccessData.RESOLVE | AccessData.ID, token.getText(), -1);
                    return -2;
                } else if (token.getType() == SuggestLexer.DOTID) {
                    // VALID: found a method/field
                    as.segment = new AccessData(as.segment, AccessData.RESOLVE | AccessData.FIELD, token.getText(), -1);
                    return 3;
                } else if (token.getType() == SuggestLexer.RBRACE) {
                    // VALID: found an index access
                    as.segment = new AccessData(as.segment, AccessData.RESOLVE | AccessData.INDEX, null, -1);
                    return 5;
                } else if (token.getType() == SuggestLexer.RP) {
                    // VALID: found a possible call/method
                    as.segment = new AccessData(as.segment, AccessData.RESOLVE, null, 0);
                    return 7;
                }
                // VALID: cannot generate suggestions
                return -1;
            });
            // 7 - look for a call/method parenthesis completion
            astates.add(as -> {
                Token token = as.tokens.get(as.current);
                if (token.getType() == SuggestLexer.LP) {
                    if (as.brackets == 0 && as.parens == 0 && as.braces == 0) {
                        // VALID: found a left parenthesis
                        return 8;
                    } else {
                        --as.parens;
                    }
                } else if (token.getType() == SuggestLexer.COMMA) {
                    if (as.brackets == 0 && as.parens == 0 && as.braces == 0) {
                        // VALID: found a comma for an additional argument
                        ++as.segment.arity;
                    }
                } else if (token.getType() == SuggestLexer.RP) {
                    ++as.parens;
                } else if (token.getType() == SuggestLexer.LBRACE) {
                    --as.braces;
                } else if (token.getType() == SuggestLexer.RBRACE) {
                    ++as.braces;
                } else if (token.getType() == SuggestLexer.LBRACK) {
                    --as.brackets;
                } else if (token.getType() == SuggestLexer.RBRACK) {
                    ++as.brackets;
                }

                if (as.brackets < 0 || as.parens < 0 || as.braces < 0) {
                    // ERROR (ignore): cannot track brackets/parens/braces, cannot generate suggestions
                    return -1;
                }

                if (as.segment.arity == 0) {
                    // NOTE: assume arity is 1 if a left parenthesis isn't immediately found
                    as.segment.arity = 1;
                }

                // VALID: continue looking for a sentinel to complete the call/method parenthesis
                return 7;
            });
            // 8 - look for call/method completion
            astates.add(as -> {
                Token token = as.tokens.get(as.current);
                if (token.getType() == SuggestLexer.ID) {
                    // VALID: found a call, generate suggestions
                    as.segment.modifiers |= AccessData.CALL;
                    as.segment.text = token.getText();
                    return -2;
                } else if (token.getType() == SuggestLexer.DOTID) {
                    // VALID: found a method
                    as.segment.modifiers |= AccessData.METHOD;
                    as.segment.text = token.getText();
                    return 3;
                } else if (token.getType() == SuggestLexer.TYPE) {
                    // VALID: found a constructor
                    as.segment.modifiers |= AccessData.CONSTRUCTOR;
                    as.segment.text = token.getText();
                    return 9;
                }
                // VALID: cannot generate suggestions
                return -1;
            });
            // 9 - validate constructor
            astates.add(as -> {
                Token token = as.tokens.get(as.current);
                if (token.getType() == SuggestLexer.NEW) {
                    // VALID: generate suggestions
                    return -2;
                }
                // ERROR (ignore): cannot generate suggestions
                return -1;
            });
        }

        protected static void walk(AccessMachine.AccessState as) {
            as.current = as.tokens.size() - 1;

            while (as.current >= 0) {
                Function<AccessMachine.AccessState, Integer> state = astates.get(as.target);
                as.target = state.apply(as);

                if (as.target < 0) {
                    break;
                }

                --as.current;
            }
        }

        protected AccessMachine() {
            // do not instantiate
        }
    }
}
