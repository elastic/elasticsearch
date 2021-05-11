/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.action;

import org.antlr.v4.runtime.Token;
import org.elasticsearch.painless.antlr.SuggestLexer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
                } else if (token.getType() == SuggestLexer.ATYPE || token.getType() == SuggestLexer.TYPE) {
                    // ERROR (process): possible return type for a different function, start over
                    fs.returnType = token.getText();
                    return 1;
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
}
