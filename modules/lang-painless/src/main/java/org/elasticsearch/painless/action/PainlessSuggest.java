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

    /**
     * Suggestion contains the type of suggestion along
     * with the suggested text.
     */
    public static class Suggestion {

        public final static String VARIABLE = "variable";
        public final static String TYPE = "type";
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

        protected static class FunctionState {

            protected final TokenState ts;

            protected int target = 0;

            protected String returnType;
            protected String functionName;
            protected String parameterType;
            protected FunctionData functionData;

            protected final List<FunctionData> functions = new ArrayList<>();

            protected int brackets;

            protected FunctionState(TokenState ts) {
                this.ts = ts;
            }
        }

        protected static final List<Function<FunctionState, Integer>> fstates;

        static {
            fstates = new ArrayList<>();
            // 0 - possible start of function
            fstates.add(fs -> {
                Token token = fs.ts.tokens.get(fs.ts.current);
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
                Token token = fs.ts.tokens.get(fs.ts.current);
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
                Token token = fs.ts.tokens.get(fs.ts.current);
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
                Token token = fs.ts.tokens.get(fs.ts.current);
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
                    fs.functionData.bodyStartToken = fs.ts.current + 1;
                    return 5;
                }
                // ERROR (ignore): unexpected token, keep looking for a sentinel
                return 3;
            });
            // 4 - start of function body
            fstates.add(fs -> {
                Token token = fs.ts.tokens.get(fs.ts.current);
                if (token.getType() == SuggestLexer.LBRACK) {
                    // VALID: found start of function body
                    fs.brackets = 1;
                    fs.functionData.bodyStartToken = fs.ts.current + 1;
                    return 5;
                }
                // ERROR (ignore): unexpected token, keep looking for a sentinel
                return 4;
            });
            // 5 - possible end of function body
            fstates.add(fs -> {
                Token token = fs.ts.tokens.get(fs.ts.current);
                if (token.getType() == SuggestLexer.LBRACK) {
                    // VALID: increase scope
                    ++fs.brackets;
                } else if (token.getType() == SuggestLexer.RBRACK) {
                    // VALID: decrease scope
                    --fs.brackets;
                    if (fs.brackets == 0) {
                        // VALID: end of function body
                        fs.functionData.bodyEndToken = fs.ts.current - 1;
                        return 0;
                    }
                }
                // VALID: keep looking for end of function body
                return 5;
            });
            // 6 - parameter name
            fstates.add(fs -> {
                Token token = fs.ts.tokens.get(fs.ts.current);
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
                Token token = fs.ts.tokens.get(fs.ts.current);
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
                Token token = fs.ts.tokens.get(fs.ts.current);
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
            TokenState ts = fs.ts;

            while (ts.current < ts.tokens.size()) {
                Function<FunctionState, Integer> state = fstates.get(fs.target);
                fs.target = state.apply(fs);
                ++ts.current;
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

        protected static class LambdaState {

            protected final TokenState ws;

            protected LambdaState(TokenState ws) {
                this.ws = ws;
            }

            protected int target = 0;

            protected LambdaData lambdaData;

            protected final List<LambdaMachine.LambdaData> lambdas = new ArrayList<>();
        }

        protected static final List<Function<LambdaMachine.LambdaState, Integer>> lstates;

        static {
            lstates = new ArrayList<>();

            // 0 - possible start of a lambda header
            lstates.add(ls -> {
                Token token = ls.ws.tokens.get(ls.ws.current);
                if (token.getType() == SuggestLexer.ARROW) {
                    // VALID: found start of a lambda header
                    ls.lambdaData = new LambdaData();
                    ls.lambdas.add(ls.lambdaData);
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    ls.lambdaData.headerEndToken = ls.ws.current;
                    return 1;
                }
                // VALID: not a start of a lambda header
                return 0;
            });
            // 1 - parameter name or start of parameters
            lstates.add(ls -> {
                Token token = ls.ws.tokens.get(ls.ws.current);
                if (token.getType() == SuggestLexer.ID) {
                    // VALID: found a singular parameter name
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    ls.lambdaData.parameterTypes.add("def");
                    ls.lambdaData.parameterNames.add(token.getText());
                } else if (token.getType() == SuggestLexer.ARROW) {
                    // ERROR (process): unexpected token, found a possible new start of a lambda header
                    ls.lambdaData = new LambdaData();
                    ls.lambdas.add(ls.lambdaData);
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    ls.lambdaData.headerEndToken = ls.ws.current;
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
                Token token = ls.ws.tokens.get(ls.ws.current);
                if (token.getType() == SuggestLexer.LP) {
                    // VALID: found a left parenthesis, end of lambda header
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    return 0;
                } else if (token.getType() == SuggestLexer.ID) {
                    // VALID: found a parameter name
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    ls.lambdaData.parameterTypes.add("def");
                    ls.lambdaData.parameterNames.add(token.getText());
                    return 3;
                } else if (token.getType() == SuggestLexer.ARROW) {
                    // ERROR (process): unexpected token, found a possible new start of a lambda header
                    ls.lambdaData = new LambdaData();
                    ls.lambdas.add(ls.lambdaData);
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    ls.lambdaData.headerEndToken = ls.ws.current;
                    return 1;
                }
                // ERROR (ignore): unexpected token, start looking for a new lambda header
                return 0;
            });
            // 3 - parameter type or end of parameters
            lstates.add(ls -> {
                Token token = ls.ws.tokens.get(ls.ws.current);
                if (token.getType() == SuggestLexer.LP) {
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    return 0;
                } else if (token.getType() == SuggestLexer.ATYPE || token.getType() == SuggestLexer.TYPE) {
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    ls.lambdaData.parameterTypes.set(ls.lambdaData.parameterTypes.size() - 1, token.getText());
                    return 4;
                } else if (token.getType() == SuggestLexer.COMMA) {
                    return 2;
                } else if (token.getType() == SuggestLexer.ARROW) {
                    ls.lambdaData = new LambdaData();
                    ls.lambdas.add(ls.lambdaData);
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    ls.lambdaData.headerEndToken = ls.ws.current;
                    return 1;
                }
                return 0;
            });
            // 4
            lstates.add(ls -> {
                Token token = ls.ws.tokens.get(ls.ws.current);
                if (token.getType() == SuggestLexer.LP) {
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    return 0;
                } if (token.getType() == SuggestLexer.COMMA) {
                    return 2;
                } else if (token.getType() == SuggestLexer.ARROW) {
                    ls.lambdaData = new LambdaData();
                    ls.lambdas.add(ls.lambdaData);
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    ls.lambdaData.headerEndToken = ls.ws.current;
                    return 1;
                }
                return 0;
            });
        }

        protected static void walk(LambdaMachine.LambdaState ls) {
            TokenState ws = ls.ws;
            ws.current = ws.tokens.size() - 1;

            while (ws.current >= 0) {
                Function<LambdaMachine.LambdaState, Integer> state = lstates.get(ls.target);
                ls.target = state.apply(ls);
                --ws.current;
            }
        }

        protected LambdaMachine() {

        }
    }

    protected static class BlockMachine {

        protected static class BlockState {

            protected static class BlockScope {

                protected final BlockScope parent;

                protected int type;
                protected int sentinel;
                protected boolean pop = false;
                protected int parens = 0;
                protected int braces = 0;

                protected final Map<String, String> variables = new HashMap<>();
                protected int decltarget = 0;
                protected String decltype = null;
                protected int declparens = 0;
                protected int declbraces = 0;

                protected BlockScope(BlockScope parent, int type, int sentinel) {
                    this.parent = parent;
                    this.type = type;
                    this.sentinel = sentinel;
                }

                public Map<String, String> getVariables() {
                    Map<String, String> rtn = new HashMap<>(variables);

                    if (parent != null) {
                        rtn.putAll(parent.getVariables());
                    }

                    return rtn;
                }

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

            protected final TokenState ws;
            protected final Map<Integer, LambdaMachine.LambdaData> mld;

            protected BlockScope scope = new BlockScope(null, SuggestLexer.EOF, SuggestLexer.EOF);

            protected BlockState(TokenState ws, Map<Integer, LambdaMachine.LambdaData> mld) {
                this.ws = ws;
                this.mld = mld;
            }
        }

        protected static final List<Function<BlockState, Integer>> declstates;

        static {
            declstates = new ArrayList<>();

            // 0
            declstates.add(bs -> {
                Token token = bs.ws.tokens.get(bs.ws.current);
                if (token.getType() == SuggestLexer.ATYPE || token.getType() == SuggestLexer.TYPE) {
                    bs.scope.decltype = token.getText();
                    return 1;
                } else if (token.getType() == SuggestLexer.IN) {
                    Token prev = bs.ws.current > 0 ? bs.ws.tokens.get(bs.ws.current - 1) : null;

                    if (prev != null && prev.getType() == SuggestLexer.ID) {
                        bs.scope.variables.put(prev.getText(), "def");
                    }
                }
                return 0;
            });
            // 1
            declstates.add(bs -> {
                Token token = bs.ws.tokens.get(bs.ws.current);
                if (token.getType() == SuggestLexer.ATYPE || token.getType() == SuggestLexer.TYPE) {
                    bs.scope.decltype = token.getText();
                    return 1;
                } else if (token.getType() == SuggestLexer.ID) {
                    bs.scope.variables.put(token.getText(), bs.scope.decltype);
                    return 2;
                }
                return 0;
            });
            // 2
            declstates.add(bs -> {
                Token token = bs.ws.tokens.get(bs.ws.current);
                if (token.getType() == SuggestLexer.COMMA) {
                    return 1;
                } else if (token.getType() == SuggestLexer.ASSIGN) {
                    return 3;
                } else if (token.getType() == SuggestLexer.ATYPE || token.getType() == SuggestLexer.TYPE) {
                    bs.scope.decltype = token.getText();
                    return 1;
                }
                return 0;
            });
            // 3
            declstates.add(bs -> {
                Token token = bs.ws.tokens.get(bs.ws.current);
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

        protected static void scope(BlockState bs, StringBuilder builder) {
            TokenState ws = bs.ws;

            int token = ws.tokens.get(ws.current).getType();
            int prev = ws.current > 0 ? ws.tokens.get(ws.current - 1).getType() : SuggestLexer.EOF;

            if (bs.scope.pop) {
                if (token == SuggestLexer.CATCH && (bs.scope.type == SuggestLexer.TRY || bs.scope.type == SuggestLexer.CATCH)) {
                    bs.scope = bs.scope.parent;
                } else if (token == SuggestLexer.ELSE) {
                    while (bs.scope.type != SuggestLexer.IF && bs.scope.sentinel == SuggestLexer.SEMICOLON) {
                        bs.scope = bs.scope.parent;
                    }

                    if (bs.scope.type == SuggestLexer.IF) {
                        bs.scope = bs.scope.parent;
                    }
                } else {
                    bs.scope = bs.scope.parent;

                    while (bs.scope.sentinel == SuggestLexer.SEMICOLON) {
                        bs.scope = bs.scope.parent;
                    }
                }
            }

            LambdaMachine.LambdaData ld = bs.mld.get(ws.current);

            if (ld != null) {
                bs.scope = new BlockState.BlockScope(bs.scope, SuggestLexer.ARROW, SuggestLexer.EOF);

                for (int param = 0; param < ld.parameterTypes.size(); ++param) {
                    bs.scope.variables.put(ld.parameterNames.get(param), ld.parameterTypes.get(param));
                }

                ws.current = ld.headerEndToken;
                token = SuggestLexer.ARROW;
            } else if (token == SuggestLexer.WHILE || token == SuggestLexer.IF || token == SuggestLexer.ELSE) {
                if (prev == SuggestLexer.ELSE && token == SuggestLexer.IF) {
                    bs.scope.type = SuggestLexer.IF;
                } else {
                    bs.scope = new BlockState.BlockScope(bs.scope, token, SuggestLexer.SEMICOLON);
                }
            } else if (token == SuggestLexer.FOR) {
                bs.scope = new BlockState.BlockScope(bs.scope, token, SuggestLexer.RP);
            } else if (token == SuggestLexer.DO || token == SuggestLexer.TRY || token == SuggestLexer.CATCH) {
                bs.scope = new BlockState.BlockScope(bs.scope, token, SuggestLexer.RBRACK);
            } else if (token == SuggestLexer.LBRACK) {
                if (bs.scope.sentinel == SuggestLexer.SEMICOLON || bs.scope.sentinel == SuggestLexer.RP) {
                    bs.scope.sentinel = SuggestLexer.RBRACK;
                }
            } else if (token == SuggestLexer.LP) {
                ++bs.scope.parens;
            } else if (token == SuggestLexer.RP) {
                bs.scope.parens = Math.max(0, bs.scope.parens - 1);

                if (bs.scope.sentinel == SuggestLexer.RP && bs.scope.parens == 0) {
                    bs.scope.sentinel = SuggestLexer.SEMICOLON;
                }
            } else if (token == SuggestLexer.LBRACE) {
                ++bs.scope.braces;
            } else if (token == SuggestLexer.RBRACE) {
                bs.scope.braces = Math.max(0, bs.scope.braces - 1);
            }

            if (bs.scope.type == SuggestLexer.ARROW) {
                if (token == SuggestLexer.COMMA || token == SuggestLexer.RP && bs.scope.parens == 0 && bs.scope.braces == 0) {
                    bs.scope = bs.scope.parent;
                }
            } else if (token == bs.scope.sentinel) {
                if (bs.scope.type == SuggestLexer.DO) {
                    bs.scope.type = SuggestLexer.WHILE;
                    bs.scope.sentinel = SuggestLexer.SEMICOLON;
                } else {
                    bs.scope.pop = true;
                }
            }
        }

        protected static void walk(BlockState bs, StringBuilder builder) {
            TokenState ws = bs.ws;

            // DEBUG
            String previous = "[EOF : EOF]";
            // END DEBUG

            while (ws.current < ws.tokens.size()) {
                scope(bs, builder);

                // DEBUG
                String str = bs.scope.toString();
                if (str.equals(previous) == false) {
                    int token = ws.tokens.get(ws.current).getType();
                    builder.append(token == -1 ? "EOF" : SuggestLexer.ruleNames[token - 1]);
                    builder.append(" : ");
                    builder.append(bs.scope);
                    builder.append("\n");
                    previous = str;
                }
                // END DEBUG

                Function<BlockState, Integer> declstate = declstates.get(bs.scope.decltarget);
                bs.scope.decltarget = declstate.apply(bs);
                ++ws.current;
            }
        }

        protected BlockMachine() {

        }
    }

    protected static class Segment {

        protected static final int RESOLVE = 1 << 0;
        protected static final int SUGGEST = 1 << 1;
        protected static final int ID = 1 << 2;
        protected static final int TYPE = 1 << 3;
        protected static final int CALL = 1 << 4;
        protected static final int STATIC = 1 << 5;
        protected static final int METHOD = 1 << 6;
        protected static final int FIELD = 1 << 7;
        protected static final int CONSTRUCTOR = 1 << 8;
        protected static final int INDEX = 1 << 9;
        protected static final int NUMBER = 1 << 10;
        protected static final int PARAMETERS = 1 << 12;

        protected Segment child;
        protected int modifiers;
        protected String text;
        protected int arity;

        protected Segment(Segment child, int modifiers, String text, int arity) {
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

    protected static class AccessMachine {

        protected static class AccessState {

            protected final TokenState ws;

            protected AccessState(TokenState ws) {
                this.ws = ws;
            }

            protected int target = 0;
            Segment segment;

            int brackets = 0;
            int parens = 0;
            int braces = 0;
        }

        protected static final List<Function<AccessState, Integer>> astates;

        static {
            astates = new ArrayList<>();

            // 0
            astates.add(as -> {
                Token token = as.ws.tokens.get(as.ws.current);
                if (token.getType() == SuggestLexer.ID || token.getType() == SuggestLexer.TYPE) {
                    as.segment = new Segment(null, Segment.SUGGEST | Segment.TYPE | Segment.ID | Segment.CALL, token.getText(), -1);
                    return as.ws.current == 0 ? -2 : 1;
                } else if (token.getType() == SuggestLexer.LP) {
                    as.segment = new Segment(null, Segment.PARAMETERS, null, -1);
                    return 2;
                } else if (token.getType() == SuggestLexer.DOT || token.getType() == SuggestLexer.NSDOT) {
                    as.segment = new Segment(null, Segment.SUGGEST | Segment.FIELD | Segment.METHOD, null, -1);
                    return 4;
                } else if (token.getType() == SuggestLexer.DOTID) {
                    as.segment = new Segment(null, Segment.SUGGEST | Segment.FIELD | Segment.METHOD, token.getText(), -1);
                    return 3;
                }
                return -1;
            });
            // 1
            astates.add(as -> {
                Token token = as.ws.tokens.get(as.ws.current);
                if (token.getType() == SuggestLexer.TYPE || token.getType() == SuggestLexer.ATYPE) {
                    return -1;
                } else if (token.getType() == SuggestLexer.NEW) {
                    as.segment = new Segment(null, Segment.SUGGEST | Segment.TYPE, token.getText(), -1);
                }
                return -2;
            });
            // 2
            astates.add(as -> {
                Token token = as.ws.tokens.get(as.ws.current);
                if (token.getType() == SuggestLexer.TYPE) {
                    as.segment.modifiers |= Segment.CONSTRUCTOR;
                    as.segment.text = token.getText();
                    return 9;
                } else if (token.getType() == SuggestLexer.ID) {
                    as.segment.modifiers |= Segment.CALL;
                    as.segment.text = token.getText();
                    return -2;
                } else if (token.getType() == SuggestLexer.DOTID) {
                    as.segment.modifiers |= Segment.METHOD;
                    as.segment.text = token.getText();
                    return 3;
                }
                return -1;
            });
            // 3
            astates.add(as -> {
                Token token = as.ws.tokens.get(as.ws.current);
                if (token.getType() == SuggestLexer.DOT || token.getType() == SuggestLexer.NSDOT) {
                    return 4;
                }
                return -1;
            });
            // 4
            astates.add(as -> {
                Token token = as.ws.tokens.get(as.ws.current);
                if (token.getType() == SuggestLexer.ID) {
                    as.segment = new Segment(as.segment, Segment.RESOLVE | Segment.ID, token.getText(), -1);
                    return -2;
                } else if (token.getType() == SuggestLexer.TYPE || token.getType() == SuggestLexer.ATYPE) {
                    as.segment.modifiers |= Segment.STATIC;
                    as.segment = new Segment(as.segment, Segment.RESOLVE | Segment.TYPE, token.getText(), -1);
                    return -2;
                } else if (token.getType() == SuggestLexer.DOTID) {
                    as.segment = new Segment(as.segment, Segment.RESOLVE | Segment.FIELD, token.getText(), -1);
                    return 3;
                } else if (token.getType() == SuggestLexer.DOTINTEGER) {
                    as.segment = new Segment(as.segment, Segment.RESOLVE | Segment.NUMBER, token.getText(), -1);
                    return 3;
                } else if (token.getType() == SuggestLexer.RBRACE) {
                    as.segment = new Segment(as.segment, Segment.RESOLVE | Segment.INDEX, null, -1);
                    return 5;
                } else if (token.getType() == SuggestLexer.RP) {
                    as.segment = new Segment(as.segment, Segment.RESOLVE, null, 0);
                    return 7;
                }
                return -1;
            });
            // 5
            astates.add(as -> {
                Token token = as.ws.tokens.get(as.ws.current);
                if (token.getType() == SuggestLexer.LBRACE) {
                    if (as.brackets == 0 && as.parens == 0 && as.braces == 0) {
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
                    return -1;
                }

                return 5;
            });
            // 6
            astates.add(as -> {
                Token token = as.ws.tokens.get(as.ws.current);
                if (token.getType() == SuggestLexer.ID) {
                    as.segment = new Segment(as.segment, Segment.RESOLVE | Segment.ID, token.getText(), -1);
                    return -2;
                } else if (token.getType() == SuggestLexer.DOTID) {
                    as.segment = new Segment(as.segment, Segment.RESOLVE | Segment.FIELD, token.getText(), -1);
                    return 3;
                } else if (token.getType() == SuggestLexer.RBRACE) {
                    as.segment = new Segment(as.segment, Segment.RESOLVE | Segment.INDEX, null, -1);
                    return 5;
                } else if (token.getType() == SuggestLexer.RP) {
                    as.segment = new Segment(as.segment, Segment.RESOLVE, null, 0);
                    return 7;
                }
                return -1;
            });
            // 7
            astates.add(as -> {
                Token token = as.ws.tokens.get(as.ws.current);
                if (token.getType() == SuggestLexer.LP) {
                    if (as.brackets == 0 && as.parens == 0 && as.braces == 0) {
                        return 8;
                    } else {
                        --as.parens;
                    }
                } else if (token.getType() == SuggestLexer.COMMA) {
                    if (as.brackets == 0 && as.parens == 0 && as.braces == 0) {
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
                    return -1;
                }

                if (as.segment.arity == 0) {
                    as.segment.arity = 1;
                }

                return 7;
            });
            // 8
            astates.add(as -> {
                Token token = as.ws.tokens.get(as.ws.current);
                if (token.getType() == SuggestLexer.ID) {
                    as.segment.modifiers |= Segment.CALL;
                    as.segment.text = token.getText();
                    return -2;
                } else if (token.getType() == SuggestLexer.DOTID) {
                    as.segment.modifiers |= Segment.METHOD;
                    as.segment.text = token.getText();
                    return 3;
                } else if (token.getType() == SuggestLexer.TYPE) {
                    as.segment.modifiers |= Segment.CONSTRUCTOR;
                    as.segment.text = token.getText();
                    return 9;
                }
                return -1;
            });
            // 9
            astates.add(as -> {
                Token token = as.ws.tokens.get(as.ws.current);
                if (token.getType() == SuggestLexer.NEW) {
                    return -2;
                }
                return -1;
            });
        }

        protected static void walk(AccessMachine.AccessState as, StringBuilder debug) {
            TokenState ws = as.ws;
            ws.current = ws.tokens.size() - 1;

            while (ws.current >= 0) {
                Function<AccessMachine.AccessState, Integer> state = astates.get(as.target);
                as.target = state.apply(as);
                //debug.append(as.target);
                //debug.append(":");
                //debug.append(as.segment.toString());
                //debug.append("\n");

                if (as.target < 0) {
                    break;
                }

                --ws.current;
            }
        }

        protected AccessMachine() {

        }
    }

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

    protected List<Suggestion> collect(Map<String, String> functions, Map<String, String> variables, Segment segment) {

        Class<?> resolved = null;

        while (segment.child != null && (segment.modifiers & Segment.RESOLVE) == Segment.RESOLVE) {

            if ((segment.modifiers & Segment.ID) == Segment.ID) {
                String type = variables.get(segment.text);
                resolved = type == null ? null : lookup.canonicalTypeNameToType(type);
            } else if ((segment.modifiers & Segment.TYPE) == Segment.TYPE) {
                resolved = segment.text == null ? null : lookup.canonicalTypeNameToType(segment.text);
            } else if ((segment.modifiers & Segment.CALL) == Segment.CALL) {
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
            } else if ((segment.modifiers & Segment.METHOD) == Segment.METHOD) {
                PainlessMethod pm = lookup.lookupPainlessMethod(
                        resolved, (segment.modifiers & Segment.STATIC) == Segment.STATIC, segment.text, segment.arity);
                resolved = pm == null ? null : pm.returnType;
            } else if ((segment.modifiers & Segment.FIELD) == Segment.FIELD) {
                PainlessField pf = lookup.lookupPainlessField(
                        resolved, (segment.modifiers & Segment.STATIC) == Segment.STATIC, segment.text);
                resolved = pf == null ? null : pf.typeParameter;
            } else if ((segment.modifiers & Segment.CONSTRUCTOR) == Segment.CONSTRUCTOR) {
                resolved = lookup.canonicalTypeNameToType(segment.text);
            } else if ((segment.modifiers & Segment.INDEX) == Segment.INDEX) {
                if (resolved != null) {
                    if (resolved.isArray()) {
                        resolved = resolved.getComponentType();
                    } else if (List.class.isAssignableFrom(resolved) || Map.class.isAssignableFrom(resolved)) {
                        resolved = def.class;
                    }
                }
            } else if ((segment.modifiers & Segment.NUMBER) == Segment.NUMBER) {
                resolved = def.class;
            }

            if (resolved == null || resolved == def.class) {
                return new ArrayList<>();
            } else if (resolved.isPrimitive()) {
                resolved = PainlessLookupUtility.typeToBoxedType(resolved);
            }

            segment = segment.child;
        }

        List<Suggestion> suggestions = new ArrayList<>();

        if (segment.child == null && (segment.modifiers & Segment.SUGGEST) == Segment.SUGGEST) {

            if ((segment.modifiers & Segment.ID) == Segment.ID) {
                for (String name : variables.keySet()) {
                    if (name.startsWith(segment.text)) {
                        suggestions.add(new Suggestion("variable", name));
                    }
                }
            }

            if ((segment.modifiers & Segment.TYPE) == Segment.TYPE) {
                for (String type : lookup.getCanonicalClassNames()) {
                    if (type.startsWith(segment.text)) {
                        suggestions.add(new Suggestion("type", type));
                    }
                }
            }

            if ((segment.modifiers & Segment.CALL) == Segment.CALL) {
                for (String call : functions.keySet()) {
                    if (call.startsWith(segment.text)) {
                        suggestions.add(new Suggestion("user", call));
                    }
                }

                for (String call : lookup.getImportedPainlessMethodsKeys()) {
                    if (call.startsWith(segment.text)) {
                        suggestions.add(new Suggestion("call", call));
                    }
                }

                for (String call : lookup.getPainlessClassBindingsKeys()) {
                    if (call.startsWith(segment.text)) {
                        suggestions.add(new Suggestion("call", call));
                    }
                }

                for (String call : lookup.getPainlessInstanceBindingsKeys()) {
                    if (call.startsWith(segment.text)) {
                        suggestions.add(new Suggestion("call", call));
                    }
                }
            }

            if ((segment.modifiers & Segment.METHOD) == Segment.METHOD) {
                if (resolved != null) {
                    PainlessClass pc = lookup.lookupPainlessClass(resolved);

                    if (pc != null) {
                        if ((segment.modifiers & Segment.STATIC) == Segment.STATIC) {
                            for (String method : pc.staticMethods.keySet()) {
                                if (segment.text == null || method.startsWith(segment.text)) {
                                    suggestions.add(new Suggestion("method", method));
                                }
                            }
                        } else {
                            for (String method : pc.methods.keySet()) {
                                if (segment.text == null || method.startsWith(segment.text)) {
                                    suggestions.add(new Suggestion("method", method));
                                }
                            }
                        }
                    }
                }
            }

            if ((segment.modifiers & Segment.FIELD) == Segment.FIELD) {
                if (resolved != null) {
                    PainlessClass pc = lookup.lookupPainlessClass(resolved);

                    if (pc != null) {
                        if ((segment.modifiers & Segment.STATIC) == Segment.STATIC) {
                            for (String field : pc.staticFields.keySet()) {
                                if (segment.text == null || field.startsWith(segment.text)) {
                                    suggestions.add(new Suggestion("field", field));
                                }
                            }
                        } else {
                            for (String field : pc.fields.keySet()) {
                                if (segment.text == null || field.startsWith(segment.text)) {
                                    suggestions.add(new Suggestion("field", field));
                                }
                            }
                        }
                    }
                }
            }
        } else if ((segment.modifiers & Segment.PARAMETERS) == Segment.PARAMETERS) {
            if ((segment.modifiers & Segment.CONSTRUCTOR) == Segment.CONSTRUCTOR) {
                PainlessClass pc = lookup.lookupPainlessClass(lookup.canonicalTypeNameToType(segment.text));

                if (pc != null) {
                    for (PainlessConstructor pcon : pc.constructors.values()) {
                        StringBuilder parameters = new StringBuilder(" ");
                        for (Class<?> type : pcon.typeParameters) {
                            parameters.append(PainlessLookupUtility.typeToCanonicalTypeName(type));
                            parameters.append(" ");
                        }
                        suggestions.add(new Suggestion("parameters", parameters.toString()));
                    }
                }
            }

            /*if ((segment.modifiers & Segment.CALL) == Segment.CALL) {
                for (String call : functions.keySet()) {
                    if (call.substring(0, call.length() - 2).equals(segment.text)) {
                        StringBuilder parameters = new StringBuilder(" ");
                        for (Class<?> type : pcon.typeParameters) {
                            parameters.append(PainlessLookupUtility.typeToCanonicalTypeName(type));
                            parameters.append(" ");
                        }
                        suggestions.add(new Suggestion("parameters", parameters.toString()));
                    }
                }
            }*/

            if ((segment.modifiers & Segment.METHOD) == Segment.METHOD) {
                if (resolved != null) {
                    PainlessClass pc = lookup.lookupPainlessClass(resolved);

                    if (pc != null) {
                        if ((segment.modifiers & Segment.STATIC) == Segment.STATIC) {
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

    protected List<Suggestion> suggest() {
        FunctionMachine.FunctionState fs = new FunctionMachine.FunctionState(new TokenState(this.tokens));
        FunctionMachine.walk(fs);

        FunctionMachine.FunctionData fd = fs.functions.isEmpty() ? null : fs.functions.get(fs.functions.size() - 1);
        int start = 0;
        if (fd != null) {
            if (fd.bodyEndToken == -1) {
                start = fd.bodyStartToken;
            } else {
                start = fd.bodyEndToken + 2;
            }
        }

        StringBuilder segments = new StringBuilder();

        List<? extends Token> tokens = this.tokens.subList(start, this.tokens.size());

        LambdaMachine.LambdaState ls = new LambdaMachine.LambdaState(new TokenState(tokens));
        LambdaMachine.walk(ls);

        Map<Integer, LambdaMachine.LambdaData> mld = new HashMap<>();

        for (LambdaMachine.LambdaData ld : ls.lambdas) {
            mld.put(ld.headerStartToken, ld);
        }

        StringBuilder builder = new StringBuilder();
        BlockMachine.BlockState bs = new BlockMachine.BlockState(new TokenState(tokens), mld);
        BlockMachine.walk(bs, builder);

        AccessMachine.AccessState as = new AccessMachine.AccessState(new TokenState(tokens));
        AccessMachine.walk(as, builder);

        List<Suggestion> suggestions = new ArrayList<>();

        if (as.target == -2) {
            Map<String, String> functions = new HashMap<>();

            for (FunctionMachine.FunctionData function : fs.functions) {
                functions.put(function.functionName + "/" + function.parameterTypes.size(), function.returnType);
            }

            Map<String, String> variables = bs.scope.getVariables();

            if (fd == null || fd.bodyEndToken != -1) {
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
                if (fd.parameterNames.size() == fd.parameterTypes.size()) {
                    for (int parameter = 0; parameter < fd.parameterNames.size(); ++parameter) {
                        variables.put(fd.parameterNames.get(parameter), fd.parameterTypes.get(parameter));
                    }
                }
            }

            suggestions = collect(functions, variables, as.segment);

            // DEBUG
            suggestions.add(new Suggestion("variables", variables.toString()));
            //segments.append(variables);
            // END DEBUG
        }

        // DEBUG
        //suggestions.add(new Suggestion("debug", "\n\n" + variables));
        // END DEBUG

        return suggestions;
    }
}
