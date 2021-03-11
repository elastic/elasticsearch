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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.painless.antlr.EnhancedSuggestLexer;
import org.elasticsearch.painless.antlr.SuggestLexer;
import org.elasticsearch.painless.lookup.PainlessClass;
import org.elasticsearch.painless.lookup.PainlessClassBinding;
import org.elasticsearch.painless.lookup.PainlessField;
import org.elasticsearch.painless.lookup.PainlessInstanceBinding;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class PainlessSuggest {

    public static class Suggestion implements Writeable, ToXContentObject {

        private static final ParseField TYPE_FIELD = new ParseField("type");
        private static final ParseField TEXT_FIELD = new ParseField("text");

        private final String type;
        private final String text;

        public Suggestion(String type, String text) {
            this.type = type;
            this.text = text;
        }

        public Suggestion(StreamInput in) throws IOException {
            this.type = in.readString();
            this.text = in.readString();
        }

        public String getType() {
            return type;
        }

        public String getText() {
            return text;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.type);
            out.writeString(this.text);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TYPE_FIELD.getPreferredName(), type);
            builder.field(TEXT_FIELD.getPreferredName(), text);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Suggestion that = (Suggestion)o;
            return Objects.equals(type, that.type) && Objects.equals(text, that.text);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, text);
        }

        @Override
        public String toString() {
            return "Suggestion{" +
                    "type='" + type + '\'' +
                    ", text='" + text + '\'' +
                    '}';
        }
    }

    private static class WalkState {

        private final List<? extends Token> tokens;

        private WalkState(List<? extends Token> tokens) {
            this.tokens = Collections.unmodifiableList(tokens);
        }

        private int current = 0;
    }

    private static class FunctionMachine {

        private static class FunctionData {
            private String returnType = "";
            private String functionName = "";
            private final List<String> parameterTypes = new ArrayList<>();
            private final List<String> parameterNames = new ArrayList<>();

            private int bodyStartToken = -1;
            private int bodyEndToken = -1;

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

        private static class FunctionState {

            private final WalkState ws;

            private FunctionState(WalkState ws) {
                this.ws = ws;
            }

            private int target = 0;

            private String returnType;
            private String functionName;
            private String parameterType;
            private FunctionData functionData;

            private final List<FunctionData> functions = new ArrayList<>();

            private int brackets;
        }

        private static final List<Function<FunctionState, Integer>> fstates;

        static {
            fstates = new ArrayList<>();
            // 0 - possible start of function
            fstates.add(fs -> {
                Token token = fs.ws.tokens.get(fs.ws.current);
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
                Token token = fs.ws.tokens.get(fs.ws.current);
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
                Token token = fs.ws.tokens.get(fs.ws.current);
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
                Token token = fs.ws.tokens.get(fs.ws.current);
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
                    fs.functionData.bodyStartToken = fs.ws.current + 1;
                    return 5;
                }
                // ERROR (ignore): unexpected token, keep looking for a sentinel
                return 3;
            });
            // 4 - start of function body
            fstates.add(fs -> {
                Token token = fs.ws.tokens.get(fs.ws.current);
                if (token.getType() == SuggestLexer.LBRACK) {
                    // VALID: found start of function body
                    fs.brackets = 1;
                    fs.functionData.bodyStartToken = fs.ws.current + 1;
                    return 5;
                }
                // ERROR (ignore): unexpected token, keep looking for a sentinel
                return 4;
            });
            // 5 - possible end of function body
            fstates.add(fs -> {
                Token token = fs.ws.tokens.get(fs.ws.current);
                if (token.getType() == SuggestLexer.LBRACK) {
                    // VALID: increase scope
                    ++fs.brackets;
                } else if (token.getType() == SuggestLexer.RBRACK) {
                    // VALID: decrease scope
                    --fs.brackets;
                    if (fs.brackets == 0) {
                        // VALID: end of function body
                        fs.functionData.bodyEndToken = fs.ws.current - 1;
                        return 0;
                    }
                }
                // VALID: keep looking for end of function body
                return 5;
            });
            // 6 - parameter name
            fstates.add(fs -> {
                Token token = fs.ws.tokens.get(fs.ws.current);
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
                Token token = fs.ws.tokens.get(fs.ws.current);
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
                Token token = fs.ws.tokens.get(fs.ws.current);
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

        private static void walk(FunctionState fs) {
            WalkState ws = fs.ws;

            while (ws.current < ws.tokens.size()) {
                Function<FunctionState, Integer> state = fstates.get(fs.target);
                fs.target = state.apply(fs);
                ++ws.current;
            }
        }

        private FunctionMachine() {

        }
    }

    private static class LambdaMachine {

        private static class LambdaData {
            private final List<String> parameterTypes = new ArrayList<>();
            private final List<String> parameterNames = new ArrayList<>();

            private int headerStartToken = -1;
            private int headerEndToken = -1;

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

        private static class LambdaState {

            private final WalkState ws;

            private LambdaState(WalkState ws) {
                this.ws = ws;
            }

            private int target = 0;

            private LambdaData lambdaData;

            private final List<LambdaMachine.LambdaData> lambdas = new ArrayList<>();
        }

        private static final List<Function<LambdaMachine.LambdaState, Integer>> lstates;

        static {
            lstates = new ArrayList<>();

            // 0
            lstates.add(ls -> {
                Token token = ls.ws.tokens.get(ls.ws.current);
                if (token.getType() == SuggestLexer.ARROW) {
                    ls.lambdaData = new LambdaData();
                    ls.lambdas.add(ls.lambdaData);
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    ls.lambdaData.headerEndToken = ls.ws.current;
                    return 1;
                }
                return 0;
            });
            // 1
            lstates.add(ls -> {
                Token token = ls.ws.tokens.get(ls.ws.current);
                if (token.getType() == SuggestLexer.ID) {
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    ls.lambdaData.parameterTypes.add("def");
                    ls.lambdaData.parameterNames.add(token.getText());
                } else if (token.getType() == SuggestLexer.ARROW) {
                    ls.lambdaData = new LambdaData();
                    ls.lambdas.add(ls.lambdaData);
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    ls.lambdaData.headerEndToken = ls.ws.current;
                    return 1;
                } else if (token.getType() == SuggestLexer.RP) {
                    return 2;
                }
                return 0;
            });
            // 2
            lstates.add(ls -> {
                Token token = ls.ws.tokens.get(ls.ws.current);
                if (token.getType() == SuggestLexer.LP) {
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    return 0;
                } else if (token.getType() == SuggestLexer.ID) {
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    ls.lambdaData.parameterTypes.add("def");
                    ls.lambdaData.parameterNames.add(token.getText());
                    return 3;
                } else if (token.getType() == SuggestLexer.ARROW) {
                    ls.lambdaData = new LambdaData();
                    ls.lambdas.add(ls.lambdaData);
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    ls.lambdaData.headerEndToken = ls.ws.current;
                    return 1;
                }
                return 0;
            });
            // 3
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

        private static void walk(LambdaMachine.LambdaState ls) {
            WalkState ws = ls.ws;
            ws.current = ws.tokens.size() - 1;

            while (ws.current >= 0) {
                Function<LambdaMachine.LambdaState, Integer> state = lstates.get(ls.target);
                ls.target = state.apply(ls);
                --ws.current;
            }
        }

        private LambdaMachine() {

        }
    }

    private static class BlockMachine {

        private static class BlockState {

            private static class BlockScope {

                private final BlockScope parent;

                private int type;
                private int sentinel;
                private boolean pop = false;
                private int parens = 0;
                private int braces = 0;

                private final Map<String, String> variables = new HashMap<>();
                private int decltarget = 0;
                private String decltype = null;
                private int declparens = 0;
                private int declbraces = 0;

                private BlockScope(BlockScope parent, int type, int sentinel) {
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

            private final WalkState ws;
            private final Map<Integer, LambdaMachine.LambdaData> mld;

            private BlockScope scope = new BlockScope(null, SuggestLexer.EOF, SuggestLexer.EOF);

            private BlockState(WalkState ws, Map<Integer, LambdaMachine.LambdaData> mld) {
                this.ws = ws;
                this.mld = mld;
            }
        }

        private static final List<Function<BlockState, Integer>> declstates;

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

        private static void scope(BlockState bs, StringBuilder builder) {
            WalkState ws = bs.ws;

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

        private static void walk(BlockState bs, StringBuilder builder) {
            WalkState ws = bs.ws;

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

        private BlockMachine() {

        }
    }

    private static class Segment {

        private static final int RESOLVE = 1 << 0;
        private static final int SUGGEST = 1 << 1;
        private static final int ID = 1 << 2;
        private static final int TYPE = 1 << 3;
        private static final int CALL = 1 << 4;
        private static final int STATIC = 1 << 5;
        private static final int METHOD = 1 << 6;
        private static final int FIELD = 1 << 7;
        private static final int CONSTRUCTOR = 1 << 8;
        private static final int INDEX = 1 << 9;
        private static final int NUMBER = 1 << 10;
        private static final int PARAMETERS = 1 << 12;

        private Segment child;
        private int modifiers;
        private String text;
        private int arity;

        private Segment(Segment child, int modifiers, String text, int arity) {
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

    private static class AccessMachine {

        private static class AccessState {

            private final WalkState ws;

            private AccessState(WalkState ws) {
                this.ws = ws;
            }

            private int target = 0;
            Segment segment;

            int brackets = 0;
            int parens = 0;
            int braces = 0;
        }

        private static final List<Function<AccessState, Integer>> astates;

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

        private static void walk(AccessMachine.AccessState as, StringBuilder debug) {
            WalkState ws = as.ws;
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

        private AccessMachine() {

        }
    }

    public static List<Suggestion> suggest(PainlessLookup lookup, String source) {
        ANTLRInputStream stream = new ANTLRInputStream(source);
        SuggestLexer lexer = new EnhancedSuggestLexer(stream, lookup);
        lexer.removeErrorListeners();

        return new PainlessSuggest(lookup, lexer.getAllTokens()).suggest();
    }

    private final PainlessLookup lookup;
    private final List<? extends Token> tokens;

    private PainlessSuggest(PainlessLookup lookup, List<? extends Token> tokens) {
        this.lookup = Objects.requireNonNull(lookup);
        this.tokens = Collections.unmodifiableList(Objects.requireNonNull(tokens));
    }

    private List<Suggestion> collect(Map<String, String> functions, Map<String, String> variables, Segment segment) {

        Class<?> resolved = null;

        while (segment.child != null && (segment.modifiers & Segment.RESOLVE) == Segment.RESOLVE) {

            if ((segment.modifiers & Segment.ID) == Segment.ID) {
                String type = variables.get(segment.text);
                resolved = type == null ? null : lookup.canonicalTypeNameToType(type);
            } else if ((segment.modifiers & Segment.TYPE) == Segment.TYPE) {
                resolved = segment.text == null ? null : lookup.canonicalTypeNameToType(segment.text);
            } else if ((segment.modifiers & Segment.CALL) == Segment.CALL) {
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
                return Collections.emptyList();
            } else if (resolved.isPrimitive()) {
                resolved = PainlessLookupUtility.typeToBoxedType(resolved);
            }

            segment = segment.child;
        }

        List<Suggestion> suggestions = Collections.emptyList();

        if (segment.child == null && (segment.modifiers & Segment.SUGGEST) == Segment.SUGGEST) {
            suggestions = new ArrayList<>();

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
        }

        return suggestions;
    }

    private List<Suggestion> suggest() {
        FunctionMachine.FunctionState fs = new FunctionMachine.FunctionState(new WalkState(this.tokens));
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

        segments.append(tokens);

        LambdaMachine.LambdaState ls = new LambdaMachine.LambdaState(new WalkState(tokens));
        LambdaMachine.walk(ls);

        Map<Integer, LambdaMachine.LambdaData> mld = new HashMap<>();

        for (LambdaMachine.LambdaData ld : ls.lambdas) {
            mld.put(ld.headerStartToken, ld);
        }

        StringBuilder builder = new StringBuilder();
        BlockMachine.BlockState bs = new BlockMachine.BlockState(new WalkState(tokens), mld);
        BlockMachine.walk(bs, builder);

        AccessMachine.AccessState as = new AccessMachine.AccessState(new WalkState(tokens));
        AccessMachine.walk(as, builder);

        List<Suggestion> suggestions = new ArrayList<>();

        if (as.target == -2) {
            Map<String, String> functions = new HashMap<>();

            for (FunctionMachine.FunctionData function : fs.functions) {
                functions.put(function.functionName + "/" + function.parameterTypes.size(), function.returnType);
            }

            suggestions = collect(functions, bs.scope.getVariables(), as.segment);

            // DEBUG
            segments.append(functions);
            // END DEBUG
        }

        // DEBUG
        suggestions.add(new Suggestion("debug", "\n\n" + segments.toString()));
        // END DEBUG

        return suggestions;
    }
}
