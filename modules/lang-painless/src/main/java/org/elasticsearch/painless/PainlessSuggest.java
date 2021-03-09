/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

public class PainlessSuggest {

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
                if (token.getType() == PainlessLexer.ATYPE || token.getType() == PainlessLexer.TYPE) {
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
                if (token.getType() == PainlessLexer.ID) {
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
                if (token.getType() == PainlessLexer.LP) {
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
                if (token.getType() == PainlessLexer.ATYPE || token.getType() == PainlessLexer.TYPE) {
                    // VALID: found a parameter type
                    fs.parameterType = token.getText();
                    return 6;
                } else if (token.getType() == PainlessLexer.RP) {
                    // VALID: end of function header
                    return 4;
                } else if (token.getType() == PainlessLexer.LBRACK) {
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
                if (token.getType() == PainlessLexer.LBRACK) {
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
                if (token.getType() == PainlessLexer.LBRACK) {
                    // VALID: increase scope
                    ++fs.brackets;
                } else if (token.getType() == PainlessLexer.RBRACK) {
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
                if (token.getType() == PainlessLexer.ID) {
                    // VALID: found a parameter name, record parameter type and name
                    fs.functionData.parameterTypes.add(fs.parameterType);
                    fs.functionData.parameterNames.add(token.getText());
                    return 7;
                } else if (token.getType() == PainlessLexer.RP) {
                    // ERROR (process): missing parameter name, but found end of function header
                    return 4;
                } else if (token.getType() == PainlessLexer.LBRACK) {
                    // ERROR (process): missing parameter name, but found start of function body
                    return 5;
                }
                // ERROR (ignore): unexpected token, keep looking for a sentinel
                return 6;
            });
            // 7 - start of another parameter or end of parameters
            fstates.add(fs -> {
                Token token = fs.ws.tokens.get(fs.ws.current);
                if (token.getType() == PainlessLexer.COMMA) {
                    // VALID: found comma, look for another parameter
                    return 8;
                } else if (token.getType() == PainlessLexer.RP) {
                    // VALID: end of function header
                    return 4;
                } else if (token.getType() == PainlessLexer.LBRACK) {
                    // ERROR (process): missing comma or right parenthesis, but found start of function body
                    return 5;
                }
                // ERROR (ignore): unexpected token, keep looking for a sentinel
                return 7;
            });
            // 8 - start of another parameter
            fstates.add(fs -> {
                Token token = fs.ws.tokens.get(fs.ws.current);
                if (token.getType() == PainlessLexer.ATYPE || token.getType() == PainlessLexer.TYPE) {
                    // VALID: found a parameter type
                    fs.parameterType = token.getText();
                    return 6;
                } else if (token.getType() == PainlessLexer.RP) {
                    // ERROR (process): missing parameter type, but found end of function header
                    return 4;
                } else if (token.getType() == PainlessLexer.LBRACK) {
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
                if (token.getType() == PainlessLexer.ARROW) {
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
                if (token.getType() == PainlessLexer.ID) {
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    ls.lambdaData.parameterTypes.add("def");
                    ls.lambdaData.parameterNames.add(token.getText());
                } else if (token.getType() == PainlessLexer.ARROW) {
                    ls.lambdaData = new LambdaData();
                    ls.lambdas.add(ls.lambdaData);
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    ls.lambdaData.headerEndToken = ls.ws.current;
                    return 1;
                } else if (token.getType() == PainlessLexer.RP) {
                    return 2;
                }
                return 0;
            });
            // 2
            lstates.add(ls -> {
                Token token = ls.ws.tokens.get(ls.ws.current);
                if (token.getType() == PainlessLexer.LP) {
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    return 0;
                } else if (token.getType() == PainlessLexer.ID) {
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    ls.lambdaData.parameterTypes.add("def");
                    ls.lambdaData.parameterNames.add(token.getText());
                    return 3;
                } else if (token.getType() == PainlessLexer.ARROW) {
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
                if (token.getType() == PainlessLexer.LP) {
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    return 0;
                } else if (token.getType() == PainlessLexer.ATYPE || token.getType() == PainlessLexer.TYPE) {
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    ls.lambdaData.parameterTypes.set(ls.lambdaData.parameterTypes.size() - 1, token.getText());
                    return 4;
                } else if (token.getType() == PainlessLexer.COMMA) {
                    return 2;
                } else if (token.getType() == PainlessLexer.ARROW) {
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
                if (token.getType() == PainlessLexer.LP) {
                    ls.lambdaData.headerStartToken = ls.ws.current;
                    return 0;
                } if (token.getType() == PainlessLexer.COMMA) {
                    return 2;
                } else if (token.getType() == PainlessLexer.ARROW) {
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

                public String toString() {
                    StringBuilder builder = new StringBuilder();
                    builder.append("[");
                    builder.append(type == -1 ? "EOF" : PainlessLexer.ruleNames[type - 1]);
                    builder.append(" : ");
                    builder.append(sentinel == -1 ? "EOF" : PainlessLexer.ruleNames[sentinel - 1]);
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

            //private final PainlessLookup lookup;
            private final WalkState ws;
            private final Map<Integer, LambdaMachine.LambdaData> mld;

            private BlockScope scope = new BlockScope(null, PainlessLexer.EOF, PainlessLexer.EOF);

            private BlockState(/*PainlessLookup lookup, */WalkState ws, Map<Integer, LambdaMachine.LambdaData> mld) {
                //this.lookup = Objects.requireNonNull(lookup);
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
                if (token.getType() == PainlessLexer.ATYPE || token.getType() == PainlessLexer.TYPE) {
                    bs.scope.decltype = token.getText();
                    return 1;
                } else if (token.getType() == PainlessLexer.IN) {
                    Token prev = bs.ws.current > 0 ? bs.ws.tokens.get(bs.ws.current - 1) : null;

                    if (prev != null && prev.getType() == PainlessLexer.ID) {
                        bs.scope.variables.put(prev.getText(), "def");
                    }
                }
                return 0;
            });
            // 1
            declstates.add(bs -> {
                Token token = bs.ws.tokens.get(bs.ws.current);
                if (token.getType() == PainlessLexer.ATYPE || token.getType() == PainlessLexer.TYPE) {
                    bs.scope.decltype = token.getText();
                    return 1;
                } else if (token.getType() == PainlessLexer.ID) {
                    bs.scope.variables.put(token.getText(), bs.scope.decltype);
                    return 2;
                }
                return 0;
            });
            // 2
            declstates.add(bs -> {
                Token token = bs.ws.tokens.get(bs.ws.current);
                if (token.getType() == PainlessLexer.COMMA) {
                    return 1;
                } else if (token.getType() == PainlessLexer.ASSIGN) {
                    return 3;
                } else if (token.getType() == PainlessLexer.ATYPE || token.getType() == PainlessLexer.TYPE) {
                    bs.scope.decltype = token.getText();
                    return 1;
                }
                return 0;
            });
            // 3
            declstates.add(bs -> {
                Token token = bs.ws.tokens.get(bs.ws.current);
                if (token.getType() == PainlessLexer.COMMA && bs.scope.declparens == 0 && bs.scope.declbraces == 0) {
                    return 1;
                } else if (token.getType() == PainlessLexer.LP) {
                    ++bs.scope.declparens;
                    return 3;
                } else if (token.getType() == PainlessLexer.RP) {
                    --bs.scope.declparens;
                    return 3;
                } else if (token.getType() == PainlessLexer.LBRACE) {
                    ++bs.scope.declbraces;
                    return 3;
                } else if (token.getType() == PainlessLexer.RBRACE) {
                    --bs.scope.declbraces;
                } else if (token.getType() == PainlessLexer.SEMICOLON) {
                    return 0;
                }
                return 3;
            });
        }

        private static void scope(BlockState bs, StringBuilder builder) {
            WalkState ws = bs.ws;

            int token = ws.tokens.get(ws.current).getType();
            int prev = ws.current > 0 ? ws.tokens.get(ws.current - 1).getType() : PainlessLexer.EOF;

            if (bs.scope.pop) {
                if (token == PainlessLexer.CATCH && (bs.scope.type == PainlessLexer.TRY || bs.scope.type == PainlessLexer.CATCH)) {
                    bs.scope = bs.scope.parent;
                } else if (token == PainlessLexer.ELSE) {
                    while (bs.scope.type != PainlessLexer.IF && bs.scope.sentinel == PainlessLexer.SEMICOLON) {
                        bs.scope = bs.scope.parent;
                    }

                    if (bs.scope.type == PainlessLexer.IF) {
                        bs.scope = bs.scope.parent;
                    }
                } else {
                    bs.scope = bs.scope.parent;

                    while (bs.scope.sentinel == PainlessLexer.SEMICOLON) {
                        bs.scope = bs.scope.parent;
                    }
                }
            }

            LambdaMachine.LambdaData ld = bs.mld.get(ws.current);

            if (ld != null) {
                bs.scope = new BlockState.BlockScope(bs.scope, PainlessLexer.ARROW, PainlessLexer.EOF);

                for (int param = 0; param < ld.parameterTypes.size(); ++param) {
                    bs.scope.variables.put(ld.parameterNames.get(param), ld.parameterTypes.get(param));
                }

                ws.current = ld.headerEndToken;
                token = PainlessLexer.ARROW;
            } else if (token == PainlessLexer.WHILE || token == PainlessLexer.IF || token == PainlessLexer.ELSE) {
                if (prev == PainlessLexer.ELSE && token == PainlessLexer.IF) {
                    bs.scope.type = PainlessLexer.IF;
                } else {
                    bs.scope = new BlockState.BlockScope(bs.scope, token, PainlessLexer.SEMICOLON);
                }
            } else if (token == PainlessLexer.FOR) {
                bs.scope = new BlockState.BlockScope(bs.scope, token, PainlessLexer.RP);
            } else if (token == PainlessLexer.DO || token == PainlessLexer.TRY || token == PainlessLexer.CATCH) {
                bs.scope = new BlockState.BlockScope(bs.scope, token, PainlessLexer.RBRACK);
            } else if (token == PainlessLexer.LBRACK) {
                if (bs.scope.sentinel == PainlessLexer.SEMICOLON || bs.scope.sentinel == PainlessLexer.RP) {
                    bs.scope.sentinel = PainlessLexer.RBRACK;
                }
            } else if (token == PainlessLexer.LP) {
                ++bs.scope.parens;
            } else if (token == PainlessLexer.RP) {
                bs.scope.parens = Math.max(0, bs.scope.parens - 1);

                if (bs.scope.sentinel == PainlessLexer.RP && bs.scope.parens == 0) {
                    bs.scope.sentinel = PainlessLexer.SEMICOLON;
                }
            } else if (token == PainlessLexer.LBRACE) {
                ++bs.scope.braces;
            } else if (token == PainlessLexer.RBRACE) {
                bs.scope.braces = Math.max(0, bs.scope.braces - 1);
            }

            if (bs.scope.type == PainlessLexer.ARROW) {
                if (token == PainlessLexer.COMMA || token == PainlessLexer.RP && bs.scope.parens == 0 && bs.scope.braces == 0) {
                    bs.scope = bs.scope.parent;
                }
            } else if (token == bs.scope.sentinel) {
                if (bs.scope.type == PainlessLexer.DO) {
                    bs.scope.type = PainlessLexer.WHILE;
                    bs.scope.sentinel = PainlessLexer.SEMICOLON;
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
                    builder.append(token == -1 ? "EOF" : PainlessLexer.ruleNames[token - 1]);
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

    private static class AccessMachine {

        private static class Segment {

            private static final int NEW = 1;
            private static final int ID = 2;
            private static final int TYPE = 3;
            private static final int OPEN = 4;
            private static final int FIELD = 5;
            private static final int CALL = 6;
            private static final int CONSTRUCTOR = 7;
            private static final int INDEX = 7;
            private static final int NUMBER = 8;
            private static final int DOT = 9;

            private Segment child;
            private int type;
            private String id;
            private int arity;

            private Segment(Segment child, int type, String id, int arity) {
                this.child = child;
                this.type = type;
                this.id = id;
                this.arity = arity;
            }

            @Override
            public String toString() {
                return "Segment{" +
                        "child=" + child +
                        ", type=" + type +
                        ", id='" + id + '\'' +
                        ", arity=" + arity +
                        '}';
            }
        }

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
                if (token.getType() == PainlessLexer.ID || token.getType() == PainlessLexer.TYPE) {
                    as.segment = new Segment(null, Segment.ID, token.getText(), -1);
                    return as.ws.current == 0 ? -2 : 1;
                } else if (token.getType() == PainlessLexer.LP) {
                    as.segment = new Segment(null, Segment.OPEN, null, -1);
                    return 2;
                } else if (token.getType() == PainlessLexer.DOT || token.getType() == PainlessLexer.NSDOT) {
                    as.segment = new Segment(null, Segment.DOT, token.getText(), -1);
                    return 4;
                } else if (token.getType() == PainlessLexer.DOTID) {
                    as.segment = new Segment(null, Segment.FIELD, token.getText(), -1);
                    return 3;
                } else if (token.getType() == PainlessLexer.DOTINTEGER) {
                    as.segment = new Segment(null, Segment.NUMBER, token.getText(), -1);
                    return 3;
                }
                return -1;
            });
            // 1
            astates.add(as -> {
                Token token = as.ws.tokens.get(as.ws.current);
                if (token.getType() == PainlessLexer.TYPE || token.getType() == PainlessLexer.ATYPE) {
                    return -1;
                } else if (token.getType() == PainlessLexer.NEW) {
                    as.segment.type = Segment.NEW;
                }
                return -2;
            });
            // 2
            astates.add(as -> {
                Token token = as.ws.tokens.get(as.ws.current);
                if (token.getType() == PainlessLexer.TYPE || token.getType() == PainlessLexer.ID) {
                    as.segment.id = token.getText();
                    return -2;
                } else if (token.getType() == PainlessLexer.DOTID) {
                    as.segment.id = token.getText();
                    return 3;
                }
                return -1;
            });
            // 3
            astates.add(as -> {
                Token token = as.ws.tokens.get(as.ws.current);
                if (token.getType() == PainlessLexer.DOT || token.getType() == PainlessLexer.NSDOT) {
                    return 4;
                }
                return -1;
            });
            // 4
            astates.add(as -> {
                Token token = as.ws.tokens.get(as.ws.current);
                if (token.getType() == PainlessLexer.ID) {
                    as.segment = new Segment(as.segment, Segment.ID, token.getText(), -1);
                    return -2;
                } else if (token.getType() == PainlessLexer.TYPE || token.getType() == PainlessLexer.ATYPE) {
                    as.segment = new Segment(as.segment, Segment.TYPE, token.getText(), -1);
                    return -2;
                } else if (token.getType() == PainlessLexer.DOTID) {
                    as.segment = new Segment(as.segment, Segment.FIELD, token.getText(), -1);
                    return 3;
                } else if (token.getType() == PainlessLexer.DOTINTEGER) {
                    as.segment = new Segment(as.segment, Segment.NUMBER, token.getText(), -1);
                    return 3;
                } else if (token.getType() == PainlessLexer.RBRACE) {
                    as.segment = new Segment(as.segment, Segment.INDEX, null, -1);
                    return 5;
                } else if (token.getType() == PainlessLexer.RP) {
                    as.segment = new Segment(as.segment, Segment.CALL, null, 0);
                    return 7;
                }
                return -1;
            });
            // 5
            astates.add(as -> {
                Token token = as.ws.tokens.get(as.ws.current);
                if (token.getType() == PainlessLexer.LBRACE) {
                    if (as.brackets == 0 && as.parens == 0 && as.braces == 0) {
                        return 6;
                    } else {
                        --as.braces;
                    }
                } else if (token.getType() == PainlessLexer.RBRACE) {
                    ++as.braces;
                } else if (token.getType() == PainlessLexer.LP) {
                    --as.parens;
                } else if (token.getType() == PainlessLexer.RP) {
                    ++as.parens;
                } else if (token.getType() == PainlessLexer.LBRACK) {
                    --as.brackets;
                } else if (token.getType() == PainlessLexer.RBRACK) {
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
                if (token.getType() == PainlessLexer.ID) {
                    as.segment = new Segment(as.segment, Segment.ID, token.getText(), -1);
                    return -2;
                } else if (token.getType() == PainlessLexer.DOTID) {
                    as.segment = new Segment(as.segment, Segment.FIELD, token.getText(), -1);
                    return 3;
                } else if (token.getType() == PainlessLexer.RBRACE) {
                    as.segment = new Segment(as.segment, Segment.INDEX, null, -1);
                    return 5;
                } else if (token.getType() == PainlessLexer.RP) {
                    as.segment = new Segment(as.segment, Segment.CALL, null, 0);
                    return 7;
                }
                return -1;
            });
            // 7
            astates.add(as -> {
                Token token = as.ws.tokens.get(as.ws.current);
                if (token.getType() == PainlessLexer.LP) {
                    if (as.brackets == 0 && as.parens == 0 && as.braces == 0) {
                        return 8;
                    } else {
                        --as.parens;
                    }
                } else if (token.getType() == PainlessLexer.COMMA) {
                    if (as.brackets == 0 && as.parens == 0 && as.braces == 0) {
                        ++as.segment.arity;
                    }
                } else if (token.getType() == PainlessLexer.RP) {
                    ++as.parens;
                } else if (token.getType() == PainlessLexer.LBRACE) {
                    --as.braces;
                } else if (token.getType() == PainlessLexer.RBRACE) {
                    ++as.braces;
                } else if (token.getType() == PainlessLexer.LBRACK) {
                    --as.brackets;
                } else if (token.getType() == PainlessLexer.RBRACK) {
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
                if (token.getType() == PainlessLexer.ID) {
                    as.segment.id = token.getText();
                    return -2;
                } else if (token.getType() == PainlessLexer.DOTID) {
                    as.segment.id = token.getText();
                    return 3;
                } else if (token.getType() == PainlessLexer.TYPE) {
                    as.segment.type = Segment.CONSTRUCTOR;
                    as.segment.id = token.getText();
                    return 9;
                }
                return -1;
            });
            // 9
            astates.add(as -> {
                Token token = as.ws.tokens.get(as.ws.current);
                if (token.getType() == PainlessLexer.NEW) {
                    return -2;
                }
                return -1;
            });
        }

        private static void walk(AccessMachine.AccessState as) {
            WalkState ws = as.ws;
            ws.current = ws.tokens.size() - 1;

            while (ws.current >= 0) {
                Function<AccessMachine.AccessState, Integer> state = astates.get(as.target);
                as.target = state.apply(as);

                if (as.target < 0) {
                    break;
                }

                --ws.current;
            }
        }

        private AccessMachine() {

        }
    }

    private static List<String> track(List<? extends Token> tokens) {
        return new Tracker(tokens).track();
    }

    private final List<? extends Token> tokens;

    private Tracker(List<? extends Token> tokens) {
        this.tokens = Collections.unmodifiableList(tokens);
    }

    private List<String> track() {
        //FunctionState fs = new FunctionState(tokens);
        //FunctionMachine.walk(fs);

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
        AccessMachine.walk(as);

        if (as.target == -2) {
            builder.append(as.segment);
        }

        //for (FunctionMachine.FunctionState functionState = ws.functions) {

        //}

        if (true) throw new RuntimeException("\n\n" + builder);
        //if (true) throw new RuntimeException("\n\n" + builder.toString());
        //tokens.stream().map(t -> PainlessLexer.ruleNames[t.getType()] + ":" + t.getText()).collect(Collectors.toList())
        //        .toString()
        //);

        return null;
    }
}
