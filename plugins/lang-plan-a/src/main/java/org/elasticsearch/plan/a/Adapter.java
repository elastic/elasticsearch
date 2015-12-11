/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plan.a;

import java.util.HashMap;
import java.util.Map;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;

import static org.elasticsearch.plan.a.Definition.*;
import static org.elasticsearch.plan.a.PlanAParser.*;

class Adapter {
    static class StatementMetadata {
        final ParserRuleContext source;

        boolean last;

        boolean allExit;
        boolean allReturn;
        boolean anyReturn;
        boolean allBreak;
        boolean anyBreak;
        boolean allContinue;
        boolean anyContinue;

        private StatementMetadata(final ParserRuleContext source) {
            this.source = source;

            last = false;

            allExit = false;
            allReturn = false;
            anyReturn = false;
            allBreak = false;
            anyBreak = false;
            allContinue = false;
            anyContinue = false;
        }
    }

    static class ExpressionMetadata {
        final ParserRuleContext source;

        boolean read;
        boolean statement;

        Object preConst;
        Object postConst;
        boolean isNull;

        Type to;
        Type from;
        boolean explicit;
        boolean typesafe;

        Cast cast;

        private ExpressionMetadata(final ParserRuleContext source) {
            this.source = source;

            read = true;
            statement = false;

            preConst = null;
            postConst = null;
            isNull = false;

            to = null;
            from = null;
            explicit = false;
            typesafe = true;

            cast = null;
        }
    }

    static class ExternalMetadata {
        final ParserRuleContext source;

        boolean read;
        ParserRuleContext storeExpr;
        int token;
        boolean pre;
        boolean post;

        int scope;
        Type current;
        boolean statik;
        boolean statement;
        Object constant;

        private ExternalMetadata(final ParserRuleContext source) {
            this.source = source;

            read = false;
            storeExpr = null;
            token = 0;
            pre = false;
            post = false;

            scope = 0;
            current = null;
            statik = false;
            statement = false;
            constant = null;
        }
    }

    static class ExtNodeMetadata {
        final ParserRuleContext parent;
        final ParserRuleContext source;

        Object target;
        boolean last;

        Type type;
        Type promote;

        Cast castFrom;
        Cast castTo;

        private ExtNodeMetadata(final ParserRuleContext parent, final ParserRuleContext source) {
            this.parent = parent;
            this.source = source;

            target = null;
            last = false;

            type = null;
            promote = null;

            castFrom = null;
            castTo = null;
        }
    }

    static String error(final ParserRuleContext ctx) {
        return "Error [" + ctx.getStart().getLine() + ":" + ctx.getStart().getCharPositionInLine() + "]: ";
    }

    final Definition definition;
    final String source;
    final ParserRuleContext root;
    final CompilerSettings settings;

    private final Map<ParserRuleContext, StatementMetadata> statementMetadata;
    private final Map<ParserRuleContext, ExpressionMetadata> expressionMetadata;
    private final Map<ParserRuleContext, ExternalMetadata> externalMetadata;
    private final Map<ParserRuleContext, ExtNodeMetadata> extNodeMetadata;

    Adapter(final Definition definition, final String source, final ParserRuleContext root, final CompilerSettings settings) {
        this.definition = definition;
        this.source = source;
        this.root = root;
        this.settings = settings;

        statementMetadata = new HashMap<>();
        expressionMetadata = new HashMap<>();
        externalMetadata = new HashMap<>();
        extNodeMetadata = new HashMap<>();
    }

    StatementMetadata createStatementMetadata(final ParserRuleContext source) {
        final StatementMetadata sourcesmd = new StatementMetadata(source);
        statementMetadata.put(source, sourcesmd);

        return sourcesmd;
    }

    StatementMetadata getStatementMetadata(final ParserRuleContext source) {
        final StatementMetadata sourcesmd = statementMetadata.get(source);

        if (sourcesmd == null) {
            throw new IllegalStateException(error(source) + "Statement metadata does not exist at" +
                    " the parse node with text [" + source.getText() + "].");
        }

        return sourcesmd;
    }

    ExpressionContext updateExpressionTree(ExpressionContext source) {
        if (source instanceof PrecedenceContext) {
            final ParserRuleContext parent = source.getParent();
            int index = 0;

            for (final ParseTree child : parent.children) {
                if (child == source) {
                    break;
                }

                ++index;
            }

            while (source instanceof PrecedenceContext) {
                source = ((PrecedenceContext)source).expression();
            }

            parent.children.set(index, source);
        }

        return source;
    }

    ExpressionMetadata createExpressionMetadata(ParserRuleContext source) {
        final ExpressionMetadata sourceemd = new ExpressionMetadata(source);
        expressionMetadata.put(source, sourceemd);

        return sourceemd;
    }
    
    ExpressionMetadata getExpressionMetadata(final ParserRuleContext source) {
        final ExpressionMetadata sourceemd = expressionMetadata.get(source);

        if (sourceemd == null) {
            throw new IllegalStateException(error(source) + "Expression metadata does not exist at" +
                    " the parse node with text [" + source.getText() + "].");
        }

        return sourceemd;
    }

    ExternalMetadata createExternalMetadata(final ParserRuleContext source) {
        final ExternalMetadata sourceemd = new ExternalMetadata(source);
        externalMetadata.put(source, sourceemd);

        return sourceemd;
    }

    ExternalMetadata getExternalMetadata(final ParserRuleContext source) {
        final ExternalMetadata sourceemd = externalMetadata.get(source);

        if (sourceemd == null) {
            throw new IllegalStateException(error(source) + "External metadata does not exist at" +
                    " the parse node with text [" + source.getText() + "].");
        }

        return sourceemd;
    }

    ExtNodeMetadata createExtNodeMetadata(final ParserRuleContext parent, final ParserRuleContext source) {
        final ExtNodeMetadata sourceemd = new ExtNodeMetadata(parent, source);
        extNodeMetadata.put(source, sourceemd);

        return sourceemd;
    }

    ExtNodeMetadata getExtNodeMetadata(final ParserRuleContext source) {
        final ExtNodeMetadata sourceemd = extNodeMetadata.get(source);

        if (sourceemd == null) {
            throw new IllegalStateException(error(source) + "External metadata does not exist at" +
                    " the parse node with text [" + source.getText() + "].");
        }

        return sourceemd;
    }
}
