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

package org.elasticsearch.painless.tree.walker.analyzer;

class AnalyzerExternal {
    void visitExternal(final ExternalContext ctx) {
        final ExpressionMetadata extemd = metadata.getExpressionMetadata(ctx);

        final ExtstartContext extstartctx = ctx.extstart();
        final ExternalMetadata extstartemd = metadata.createExternalMetadata(extstartctx);
        extstartemd.read = extemd.read;
        analyzer.visit(extstartctx);

        extemd.statement = extstartemd.statement;
        extemd.preConst = extstartemd.constant;
        extemd.from = extstartemd.current;
        extemd.typesafe = extstartemd.current.sort != Sort.DEF;
    }

    void visitPostinc(final PostincContext ctx) {
        final ExpressionMetadata postincemd = metadata.getExpressionMetadata(ctx);

        final ExtstartContext extstartctx = ctx.extstart();
        final ExternalMetadata extstartemd = metadata.createExternalMetadata(extstartctx);
        extstartemd.read = postincemd.read;
        extstartemd.storeExpr = ctx.increment();
        extstartemd.token = ADD;
        extstartemd.post = true;
        analyzer.visit(extstartctx);

        postincemd.statement = true;
        postincemd.from = extstartemd.read ? extstartemd.current : definition.voidType;
        postincemd.typesafe = extstartemd.current.sort != Sort.DEF;
    }

    void visitPreinc(final PreincContext ctx) {
        final ExpressionMetadata preincemd = metadata.getExpressionMetadata(ctx);

        final ExtstartContext extstartctx = ctx.extstart();
        final ExternalMetadata extstartemd = metadata.createExternalMetadata(extstartctx);
        extstartemd.read = preincemd.read;
        extstartemd.storeExpr = ctx.increment();
        extstartemd.token = ADD;
        extstartemd.pre = true;
        analyzer.visit(extstartctx);

        preincemd.statement = true;
        preincemd.from = extstartemd.read ? extstartemd.current : definition.voidType;
        preincemd.typesafe = extstartemd.current.sort != Sort.DEF;
    }

    void visitAssignment(final AssignmentContext ctx) {
        final ExpressionMetadata assignemd = metadata.getExpressionMetadata(ctx);

        final ExtstartContext extstartctx = ctx.extstart();
        final ExternalMetadata extstartemd = metadata.createExternalMetadata(extstartctx);

        extstartemd.read = assignemd.read;
        extstartemd.storeExpr = AnalyzerUtility.updateExpressionTree(ctx.expression());

        if (ctx.AMUL() != null) {
            extstartemd.token = MUL;
        } else if (ctx.ADIV() != null) {
            extstartemd.token = DIV;
        } else if (ctx.AREM() != null) {
            extstartemd.token = REM;
        } else if (ctx.AADD() != null) {
            extstartemd.token = ADD;
        } else if (ctx.ASUB() != null) {
            extstartemd.token = SUB;
        } else if (ctx.ALSH() != null) {
            extstartemd.token = LSH;
        } else if (ctx.AUSH() != null) {
            extstartemd.token = USH;
        } else if (ctx.ARSH() != null) {
            extstartemd.token = RSH;
        } else if (ctx.AAND() != null) {
            extstartemd.token = BWAND;
        } else if (ctx.AXOR() != null) {
            extstartemd.token = BWXOR;
        } else if (ctx.AOR() != null) {
            extstartemd.token = BWOR;
        }

        analyzer.visit(extstartctx);

        assignemd.statement = true;
        assignemd.from = extstartemd.read ? extstartemd.current : definition.voidType;
        assignemd.typesafe = extstartemd.current.sort != Sort.DEF;
    }
}
