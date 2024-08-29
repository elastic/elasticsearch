/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.esql.parser.EsqlBaseParser.IdentifierContext;
import org.elasticsearch.xpack.esql.parser.EsqlBaseParser.IndexStringContext;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.transport.RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR;

abstract class IdentifierBuilder extends AbstractBuilder {

    @Override
    public String visitIdentifier(IdentifierContext ctx) {
        return ctx == null ? null : unquoteIdentifier(ctx.QUOTED_IDENTIFIER(), ctx.UNQUOTED_IDENTIFIER());
    }

    protected static String unquoteIdentifier(TerminalNode quotedNode, TerminalNode unquotedNode) {
        String result;
        if (quotedNode != null) {
            result = unquoteIdString(quotedNode.getText());
        } else {
            result = unquotedNode.getText();
        }
        return result;
    }

    protected static String unquoteIdString(String quotedString) {
        return quotedString.substring(1, quotedString.length() - 1).replace("``", "`");
    }

    @Override
    public String visitIndexString(IndexStringContext ctx) {
        TerminalNode n = ctx.UNQUOTED_SOURCE();
        return n != null ? n.getText() : unquote(ctx.QUOTED_STRING().getText());
    }

    public String visitIndexPattern(List<EsqlBaseParser.IndexPatternContext> ctx) {
        List<String> patterns = new ArrayList<>(ctx.size());
        ctx.forEach(c -> {
            String indexPattern = visitIndexString(c.indexString());
            patterns.add(
                c.clusterString() != null ? c.clusterString().getText() + REMOTE_CLUSTER_INDEX_SEPARATOR + indexPattern : indexPattern
            );
        });
        return Strings.collectionToDelimitedString(patterns, ",");
    }
}
