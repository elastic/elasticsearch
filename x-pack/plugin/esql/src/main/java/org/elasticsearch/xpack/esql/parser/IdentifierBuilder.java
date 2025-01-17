/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.parser.EsqlBaseParser.IdentifierContext;
import org.elasticsearch.xpack.esql.parser.EsqlBaseParser.IndexStringContext;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.transport.RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR;
import static org.elasticsearch.transport.RemoteClusterAware.isRemoteIndexName;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.EXCLUSION;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.WILDCARD;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.source;

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

    protected static String quoteIdString(String unquotedString) {
        return "`" + unquotedString.replace("`", "``") + "`";
    }

    @Override
    public String visitIndexString(IndexStringContext ctx) {
        TerminalNode n = ctx.UNQUOTED_SOURCE();
        return n != null ? n.getText() : unquote(ctx.QUOTED_STRING().getText());
    }

    public String visitIndexPattern(List<EsqlBaseParser.IndexPatternContext> ctx) {
        List<String> patterns = new ArrayList<>(ctx.size());
        Holder<Boolean> hasSeenStar = new Holder<>(false);
        ctx.forEach(c -> {
            String indexPattern = visitIndexString(c.indexString());
            String clusterString = c.clusterString() != null ? c.clusterString().getText() : null;
            // skip validating index on remote cluster, because the behavior of remote cluster is not consistent with local cluster
            // For example, invalid#index is an invalid index name, however FROM *:invalid#index does not return an error
            if (clusterString == null) {
                hasSeenStar.set(indexPattern.contains(WILDCARD) || hasSeenStar.get());
                validateIndexPattern(indexPattern, c, hasSeenStar.get());
            }
            patterns.add(clusterString != null ? clusterString + REMOTE_CLUSTER_INDEX_SEPARATOR + indexPattern : indexPattern);
        });
        return Strings.collectionToDelimitedString(patterns, ",");
    }

    private static void validateIndexPattern(String indexPattern, EsqlBaseParser.IndexPatternContext ctx, boolean hasSeenStar) {
        // multiple index names can be in the same double quote, e.g. indexPattern = "idx1, *, -idx2"
        String[] indices = indexPattern.split(",");
        boolean hasExclusion = false;
        for (String index : indices) {
            if (isRemoteIndexName(index)) { // skip the validation if there is remote cluster
                continue;
            }
            hasSeenStar = index.contains(WILDCARD) || hasSeenStar;
            index = index.replace(WILDCARD, "").strip();
            if (index.isBlank()) {
                continue;
            }
            hasExclusion = index.startsWith(EXCLUSION);
            index = removeExclusion(index);
            String tempName;
            try {
                // remove the exclusion outside of <>, from index names with DateMath expression,
                // e.g. -<-logstash-{now/d}> becomes <-logstash-{now/d}> before calling resolveDateMathExpression
                tempName = IndexNameExpressionResolver.resolveDateMathExpression(index);
            } catch (ElasticsearchParseException e) {
                // throws exception if the DateMath expression is invalid, resolveDateMathExpression does not complain about exclusions
                throw new ParsingException(e, source(ctx), e.getMessage());
            }
            hasExclusion = tempName.startsWith(EXCLUSION) || hasExclusion;
            index = tempName.equals(index) ? index : removeExclusion(tempName);
            try {
                MetadataCreateIndexService.validateIndexOrAliasName(index, InvalidIndexNameException::new);
            } catch (InvalidIndexNameException e) {
                // ignore invalid index name if it has exclusions and there is an index with wildcard before it
                if (hasSeenStar && hasExclusion) {
                    continue;
                }
                throw new ParsingException(e, source(ctx), e.getMessage());
            }
        }
    }

    private static String removeExclusion(String indexPattern) {
        return indexPattern.charAt(0) == EXCLUSION.charAt(0) ? indexPattern.substring(1) : indexPattern;
    }
}
