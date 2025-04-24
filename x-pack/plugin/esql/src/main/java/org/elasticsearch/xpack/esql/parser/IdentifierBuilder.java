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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.parser.EsqlBaseParser.IdentifierContext;
import org.elasticsearch.xpack.esql.parser.EsqlBaseParser.IndexStringContext;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.SelectorResolver.SELECTOR_SEPARATOR;
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
    public String visitClusterString(EsqlBaseParser.ClusterStringContext ctx) {
        if (ctx == null) {
            return null;
        } else if (ctx.UNQUOTED_SOURCE() != null) {
            return ctx.UNQUOTED_SOURCE().getText();
        } else {
            return unquote(ctx.QUOTED_STRING().getText());
        }
    }

    @Override
    public String visitIndexString(IndexStringContext ctx) {
        if (ctx.UNQUOTED_SOURCE() != null) {
            return ctx.UNQUOTED_SOURCE().getText();
        } else {
            return unquote(ctx.QUOTED_STRING().getText());
        }
    }

    @Override
    public String visitSelectorString(EsqlBaseParser.SelectorStringContext ctx) {
        if (ctx == null) {
            return null;
        } else if (ctx.UNQUOTED_SOURCE() != null) {
            return ctx.UNQUOTED_SOURCE().getText();
        } else {
            return unquote(ctx.QUOTED_STRING().getText());
        }
    }

    public String visitIndexPattern(List<EsqlBaseParser.IndexPatternContext> ctx) {
        List<String> patterns = new ArrayList<>(ctx.size());
        Holder<Boolean> hasSeenStar = new Holder<>(false);
        ctx.forEach(c -> {
            String indexPattern = visitIndexString(c.indexString());
            String clusterString = visitClusterString(c.clusterString());
            String selectorString = visitSelectorString(c.selectorString());
            // skip validating index on remote cluster, because the behavior of remote cluster is not consistent with local cluster
            // For example, invalid#index is an invalid index name, however FROM *:invalid#index does not return an error
            if (clusterString == null) {
                hasSeenStar.set(indexPattern.contains(WILDCARD) || hasSeenStar.get());
                validateIndexPattern(indexPattern, c, hasSeenStar.get());
                // Other instances of Elasticsearch may have differing selectors so only validate selector string if remote cluster
                // string is unset
                if (selectorString != null) {
                    try {
                        // Ensures that the selector provided is one of the valid kinds
                        IndexNameExpressionResolver.SelectorResolver.validateIndexSelectorString(indexPattern, selectorString);
                    } catch (InvalidIndexNameException e) {
                        throw new ParsingException(e, source(c), e.getMessage());
                    }
                }
            } else {
                validateClusterString(clusterString, c);
                // Do not allow selectors on remote cluster expressions until they are supported
                if (selectorString != null) {
                    throwOnMixingSelectorWithCluster(reassembleIndexName(clusterString, indexPattern, selectorString), c);
                }
            }
            patterns.add(reassembleIndexName(clusterString, indexPattern, selectorString));
        });
        return Strings.collectionToDelimitedString(patterns, ",");
    }

    private static void throwOnMixingSelectorWithCluster(String indexPattern, EsqlBaseParser.IndexPatternContext c) {
        InvalidIndexNameException ie = new InvalidIndexNameException(
            indexPattern,
            "Selectors are not yet supported on remote cluster patterns"
        );
        throw new ParsingException(ie, source(c), ie.getMessage());
    }

    private static String reassembleIndexName(String clusterString, String indexPattern, String selectorString) {
        if (clusterString == null && selectorString == null) {
            return indexPattern;
        }
        StringBuilder expression = new StringBuilder();
        if (clusterString != null) {
            expression.append(clusterString).append(REMOTE_CLUSTER_INDEX_SEPARATOR);
        }
        expression.append(indexPattern);
        if (selectorString != null) {
            expression.append(SELECTOR_SEPARATOR).append(selectorString);
        }
        return expression.toString();
    }

    protected static void validateClusterString(String clusterString, EsqlBaseParser.IndexPatternContext ctx) {
        if (clusterString.indexOf(RemoteClusterService.REMOTE_CLUSTER_INDEX_SEPARATOR) != -1) {
            throw new ParsingException(source(ctx), "cluster string [{}] must not contain ':'", clusterString);
        }
    }

    private static void validateIndexPattern(String indexPattern, EsqlBaseParser.IndexPatternContext ctx, boolean hasSeenStar) {
        // multiple index names can be in the same double quote, e.g. indexPattern = "idx1, *, -idx2"
        String[] indices = indexPattern.split(",");
        boolean hasExclusion = false;
        for (String index : indices) {
            // Strip spaces off first because validation checks are not written to handle them
            index = index.strip();
            if (isRemoteIndexName(index)) { // skip the validation if there is remote cluster
                // Ensure that there are no selectors as they are not yet supported
                if (index.contains(SELECTOR_SEPARATOR)) {
                    throwOnMixingSelectorWithCluster(index, ctx);
                }
                continue;
            }
            try {
                Tuple<String, String> splitPattern = IndexNameExpressionResolver.splitSelectorExpression(index);
                if (splitPattern.v2() != null) {
                    index = splitPattern.v1();
                }
            } catch (InvalidIndexNameException e) {
                // throws exception if the selector expression is invalid. Selector resolution does not complain about exclusions
                throw new ParsingException(e, source(ctx), e.getMessage());
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
