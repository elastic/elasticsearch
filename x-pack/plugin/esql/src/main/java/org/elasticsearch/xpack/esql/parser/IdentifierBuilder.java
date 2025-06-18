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
import static org.elasticsearch.transport.RemoteClusterAware.splitIndexName;
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
        } else {
            return ctx.UNQUOTED_SOURCE().getText();
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
        } else {
            return ctx.UNQUOTED_SOURCE().getText();
        }
    }

    public String visitIndexPattern(List<EsqlBaseParser.IndexPatternContext> ctx) {
        List<String> patterns = new ArrayList<>(ctx.size());
        Holder<Boolean> hasSeenStar = new Holder<>(false);
        ctx.forEach(c -> {
            String indexPattern = c.unquotedIndexString() != null ? c.unquotedIndexString().getText() : visitIndexString(c.indexString());
            String clusterString = visitClusterString(c.clusterString());
            String selectorString = visitSelectorString(c.selectorString());

            hasSeenStar.set(hasSeenStar.get() || indexPattern.contains(WILDCARD));
            validateClusterAndIndexPatterns(indexPattern, c, clusterString, selectorString, hasSeenStar.get());
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

    private static void validateClusterAndIndexPatterns(
        String indexPattern,
        EsqlBaseParser.IndexPatternContext ctx,
        String clusterString,
        String selectorString,
        boolean hasSeenStar
    ) {
        // multiple index names can be in the same double quote, e.g. indexPattern = "idx1, *, -idx2"
        String[] patterns = indexPattern.split(",");
        boolean isFirstPattern = true;

        for (String pattern : patterns) {
            pattern = pattern.strip();

            /*
             * Just because there was no clusterString before this index pattern does not mean that the indices
             * are local indices. Patterns can be clubbed with remote names within quotes such as:
             * "remote_one:remote_index,local_index". In this case, clusterString will be null.
             */
            if (isRemoteIndexName(pattern)) {
                /*
                 * Handle scenarios like remote_one:"index1,remote_two:index2". The clusterString here is
                 * remote_one and is associated with index1 and not index2.
                 */
                if (clusterString != null && isFirstPattern) {
                    throw new ParsingException(
                        source(ctx),
                        "Index pattern [{}] contains a cluster alias despite specifying one [{}]",
                        pattern,
                        clusterString
                    );
                }

                // {cluster_alias, indexName}
                String[] clusterAliasAndIndex;
                try {
                    clusterAliasAndIndex = splitIndexName(pattern);
                } catch (IllegalArgumentException e) {
                    throw new ParsingException(e, source(ctx), e.getMessage());
                }

                clusterString = clusterAliasAndIndex[0];
                pattern = clusterAliasAndIndex[1];
            } else if (clusterString != null) {
                // This is not a remote index pattern and the cluster string preceding this quoted pattern
                // cannot be associated with it.
                if (isFirstPattern == false) {
                    clusterString = null;
                }
            }

            if (clusterString != null) {
                if (selectorString != null) {
                    throwOnMixingSelectorWithCluster(reassembleIndexName(clusterString, indexPattern, selectorString), ctx);
                }
                validateClusterString(clusterString, ctx);
            }

            validateIndexForCluster(clusterString, pattern, ctx, hasSeenStar);
            if (selectorString != null) {
                try {
                    // Ensures that the selector provided is one of the valid kinds
                    IndexNameExpressionResolver.SelectorResolver.validateIndexSelectorString(indexPattern, selectorString);
                } catch (InvalidIndexNameException e) {
                    throw new ParsingException(e, source(ctx), e.getMessage());
                }
            }

            isFirstPattern = false;
        }
    }

    private static void validateIndexForCluster(
        String clusterString,
        String index,
        EsqlBaseParser.IndexPatternContext ctx,
        boolean hasSeenStar
    ) {
        // Strip spaces off first because validation checks are not written to handle them
        index = index.strip();

        try {
            Tuple<String, String> splitPattern = IndexNameExpressionResolver.splitSelectorExpression(index);
            if (splitPattern.v2() != null && clusterString != null) {
                throwOnMixingSelectorWithCluster(reassembleIndexName(clusterString, splitPattern.v1(), splitPattern.v2()), ctx);
            }

            index = splitPattern.v1();
        } catch (InvalidIndexNameException e) {
            // throws exception if the selector expression is invalid. Selector resolution does not complain about exclusions
            throw new ParsingException(e, source(ctx), e.getMessage());
        }
        hasSeenStar = hasSeenStar || index.contains(WILDCARD);
        index = index.replace(WILDCARD, "").strip();
        if (index.isBlank()) {
            return;
        }
        var hasExclusion = index.startsWith(EXCLUSION);
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
                return;
            }
            throw new ParsingException(e, source(ctx), e.getMessage());
        }
    }

    private static String removeExclusion(String indexPattern) {
        return indexPattern.charAt(0) == EXCLUSION.charAt(0) ? indexPattern.substring(1) : indexPattern;
    }
}
