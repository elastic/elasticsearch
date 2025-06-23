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
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.SelectorResolver.SELECTOR_SEPARATOR;
import static org.elasticsearch.transport.RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR;
import static org.elasticsearch.transport.RemoteClusterAware.splitIndexName;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.EXCLUSION;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.WILDCARD;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.source;

abstract class IdentifierBuilder extends AbstractBuilder {

    private static final String BLANK_INDEX_ERROR_MESSAGE = "Blank index specified in index pattern";

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
            validate(clusterString, indexPattern, selectorString, c, hasSeenStar.get());
            patterns.add(reassembleIndexName(clusterString, indexPattern, selectorString));
        });
        return Strings.collectionToDelimitedString(patterns, ",");
    }

    private static void throwInvalidIndexNameException(String indexPattern, String message, EsqlBaseParser.IndexPatternContext ctx) {
        var ie = new InvalidIndexNameException(indexPattern, message);
        throw new ParsingException(ie, source(ctx), ie.getMessage());
    }

    private static void throwOnMixingSelectorWithCluster(String indexPattern, EsqlBaseParser.IndexPatternContext c) {
        throwInvalidIndexNameException(indexPattern, "Selectors are not yet supported on remote cluster patterns", c);
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

    /**
     * Takes the parsed constituent strings and validates them.
     * @param clusterString Name of the remote cluster. Can be null.
     * @param indexPattern Name of the index or pattern; can also have multiple patterns in case of quoting,
     *                     e.g. {@code FROM """index*,-index1"""}.
     * @param selectorString Selector string, i.e. "::data" or "::failures". Can be null.
     * @param ctx Index Pattern Context for generating error messages with offsets.
     * @param hasSeenStar If we've seen an asterisk so far.
     */
    private static void validate(
        String clusterString,
        String indexPattern,
        String selectorString,
        EsqlBaseParser.IndexPatternContext ctx,
        boolean hasSeenStar
    ) {
        /*
         * At this point, only 3 formats are possible:
         * "index_pattern(s)",
         * remote:index_pattern, and,
         * index_pattern::selector_string.
         *
         * The grammar prohibits remote:"index_pattern(s)" or "index_pattern(s)"::selector_string as they're
         * partially quoted. So if either of cluster string or selector string are present, there's no need
         * to split the pattern by comma since comma requires partial quoting.
         */

        String[] patterns;
        if (clusterString == null && selectorString == null) {
            // Pattern could be quoted or is singular like "index_name".
            patterns = indexPattern.split(",", -1);
        } else {
            // Either of cluster string or selector string is present. Pattern is unquoted.
            patterns = new String[] { indexPattern };
        }

        patterns = Arrays.stream(patterns).map(String::strip).toArray(String[]::new);
        if (Arrays.stream(patterns).anyMatch(String::isBlank)) {
            throwInvalidIndexNameException(indexPattern, BLANK_INDEX_ERROR_MESSAGE, ctx);
        }

        // Edge case: happens when all the index names in a pattern are empty like "FROM ",,,,,"".
        if (patterns.length == 0) {
            throwInvalidIndexNameException(indexPattern, BLANK_INDEX_ERROR_MESSAGE, ctx);
        } else if (patterns.length == 1) {
            // Pattern is either an unquoted string or a quoted string with a single index (no comma sep).
            validateSingleIndexPattern(clusterString, patterns[0], selectorString, ctx, hasSeenStar);
        } else {
            /*
             * Presence of multiple patterns requires a comma and comma requires quoting. If quoting is present,
             * cluster string and selector string cannot be present; they need to be attached within the quoting.
             * So we attempt to extract them later.
             */
            for (String pattern : patterns) {
                validateSingleIndexPattern(null, pattern, null, ctx, hasSeenStar);
            }
        }
    }

    /**
     * Validates the constituent strings. Will extract the cluster string and/or selector string from the index
     * name if clubbed together inside a quoted string.
     *
     * @param clusterString Name of the remote cluster. Can be null.
     * @param indexName Name of the index.
     * @param selectorString Selector string, i.e. "::data" or "::failures". Can be null.
     * @param ctx Index Pattern Context for generating error messages with offsets.
     * @param hasSeenStar If we've seen an asterisk so far.
     */
    private static void validateSingleIndexPattern(
        String clusterString,
        String indexName,
        String selectorString,
        EsqlBaseParser.IndexPatternContext ctx,
        boolean hasSeenStar
    ) {
        indexName = indexName.strip();

        /*
         * Precedence:
         * 1. Cannot mix cluster and selector strings.
         * 2. Cluster string must be valid.
         * 3. Index name must be valid.
         * 4. Selector string must be valid.
         *
         * Since cluster string and/or selector string can be clubbed with the index name, we must try to
         * manually extract them before we attempt to do #2, #3, and #4.
         */

        // It is possible to specify a pattern like "remote_cluster:index_name". Try to extract such details from the index string.
        if (clusterString == null && selectorString == null) {
            try {
                var split = splitIndexName(indexName);
                clusterString = split[0];
                indexName = split[1];
            } catch (IllegalArgumentException e) {
                throw new ParsingException(e, source(ctx), e.getMessage());
            }
        }

        // At the moment, selector strings for remote indices is not allowed.
        if (clusterString != null && selectorString != null) {
            throwOnMixingSelectorWithCluster(reassembleIndexName(clusterString, indexName, selectorString), ctx);
        }

        // Validation in the right precedence.
        if (clusterString != null) {
            clusterString = clusterString.strip();
            validateClusterString(clusterString, ctx);
        }

        /*
         * It is possible for selector string to be attached to the index: "index_name::selector_string".
         * Try to extract the selector string.
         */
        try {
            Tuple<String, String> splitPattern = IndexNameExpressionResolver.splitSelectorExpression(indexName);
            if (splitPattern.v2() != null) {
                // Cluster string too was clubbed with the index name like selector string.
                if (clusterString != null) {
                    throwOnMixingSelectorWithCluster(reassembleIndexName(clusterString, splitPattern.v1(), splitPattern.v2()), ctx);
                } else {
                    // We've seen a selectorString. Use it.
                    selectorString = splitPattern.v2();
                }
            }

            indexName = splitPattern.v1();
        } catch (InvalidIndexNameException e) {
            throw new ParsingException(e, source(ctx), e.getMessage());
        }

        resolveAndValidateIndex(indexName, ctx, hasSeenStar);
        if (selectorString != null) {
            selectorString = selectorString.strip();
            try {
                // Ensures that the selector provided is one of the valid kinds.
                IndexNameExpressionResolver.SelectorResolver.validateIndexSelectorString(indexName, selectorString);
            } catch (InvalidIndexNameException e) {
                throw new ParsingException(e, source(ctx), e.getMessage());
            }
        }
    }

    private static void resolveAndValidateIndex(String index, EsqlBaseParser.IndexPatternContext ctx, boolean hasSeenStar) {
        // If index name is blank without any replacements, it was likely blank right from the beginning and is invalid.
        if (index.isBlank()) {
            throwInvalidIndexNameException(index, BLANK_INDEX_ERROR_MESSAGE, ctx);
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
