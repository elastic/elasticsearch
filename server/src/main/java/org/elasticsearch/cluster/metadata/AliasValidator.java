/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.indices.InvalidAliasNameException;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseTopLevelQuery;

/**
 * Validator for an alias, to be used before adding an alias to the index metadata
 * and make sure the alias is valid
 */
public class AliasValidator {

    private AliasValidator() {
        // utility class
    }

    /**
     * Allows to validate an {@link org.elasticsearch.action.admin.indices.alias.Alias} and make sure
     * it's valid before it gets added to the index metadata. Doesn't validate the alias filter.
     * @throws IllegalArgumentException if the alias is not valid
     */
    public static void validateAlias(Alias alias, String index, ProjectMetadata projectMetadata) {
        validateAlias(alias.name(), index, alias.indexRouting(), lookup(projectMetadata));
    }

    /**
     * Allows to validate an {@link org.elasticsearch.cluster.metadata.AliasMetadata} and make sure
     * it's valid before it gets added to the index metadata. Doesn't validate the alias filter.
     * @throws IllegalArgumentException if the alias is not valid
     */
    public static void validateAliasMetadata(AliasMetadata aliasMetadata, String index, ProjectMetadata projectMetadata) {
        validateAlias(aliasMetadata.alias(), index, aliasMetadata.indexRouting(), lookup(projectMetadata));
    }

    /**
     * Allows to partially validate an alias, without knowing which index it'll get applied to.
     * Useful with index templates containing aliases. Checks also that it is possible to parse
     * the alias filter via {@link org.elasticsearch.xcontent.XContentParser},
     * without validating it as a filter though.
     * @throws IllegalArgumentException if the alias is not valid
     */
    public static void validateAliasStandalone(Alias alias) {
        validateAliasStandalone(alias.name(), alias.indexRouting());
        if (Strings.hasLength(alias.filter())) {
            try {
                XContentHelper.convertToMap(XContentFactory.xContent(alias.filter()), alias.filter(), false);
            } catch (Exception e) {
                throw new IllegalArgumentException("failed to parse filter for alias [" + alias.name() + "]", e);
            }
        }
    }

    /**
     * Validate a proposed alias.
     */
    public static void validateAlias(String alias, String index, @Nullable String indexRouting, Function<String, String> lookup) {
        validateAliasStandalone(alias, indexRouting);

        if (Strings.hasText(index) == false) {
            throw new IllegalArgumentException("index name is required");
        }

        String sameNameAsAlias = lookup.apply(alias);
        if (sameNameAsAlias != null) {
            throw new InvalidAliasNameException(alias, "an index or data stream exists with the same name as the alias");
        }
    }

    static void validateAliasStandalone(String alias, String indexRouting) {
        if (Strings.hasText(alias) == false) {
            throw new IllegalArgumentException("alias name is required");
        }
        MetadataCreateIndexService.validateIndexOrAliasName(alias, InvalidAliasNameException::new);
        if (indexRouting != null && indexRouting.indexOf(',') != -1) {
            throw new IllegalArgumentException("alias [" + alias + "] has several index routing values associated with it");
        }
    }

    /**
     * Validates an alias filter by parsing it using the
     * provided {@link SearchExecutionContext}
     * @throws IllegalArgumentException if the filter is not valid
     */
    public static void validateAliasFilter(
        String alias,
        String filter,
        SearchExecutionContext searchExecutionContext,
        NamedXContentRegistry xContentRegistry
    ) {
        assert searchExecutionContext != null;
        try (
            XContentParser parser = XContentFactory.xContent(filter)
                .createParser(
                    XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry)
                        .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
                    filter
                )
        ) {
            validateAliasFilter(parser, searchExecutionContext);
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to parse filter for alias [" + alias + "]", e);
        }
    }

    /**
     * Validates an alias filter by parsing it using the
     * provided {@link SearchExecutionContext}
     * @throws IllegalArgumentException if the filter is not valid
     */
    public static void validateAliasFilter(
        String alias,
        BytesReference filter,
        SearchExecutionContext searchExecutionContext,
        NamedXContentRegistry xContentRegistry
    ) {
        assert searchExecutionContext != null;

        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry).withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
                filter,
                XContentHelper.xContentType(filter)
            )
        ) {
            validateAliasFilter(parser, searchExecutionContext);
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to parse filter for alias [" + alias + "]", e);
        }
    }

    private static void validateAliasFilter(XContentParser parser, SearchExecutionContext searchExecutionContext) throws IOException {
        QueryBuilder parseInnerQueryBuilder = parseTopLevelQuery(parser);
        QueryBuilder queryBuilder = Rewriteable.rewrite(parseInnerQueryBuilder, searchExecutionContext, true);
        queryBuilder.toQuery(searchExecutionContext);
    }

    private static Function<String, String> lookup(ProjectMetadata projectMetadata) {
        return name -> Optional.ofNullable(projectMetadata.getIndicesLookup().get(name))
            .filter(indexAbstraction -> indexAbstraction.getType() != IndexAbstraction.Type.ALIAS)
            .map(IndexAbstraction::getName)
            .orElse(null);
    }
}
