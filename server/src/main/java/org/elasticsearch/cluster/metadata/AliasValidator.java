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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.indices.InvalidAliasNameException;

import java.io.IOException;
import java.util.function.Function;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

/**
 * Validator for an alias, to be used before adding an alias to the index metadata
 * and make sure the alias is valid
 */
public class AliasValidator {
    /**
     * Allows to validate an {@link org.elasticsearch.action.admin.indices.alias.Alias} and make sure
     * it's valid before it gets added to the index metadata. Doesn't validate the alias filter.
     * @throws IllegalArgumentException if the alias is not valid
     */
    public void validateAlias(Alias alias, String index, Metadata metadata) {
        validateAlias(alias.name(), index, alias.indexRouting(), metadata::index);
    }

    /**
     * Allows to validate an {@link org.elasticsearch.cluster.metadata.AliasMetadata} and make sure
     * it's valid before it gets added to the index metadata. Doesn't validate the alias filter.
     * @throws IllegalArgumentException if the alias is not valid
     */
    public void validateAliasMetadata(AliasMetadata aliasMetadata, String index, Metadata metadata) {
        validateAlias(aliasMetadata.alias(), index, aliasMetadata.indexRouting(), metadata::index);
    }

    /**
     * Allows to partially validate an alias, without knowing which index it'll get applied to.
     * Useful with index templates containing aliases. Checks also that it is possible to parse
     * the alias filter via {@link org.elasticsearch.common.xcontent.XContentParser},
     * without validating it as a filter though.
     * @throws IllegalArgumentException if the alias is not valid
     */
    public void validateAliasStandalone(Alias alias) {
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
    public void validateAlias(String alias, String index, @Nullable String indexRouting, Function<String, IndexMetadata> indexLookup) {
        validateAliasStandalone(alias, indexRouting);

        if (!Strings.hasText(index)) {
            throw new IllegalArgumentException("index name is required");
        }

        IndexMetadata indexNamedSameAsAlias = indexLookup.apply(alias);
        if (indexNamedSameAsAlias != null) {
            throw new InvalidAliasNameException(indexNamedSameAsAlias.getIndex(), alias, "an index exists with the same name as the alias");
        }
    }

    void validateAliasStandalone(String alias, String indexRouting) {
        if (!Strings.hasText(alias)) {
            throw new IllegalArgumentException("alias name is required");
        }
        MetadataCreateIndexService.validateIndexOrAliasName(alias, InvalidAliasNameException::new);
        if (indexRouting != null && indexRouting.indexOf(',') != -1) {
            throw new IllegalArgumentException("alias [" + alias + "] has several index routing values associated with it");
        }
    }

    /**
     * Validates an alias filter by parsing it using the
     * provided {@link org.elasticsearch.index.query.QueryShardContext}
     * @throws IllegalArgumentException if the filter is not valid
     */
    public void validateAliasFilter(String alias, String filter, QueryShardContext queryShardContext,
            NamedXContentRegistry xContentRegistry) {
        assert queryShardContext != null;
        try (XContentParser parser = XContentFactory.xContent(filter)
            .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, filter)) {
            validateAliasFilter(parser, queryShardContext);
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to parse filter for alias [" + alias + "]", e);
        }
    }

    /**
     * Validates an alias filter by parsing it using the
     * provided {@link org.elasticsearch.index.query.QueryShardContext}
     * @throws IllegalArgumentException if the filter is not valid
     */
    public void validateAliasFilter(String alias, byte[] filter, QueryShardContext queryShardContext,
            NamedXContentRegistry xContentRegistry) {
        assert queryShardContext != null;
        try (XContentParser parser = XContentFactory.xContent(filter)
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, filter)) {
            validateAliasFilter(parser, queryShardContext);
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to parse filter for alias [" + alias + "]", e);
        }
    }

    private static void validateAliasFilter(XContentParser parser, QueryShardContext queryShardContext) throws IOException {
        QueryBuilder parseInnerQueryBuilder = parseInnerQueryBuilder(parser);
        QueryBuilder queryBuilder = Rewriteable.rewrite(parseInnerQueryBuilder, queryShardContext, true);
        queryBuilder.toQuery(queryShardContext);
    }
}
