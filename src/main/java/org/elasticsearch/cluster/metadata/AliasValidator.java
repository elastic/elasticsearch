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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.indices.InvalidAliasNameException;

/**
 * Validator for an alias, to be used before adding an alias to the index metadata
 * and make sure the alias is valid
 */
public class AliasValidator extends AbstractComponent {

    @Inject
    public AliasValidator(Settings settings) {
        super(settings);
    }

    /**
     * Allows to validate an {@link org.elasticsearch.cluster.metadata.AliasAction} and make sure
     * it's valid before it gets added to the index metadata
     */
    public void validateAliasAction(AliasAction aliasAction, MetaData metaData) {
        validateAlias(aliasAction.alias(), aliasAction.index(), aliasAction.indexRouting(), metaData);
    }

    /**
     * Allows to validate an {@link org.elasticsearch.action.admin.indices.alias.Alias} and make sure
     * it's valid before it gets added to the index metadata
     */
    public void validateAlias(Alias alias, String index, MetaData metaData) {
        validateAlias(alias.name(), index, alias.indexRouting(), metaData);
    }

    private void validateAlias(String alias, String index, String indexRouting, MetaData metaData) {
        assert metaData != null;
        if (!Strings.hasText(alias) || !Strings.hasText(index)) {
            throw new ElasticsearchIllegalArgumentException("index name and alias name are required");
        }

        if (metaData.hasIndex(alias)) {
            throw new InvalidAliasNameException(new Index(index), alias, "an index exists with the same name as the alias");
        }

        if (indexRouting != null && indexRouting.indexOf(',') != -1) {
            throw new ElasticsearchIllegalArgumentException("alias [" + alias + "] has several index routing values associated with it");
        }
    }

    /**
     * Validates an alias filter by parsing it using the
     * provided {@link org.elasticsearch.index.query.IndexQueryParserService}
     * @throws org.elasticsearch.ElasticsearchIllegalArgumentException if the filter is not valid
     */
    public void validateAliasFilter(String alias, String filter, IndexQueryParserService indexQueryParserService) {
        assert indexQueryParserService != null;
        try {
            XContentParser parser = XContentFactory.xContent(filter).createParser(filter);
            try {
                indexQueryParserService.parseInnerFilter(parser);
            } finally {
                parser.close();
            }
        } catch (Throwable e) {
            throw new ElasticsearchIllegalArgumentException("failed to parse filter for alias [" + alias + "]", e);
        }
    }
}
