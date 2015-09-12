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

package org.elasticsearch.index.aliases;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.AliasFilterParsingException;
import org.elasticsearch.indices.InvalidAliasNameException;

import java.io.IOException;

/**
 *
 */
public class IndexAliasesService extends AbstractIndexComponent {

    private final IndexQueryParserService indexQueryParser;
    private volatile ImmutableOpenMap<String, AliasMetaData> aliases = ImmutableOpenMap.of();

    @Inject
    public IndexAliasesService(Index index, @IndexSettings Settings indexSettings, IndexQueryParserService indexQueryParser) {
        super(index, indexSettings);
        this.indexQueryParser = indexQueryParser;
    }

    /**
     * Returns the filter associated with listed filtering aliases.
     * <p/>
     * <p>The list of filtering aliases should be obtained by calling MetaData.filteringAliases.
     * Returns <tt>null</tt> if no filtering is required.</p>
     */
    public Query aliasFilter(String... aliasNames) {
        if (aliasNames == null || aliasNames.length == 0) {
            return null;
        }
        if (aliasNames.length == 1) {
            AliasMetaData alias = this.aliases.get(aliasNames[0]);
            if (alias == null) {
                // This shouldn't happen unless alias disappeared after filteringAliases was called.
                throw new InvalidAliasNameException(index, aliasNames[0], "Unknown alias name was passed to alias Filter");
            }
            return parse(alias);
        } else {
            // we need to bench here a bit, to see maybe it makes sense to use OrFilter
            BooleanQuery.Builder combined = new BooleanQuery.Builder();
            for (String aliasName : aliasNames) {
                AliasMetaData alias = this.aliases.get(aliasName);
                if (alias == null) {
                    // This shouldn't happen unless alias disappeared after filteringAliases was called.
                    throw new InvalidAliasNameException(index, aliasNames[0], "Unknown alias name was passed to alias Filter");
                }
                Query parsedFilter = parse(alias);
                if (parsedFilter != null) {
                    combined.add(parsedFilter, BooleanClause.Occur.SHOULD);
                } else {
                    // The filter might be null only if filter was removed after filteringAliases was called
                    return null;
                }
            }
            return combined.build();
        }
    }

    public void setAliases(ImmutableOpenMap<String, AliasMetaData> aliases) {
        this.aliases = aliases;
    }

    Query parse(AliasMetaData alias) {
        if (alias.filter() == null) {
            return null;
        }
        try {
            byte[] filterSource = alias.filter().uncompressed();
            try (XContentParser parser = XContentFactory.xContent(filterSource).createParser(filterSource)) {
                ParsedQuery parsedFilter = indexQueryParser.parseInnerFilter(parser);
                return parsedFilter == null ? null : parsedFilter.query();
            }
        } catch (IOException ex) {
            throw new AliasFilterParsingException(index, alias.getAlias(), "Invalid alias filter", ex);
        }
    }

    // Used by tests:
    void add(String alias, @Nullable CompressedXContent filter) {
        AliasMetaData aliasMetaData = AliasMetaData.builder(alias).filter(filter).build();
        aliases = ImmutableOpenMap.builder(aliases).fPut(alias, aliasMetaData).build();
    }

    boolean hasAlias(String alias) {
        return aliases.containsKey(alias);
    }

   void remove(String alias) {
       aliases = ImmutableOpenMap.builder(aliases).fRemove(alias).build();
    }
}
