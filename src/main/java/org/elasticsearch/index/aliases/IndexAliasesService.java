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
import org.apache.lucene.search.Filter;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.ParsedFilter;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.AliasFilterParsingException;
import org.elasticsearch.indices.InvalidAliasNameException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public class IndexAliasesService extends AbstractIndexComponent implements Iterable<IndexAlias> {

    private final IndexQueryParserService indexQueryParser;
    private final Map<String, IndexAlias> aliases = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    @Inject
    public IndexAliasesService(Index index, @IndexSettings Settings indexSettings, IndexQueryParserService indexQueryParser) {
        super(index, indexSettings);
        this.indexQueryParser = indexQueryParser;
    }

    public boolean hasAlias(String alias) {
        return aliases.containsKey(alias);
    }

    public IndexAlias alias(String alias) {
        return aliases.get(alias);
    }

    public IndexAlias create(String alias, @Nullable CompressedString filter) {
        return new IndexAlias(alias, filter, parse(alias, filter));
    }

    public void add(String alias, @Nullable CompressedString filter) {
        add(new IndexAlias(alias, filter, parse(alias, filter)));
    }

    public void addAll(Map<String, IndexAlias> aliases) {
        this.aliases.putAll(aliases);
    }

    /**
     * Returns the filter associated with listed filtering aliases.
     * <p/>
     * <p>The list of filtering aliases should be obtained by calling MetaData.filteringAliases.
     * Returns <tt>null</tt> if no filtering is required.</p>
     */
    public Filter aliasFilter(String... aliases) {
        if (aliases == null || aliases.length == 0) {
            return null;
        }
        if (aliases.length == 1) {
            IndexAlias indexAlias = alias(aliases[0]);
            if (indexAlias == null) {
                // This shouldn't happen unless alias disappeared after filteringAliases was called.
                throw new InvalidAliasNameException(index, aliases[0], "Unknown alias name was passed to alias Filter");
            }
            return indexAlias.parsedFilter();
        } else {
            // we need to bench here a bit, to see maybe it makes sense to use OrFilter
            BooleanQuery combined = new BooleanQuery();
            for (String alias : aliases) {
                IndexAlias indexAlias = alias(alias);
                if (indexAlias == null) {
                    // This shouldn't happen unless alias disappeared after filteringAliases was called.
                    throw new InvalidAliasNameException(index, aliases[0], "Unknown alias name was passed to alias Filter");
                }
                if (indexAlias.parsedFilter() != null) {
                    combined.add(indexAlias.parsedFilter(), BooleanClause.Occur.SHOULD);
                } else {
                    // The filter might be null only if filter was removed after filteringAliases was called
                    return null;
                }
            }
            return Queries.wrap(combined);
        }
    }

    private void add(IndexAlias indexAlias) {
        aliases.put(indexAlias.alias(), indexAlias);
    }

    public void remove(String alias) {
        aliases.remove(alias);
    }

    private Filter parse(String alias, CompressedString filter) {
        if (filter == null) {
            return null;
        }
        try {
            byte[] filterSource = filter.uncompressed();
            try (XContentParser parser = XContentFactory.xContent(filterSource).createParser(filterSource)) {
                ParsedFilter parsedFilter = indexQueryParser.parseInnerFilter(parser);
                return parsedFilter == null ? null : parsedFilter.filter();
            }
        } catch (IOException ex) {
            throw new AliasFilterParsingException(index, alias, "Invalid alias filter", ex);
        }
    }

    @Override
    public Iterator<IndexAlias> iterator() {
        return aliases.values().iterator();
    }
}
