/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilterClause;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.UnmodifiableIterator;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.XBooleanFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.xcontent.XContentIndexQueryParser;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.AliasFilterParsingException;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.collect.Lists.*;
import static org.elasticsearch.common.collect.MapBuilder.*;

/**
 * @author imotov
 */
public class IndexAliasesService extends AbstractIndexComponent implements Iterable<IndexAlias> {

    private final IndexQueryParserService indexQueryParserService;

    private volatile ImmutableMap<String, IndexAlias> aliases = ImmutableMap.of();

    private final Object mutex = new Object();

    @Inject public IndexAliasesService(Index index, @IndexSettings Settings indexSettings, IndexQueryParserService indexQueryParserService) {
        super(index, indexSettings);
        this.indexQueryParserService = indexQueryParserService;
    }

    public boolean hasAlias(String alias) {
        return aliases.containsKey(alias);
    }

    public IndexAlias alias(String alias) {
        return aliases.get(alias);
    }

    public void add(String alias, @Nullable CompressedString filter) {
        add(new IndexAlias(alias, filter, parse(alias, filter)));
    }

    /**
     * Returns the filter associated with a possibly aliased indices.
     *
     * <p>Returns <tt>null</tt> if no filtering is required. Note, if no alias if found for the provided value
     * then no filtering is done.</p>
     */
    public Filter aliasFilter(String... indices) {
        if (indices == null) {
            return null;
        }
        // optimize for the most common single index/alias scenario
        if (indices.length == 1) {
            String alias = indices[0];
            // The list contains the index itself - no filtering needed
            if (alias.equals(index.name())) {
                return null;
            }
            IndexAlias indexAlias = aliases.get(alias);
            if (indexAlias == null) {
                return null;
            }
            return indexAlias.parsedFilter();
        }
        List<Filter> filters = null;
        for (String alias : indices) {
            // The list contains the index itself - no filtering needed
            if (alias.equals(index.name())) {
                return null;
            }
            IndexAlias indexAlias = aliases.get(alias);
            if (indexAlias != null) {
                // The list contains a non-filtering alias - no filtering needed
                if (indexAlias.parsedFilter() == null) {
                    return null;
                } else {
                    if (filters == null) {
                        filters = newArrayList();
                    }
                    filters.add(indexAlias.parsedFilter());
                }
            }
        }
        if (filters == null) {
            return null;
        }
        if (filters.size() == 1) {
            return filters.get(0);
        } else {
            XBooleanFilter combined = new XBooleanFilter();
            for (Filter filter : filters) {
                combined.add(new FilterClause(filter, BooleanClause.Occur.SHOULD));
            }
            return combined;
        }
    }

    private void add(IndexAlias indexAlias) {
        synchronized (mutex) {
            aliases = newMapBuilder(aliases).put(indexAlias.alias(), indexAlias).immutableMap();
        }
    }

    public void remove(String alias) {
        synchronized (mutex) {
            aliases = newMapBuilder(aliases).remove(alias).immutableMap();
        }
    }

    private Filter parse(String alias, CompressedString filter) {
        if (filter == null) {
            return null;
        }
        XContentIndexQueryParser indexQueryParser = (XContentIndexQueryParser) indexQueryParserService.defaultIndexQueryParser();
        try {
            byte[] filterSource = filter.uncompressed();
            XContentParser parser = XContentFactory.xContent(filterSource).createParser(filterSource);
            try {
                return indexQueryParser.parseInnerFilter(parser);
            } finally {
                parser.close();
            }
        } catch (IOException ex) {
            throw new AliasFilterParsingException(index, alias, "Invalid alias filter", ex);
        }
    }

    @Override public UnmodifiableIterator<IndexAlias> iterator() {
        return aliases.values().iterator();
    }
}
