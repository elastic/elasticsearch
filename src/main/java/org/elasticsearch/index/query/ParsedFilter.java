package org.elasticsearch.index.query;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.search.Filter;
import org.elasticsearch.common.lucene.search.Queries;

public class ParsedFilter {

    public static final ParsedFilter EMPTY = new ParsedFilter(Queries.MATCH_NO_FILTER, ImmutableMap.<String, Filter>of());

    private final Filter filter;

    private final ImmutableMap<String, Filter> namedFilters;

    public ParsedFilter(Filter filter, ImmutableMap<String, Filter> namedFilters) {
        this.filter = filter;
        this.namedFilters = namedFilters;
    }

    public Filter filter() {
        return filter;
    }

    public ImmutableMap<String, Filter> namedFilters() {
        return namedFilters;
    }
}
