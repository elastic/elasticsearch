package org.elasticsearch.index.query;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.search.Filter;

public class ParsedFilter {

    private final Filter filter;
    private final ImmutableMap<String, Filter> namedFilters;

    public ParsedFilter(Filter filter, ImmutableMap<String, Filter> namedFilters) {
        assert filter != null;
        assert namedFilters != null;
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
