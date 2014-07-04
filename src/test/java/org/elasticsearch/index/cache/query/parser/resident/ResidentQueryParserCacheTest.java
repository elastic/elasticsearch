package org.elasticsearch.index.cache.query.parser.resident;

import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParserSettings;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 */
public class ResidentQueryParserCacheTest extends ElasticsearchTestCase {

    @Test
    public void testCaching() throws Exception {
        ResidentQueryParserCache cache = new ResidentQueryParserCache(new Index("test"), ImmutableSettings.EMPTY);
        QueryParserSettings key = new QueryParserSettings();
        key.queryString("abc");
        key.defaultField("a");
        key.boost(2.0f);

        Query query = new TermQuery(new Term("a", "abc"));
        cache.put(key, query);

        assertThat(cache.get(key), not(sameInstance(query)));
        assertThat(cache.get(key), equalTo(query));
    }

}
