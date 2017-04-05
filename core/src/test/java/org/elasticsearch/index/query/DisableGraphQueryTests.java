package org.elasticsearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 * Makes sure that graph analysis is disabled with shingle filters of different size
 */
public class DisableGraphQueryTests extends ESSingleNodeTestCase {
    private static IndexService indexService;
    private static QueryShardContext shardContext;
    private static Query expectedQuery;

    @Before
    public void setup() {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.shingle1.type", "shingle")
            .put("index.analysis.filter.shingle1.output_unigrams", true)
            .put("index.analysis.filter.shingle1.min_size", 2)
            .put("index.analysis.filter.shingle1.max_size", 2)
            .put("index.analysis.analyzer.text_shingle.tokenizer", "whitespace")
            .put("index.analysis.analyzer.text_shingle.filter", "lowercase, shingle1")
            .build();
        indexService = createIndex("test", settings, "t",
            "text_shingle", "type=text,analyzer=text_shingle");
        shardContext = indexService.newQueryShardContext(0, null, () -> 0L);

        // This is the expected parsed query for "foo bar baz"
        // with query parsers that ignores position length attribute.
        expectedQuery = new BooleanQuery.Builder()
            .add(
                new SynonymQuery(
                    new Term("text_shingle", "foo"),
                    new Term("text_shingle", "foo bar")
                ), BooleanClause.Occur.SHOULD)
            .add(new SynonymQuery(
                new Term("text_shingle", "bar"),
                new Term("text_shingle", "bar baz")
            ), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(
                new Term("text_shingle", "baz")
            ), BooleanClause.Occur.SHOULD)
            .build();
    }

    @After
    public void cleanup() {
        indexService = null;
        shardContext = null;
        expectedQuery = null;
    }

    public void testMatchQuery() throws IOException {
        MatchQueryBuilder builder = new MatchQueryBuilder("text_shingle", "foo bar baz");
        Query query = builder.doToQuery(shardContext);
        assertThat(expectedQuery, equalTo(query));
    }

    public void testMultiMatchQuery() throws IOException {
        MultiMatchQueryBuilder builder = new MultiMatchQueryBuilder("foo bar baz", "text_shingle");
        Query query = builder.doToQuery(shardContext);
        assertThat(expectedQuery, equalTo(query));
    }

    public void testSimpleQueryString() throws IOException {
        SimpleQueryStringBuilder builder = new SimpleQueryStringBuilder("foo bar baz");
        builder.field("text_shingle");
        builder.flags(SimpleQueryStringFlag.NONE);
        Query query = builder.doToQuery(shardContext);
        assertThat(expectedQuery, equalTo(query));
    }

    public void testQueryString() throws IOException {
        QueryStringQueryBuilder builder = new QueryStringQueryBuilder("foo bar baz");
        builder.field("text_shingle");
        builder.splitOnWhitespace(false);
        Query query = builder.doToQuery(shardContext);
        assertThat(expectedQuery, equalTo(query));
    }
}
