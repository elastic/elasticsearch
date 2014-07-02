package org.elasticsearch.search.profile;

import org.apache.lucene.queryparser.xml.builders.BooleanFilterBuilder;
import org.apache.lucene.search.Query;
import org.elasticsearch.test.ElasticsearchIntegrationTest;


import org.apache.lucene.util.English;
import org.elasticsearch.action.index.IndexRequestBuilder;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.*;

import org.junit.Test;

import java.util.ArrayList;


public class ProfileQueryTests extends ElasticsearchIntegrationTest {

    private QueryBuilder randomQueryBuilder(int numDocs, int depth) {

        if (depth == 0) {
            return QueryBuilders.termQuery("field1", English.intToEnglish(randomIntBetween(0, numDocs)));
        }

        QueryBuilder q;
        switch (randomIntBetween(0,5)) {
            case 0:
                q = QueryBuilders.filteredQuery(randomQueryBuilder(numDocs, depth -1), randomFilterBuilder(numDocs, depth -1));
                break;
            case 1:
                q = QueryBuilders.termQuery("field1", English.intToEnglish(randomIntBetween(0,numDocs)));
                break;
            case 2:
                q = QueryBuilders.termsQuery("field1", English.intToEnglish(randomIntBetween(0,numDocs)),English.intToEnglish(randomIntBetween(0,numDocs)));
                break;
            case 3:
                q = QueryBuilders.rangeQuery("field2").from(randomIntBetween(0,numDocs/2 - 1)).to(randomIntBetween(numDocs/2, numDocs));
                break;
            case 4:
                q = QueryBuilders.boolQuery();
                int numClause = randomIntBetween(0,5);
                for (int i = 0; i < numClause; i++) {
                    ((BoolQueryBuilder)q).must(randomQueryBuilder(numDocs, depth -1));
                }
                numClause = randomIntBetween(0,5);
                for (int i = 0; i < numClause; i++) {
                    ((BoolQueryBuilder)q).should(randomQueryBuilder(numDocs, depth -1));
                }
                numClause = randomIntBetween(0,5);
                for (int i = 0; i < numClause; i++) {
                    ((BoolQueryBuilder)q).mustNot(randomQueryBuilder(numDocs, depth -1));
                }
                break;
            case 5:
                q = QueryBuilders.matchAllQuery();
                break;
            default:
                q = QueryBuilders.termQuery("field1", English.intToEnglish(randomIntBetween(0,numDocs)));
                break;
        }

        return q;
    }

    private FilterBuilder randomFilterBuilder(int numDocs, int depth) {
        if (depth == 0) {
            return FilterBuilders.termFilter("field1", English.intToEnglish(randomIntBetween(0,numDocs)));
        } 
        FilterBuilder f;
        switch (randomIntBetween(0,7)) {
            case 0:
                f = FilterBuilders.matchAllFilter();
                break;
            case 1:
                f = FilterBuilders.termFilter("field1", English.intToEnglish(randomIntBetween(0,numDocs)));
                break;
            case 2:
                f = FilterBuilders.termsFilter("field1", English.intToEnglish(randomIntBetween(0,numDocs)),English.intToEnglish(randomIntBetween(0,numDocs)));
                break;
            case 3:
                f = FilterBuilders.rangeFilter("field2").from(randomIntBetween(0,numDocs/2 - 1)).to(randomIntBetween(numDocs/2, numDocs));
                break;
            case 4:
                f = FilterBuilders.boolFilter();
                int numClause = randomIntBetween(0,5);
                for (int i = 0; i < numClause; i++) {
                    ((BoolFilterBuilder)f).must(randomFilterBuilder(numDocs, depth -1));
                }
                numClause = randomIntBetween(0,5);
                for (int i = 0; i < numClause; i++) {
                    ((BoolFilterBuilder)f).should(randomFilterBuilder(numDocs, depth -1));
                }
                numClause = randomIntBetween(0,5);
                for (int i = 0; i < numClause; i++) {
                    ((BoolFilterBuilder)f).mustNot(randomFilterBuilder(numDocs, depth -1));
                }
                break;
            case 5:
                f = FilterBuilders.andFilter();
                numClause = randomIntBetween(0,5);
                for (int i = 0; i < numClause; i++) {
                    ((AndFilterBuilder)f).add(randomFilterBuilder(numDocs, depth -1));
                }
                break;
            case 6:
                f = FilterBuilders.orFilter();
                numClause = randomIntBetween(0,5);
                for (int i = 0; i < numClause; i++) {
                    ((OrFilterBuilder)f).add(randomFilterBuilder(numDocs, depth -1));
                }
                break;
            case 7:
                f = FilterBuilders.notFilter(randomFilterBuilder(numDocs, depth -1));
                break;
            default:
                f = FilterBuilders.termFilter("field1", English.intToEnglish(randomIntBetween(0,numDocs)));
                break;
        }

        return f;
    }


    @Test
    /**
     * This test simply checks to make sure nothing crashes.  Unsure how best to validate the Profile response...
     */
    public void testProfileQuery() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder().put(indexSettings());
        createIndex("test");
        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource(
                    "field1", English.intToEnglish(i),
                    "field2", i
            );
        }

        indexRandom(true, docs);
        ensureGreen();
        int iters = between(20, 100);
        for (int i = 0; i < iters; i++) {
            QueryBuilder q = randomQueryBuilder(numDocs, 3);

            SearchResponse resp = client().prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH).execute().actionGet();
            assertNotNull(resp.getProfile());

        }
    }

    @Test
    /**
     * This test verifies that the output is reasonable (timing is equal, etc) for a simple, non-nested query
     */
    public void testSimpleMatch() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder().put(indexSettings());
        createIndex("test");
        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource(
                    "field1", English.intToEnglish(i),
                    "field2", i
            );
        }

        indexRandom(true, docs);
        ensureGreen();

        QueryBuilder q = QueryBuilders.matchQuery("field1", "one");

        SearchResponse resp = client().prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH).execute().actionGet();
        Profile p = resp.getProfile();
        assertNotNull(p);
        assertEquals(p.getComponents().size(), 0);
        assertEquals(p.getClassName(), "TermQuery");
        assertEquals(p.getLuceneDetails(), "field1:one");
        assertTrue(p.time() > 0);
        assertEquals(p.time(), p.totalTime());
    }

    @Test
    /**
     * This test verifies that the output is reasonable (non-zero times, etc) for a non-nested query
     */
    public void testBool() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder().put(indexSettings());
        createIndex("test");
        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource(
                    "field1", English.intToEnglish(i),
                    "field2", i
            );
        }

        indexRandom(true, docs);
        ensureGreen();

        QueryBuilder q = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("field1", "one")).must(QueryBuilders.matchQuery("field1", "two"));

        SearchResponse resp = client().prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH).execute().actionGet();
        Profile p = resp.getProfile();

        assertNotNull(p);
        assertEquals(p.getComponents().size(), 2);
        assertEquals(p.getClassName(), "BooleanQuery");
        assertEquals(p.getLuceneDetails(), "+field1:one +field1:two");
        assertEquals(p.time(), p.totalTime());
        assertTrue(p.time() > 0);


        Profile first = p.getComponents().get(0);
        System.out.println(p.totalTime());
        System.out.println(first.totalTime());
        assertEquals(first.getComponents().size(), 0);
        assertEquals(first.getClassName(), "TermQuery");
        assertEquals(first.getLuceneDetails(), "field1:one");
        assertEquals(first.totalTime(), p.totalTime());
        assertTrue(first.time() < first.totalTime());

        Profile second = p.getComponents().get(1);
        assertEquals(second.getComponents().size(), 0);
        assertEquals(second.getClassName(), "TermQuery");
        assertEquals(second.getLuceneDetails(), "field1:two");
        assertEquals(second.totalTime(), p.totalTime());
        assertTrue(second.time() < first.totalTime());
    }


}


