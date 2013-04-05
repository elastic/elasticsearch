package org.elasticsearch.test.integration.search.facet;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.RandomStringGenerator;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.TermsFacetBuilder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.*;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class ExtendedFacetsTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder().put("index.number_of_shards", numberOfShards()).put("index.number_of_replicas", 0).build();
        for (int i = 0; i < numberOfNodes(); i++) {
            startNode("node" + i, settings);
        }
        client = getClient();
    }

    protected int numberOfShards() {
        return 1;
    }

    protected int numberOfNodes() {
        return 1;
    }

    protected int numDocs() {
        return 2500;
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node0");
    }

    @Test
    public void testTermFacet_stringFields() throws Throwable {
        client.admin().indices().prepareDelete().execute().actionGet();
        client.admin().indices().prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject()
                        .startObject("type1")
                        .startObject("properties")
                        .startObject("field1_concrete")
                        .field("type", "string")
                        .field("index", "not_analyzed")
                        .startObject("fielddata")
                        .field("format", "concrete_bytes")
                        .endObject()
                        .endObject()
                        .startObject("field1_paged")
                        .field("type", "string")
                        .field("index", "not_analyzed")
                        .startObject("fielddata")
                        .field("format", "paged_bytes")
                        .endObject()
                        .endObject()
                        .startObject("field1_fst")
                        .field("type", "string")
                        .field("index", "not_analyzed")
                        .startObject("fielddata")
                        .field("format", "fst")
                        .endObject()
                        .endObject()
                        .startObject("field2")
                        .field("type", "string")
                        .field("index", "not_analyzed")
                        .endObject()
                        .endObject()
                        .endObject().endObject()
                )
                .execute().actionGet();


        long seed = System.currentTimeMillis(); // LuceneTestCase...
        try {
            Random random = new Random(seed);
            int numOfValuesField1 = 200;
            String[] field1Values = new String[numOfValuesField1];
            for (int i = 0; i < numOfValuesField1; i++) {
                field1Values[i] = RandomStringGenerator.random(10, 0, 0, true, true, null, random);
            }

            int numOfQueryValues = 50;
            String[] queryValues = new String[numOfQueryValues];
            for (int i = 0; i < numOfQueryValues; i++) {
                queryValues[i] = RandomStringGenerator.random(5, 0, 0, true, true, null, random);
            }

            Map<String, Map<String, Integer>> controlDataSet = new HashMap<String, Map<String, Integer>>();
            for (int i = 1; i <= numDocs(); i++) {
                String field1Val = field1Values[random.nextInt(numOfValuesField1)];
                String queryVal = queryValues[random.nextInt(numOfQueryValues)];
                client.prepareIndex("test", "type1", Integer.toString(i))
                        .setSource(jsonBuilder().startObject()
                                .field("field1_concrete", field1Val)
                                .field("field1_paged", field1Val)
                                .field("field1_fst", field1Val)
                                .field("field2", queryVal)
                                .endObject())
                        .execute().actionGet();
                Map<String, Integer> controlField1Facets = controlDataSet.get(queryVal);
                if (controlField1Facets == null) {
                    controlField1Facets = new HashMap<String, Integer>();
                    controlDataSet.put(queryVal, controlField1Facets);
                }
                Integer controlCount = controlField1Facets.get(field1Val);
                if (controlCount == null) {
                    controlCount = 0;
                }
                controlField1Facets.put(field1Val, ++controlCount);
            }

            client.admin().indices().prepareRefresh().execute().actionGet();
            String[] facetFields = new String[]{"field1_concrete", "field1_paged", "field1_fst"};
            TermsFacet.ComparatorType[] compTypes = TermsFacet.ComparatorType.values();
            for (String facetField : facetFields) {
                for (String queryVal : controlDataSet.keySet()) {
                    TermsFacet.ComparatorType  compType = compTypes[random.nextInt(compTypes.length)];
                    int size;
                    if (compType == TermsFacet.ComparatorType.COUNT || compType == TermsFacet.ComparatorType.REVERSE_COUNT) {
                        // Should always equal to number of unique values b/c of the top n terms problem in case sorting by facet count.
                        size = numOfValuesField1;
                    } else {
                        size = random.nextInt(numOfValuesField1);
                    }

                    Map<String, Integer> controlFacets = controlDataSet.get(queryVal);


                    TermsFacetBuilder termsFacetBuilder = FacetBuilders.termsFacet("facet1").field(facetField)
                            .order(compType).size(size);
                    if (random.nextBoolean()) {
                        termsFacetBuilder.executionHint("map");
                    }
                    List<String> excludes = new ArrayList<String>();
                    if (random.nextBoolean()) {
                        int numExludes = random.nextInt(5) + 1;
                        List<String> facetValues = new ArrayList<String>(controlFacets.keySet());
                        for (int i = 0; i < numExludes; i++) {
                            excludes.add(facetValues.get(random.nextInt(facetValues.size())));
                        }
                        termsFacetBuilder.exclude(excludes.toArray());
                    }
                    String regex = null;
                    if (random.nextBoolean()) {
                        List<String> facetValues = new ArrayList<String>(controlFacets.keySet());
                        regex = facetValues.get(random.nextInt(facetValues.size()));
                        regex = "^" + regex.substring(0, regex.length() / 2) + ".*";
                        termsFacetBuilder.regex(regex);
                    }

                    boolean allTerms = random.nextInt(10) == 3;
                    termsFacetBuilder.allTerms(allTerms);

                    SearchResponse response = client.prepareSearch("test")
                            .setQuery(QueryBuilders.termQuery("field2", queryVal))
                            .addFacet(termsFacetBuilder)
                            .execute().actionGet();
                    TermsFacet termsFacet = response.getFacets().facet("facet1");
                    List<Tuple<Text, Integer>> controlFacetEntries = getControlFacetEntries(field1Values, controlFacets, size, compType, excludes, regex, allTerms);
                    String reason = String.format("query: %s field: %s size: %d order: %s all_terms: %s regex: %s excludes: %s", queryVal, facetField, size, compType, allTerms, regex, excludes);
                    assertThat(reason, termsFacet.getEntries().size(), equalTo(controlFacetEntries.size()));
                    for (int i = 0; i < controlFacetEntries.size(); i++) {
                        assertThat(reason, termsFacet.getEntries().get(i).getTerm(), equalTo(controlFacetEntries.get(i).v1()));
                        assertThat(reason, termsFacet.getEntries().get(i).getCount(), equalTo(controlFacetEntries.get(i).v2()));
                    }
                }
            }
        } catch (Throwable t) {
            logger.error("Failed with seed:" + seed);
            throw t;
        }
    }

    private List<Tuple<Text, Integer>> getControlFacetEntries(String[] field1Values, Map<String, Integer> controlFacets, int size, TermsFacet.ComparatorType sort, List<String> excludes, String regex, boolean allTerms) {
        Pattern pattern = null;
        if (regex != null) {
            pattern = Regex.compile(regex, null);
        }

        List<Tuple<Text, Integer>> entries = new ArrayList<Tuple<Text, Integer>>();
        for (Map.Entry<String, Integer> e : controlFacets.entrySet()) {
            if (excludes.contains(e.getKey())) {
                continue;
            }
            if (pattern != null && !pattern.matcher(e.getKey()).matches()) {
                continue;
            }

            entries.add(new Tuple<Text, Integer>(new StringText(e.getKey()), e.getValue()));
        }

        if (allTerms) {
            for (String field1Value : field1Values) {
                if (!controlFacets.containsKey(field1Value)) {
                    if (excludes.contains(field1Value)) {
                        continue;
                    }
                    if (pattern != null && !pattern.matcher(field1Value).matches()) {
                        continue;
                    }

                    entries.add(new Tuple<Text, Integer>(new StringText(field1Value), 0));
                }
            }
        }

        switch (sort) {
            case COUNT:
                Collections.sort(entries, count);
                break;
            case REVERSE_COUNT:
                Collections.sort(entries, count_reverse);
                break;
            case TERM:
                Collections.sort(entries, term);
                break;
            case REVERSE_TERM:
                Collections.sort(entries, term_reverse);
                break;
        }
        return size >= entries.size() ? entries : entries.subList(0, size);
    }

    private final static COUNT count = new COUNT();
    private final static COUNT_REVERSE count_reverse = new COUNT_REVERSE();
    private final static TERM term = new TERM();
    private final static TERM_REVERSE term_reverse = new TERM_REVERSE();

    private static class COUNT implements Comparator<Tuple<Text, Integer>> {

        @Override
        public int compare(Tuple<Text, Integer> o1, Tuple<Text, Integer> o2) {
            int cmp = o2.v2() - o1.v2();
            if (cmp != 0) {
                return cmp;
            }
            cmp =  o2.v1().compareTo(o1.v1());
            if (cmp != 0) {
                return cmp;
            }
            return System.identityHashCode(o2) - System.identityHashCode(o1);
        }

    }

    private static class COUNT_REVERSE implements Comparator<Tuple<Text, Integer>> {

        @Override
        public int compare(Tuple<Text, Integer> o1, Tuple<Text, Integer> o2) {
            return -count.compare(o1, o2);
        }

    }

    private static class TERM implements Comparator<Tuple<Text, Integer>> {

        @Override
        public int compare(Tuple<Text, Integer> o1, Tuple<Text, Integer> o2) {
            return o1.v1().compareTo(o2.v1());
        }

    }

    private static class TERM_REVERSE implements Comparator<Tuple<Text, Integer>> {

        @Override
        public int compare(Tuple<Text, Integer> o1, Tuple<Text, Integer> o2) {
            return -term.compare(o1, o2);
        }

    }

}
