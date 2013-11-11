package org.elasticsearch.search.facet;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.TermsFacetBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.*;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class ExtendedFacetsTests extends ElasticsearchIntegrationTest {

    @Override
    public Settings indexSettings() {
        return ImmutableSettings.builder()
                .put("index.number_of_shards", numberOfShards())
                .put("index.number_of_replicas", 0)
                .build();
    }

    protected int numberOfShards() {
        return 1;
    }

    protected int numDocs() {
        return 2500;
    }


    @Test
    @Slow
    public void testTermFacet_stringFields() throws Throwable {
        prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject()
                        .startObject("type1")
                        .startObject("properties")
                        .startObject("field1_paged")
                        .field("type", "string")
                        .field("index", "not_analyzed")
                        .startObject("fielddata")
                        .field("format", "paged_bytes")
                        .field("loading", randomBoolean() ? "eager" : "lazy")
                        .endObject()
                        .endObject()
                        .startObject("field1_fst")
                        .field("type", "string")
                        .field("index", "not_analyzed")
                        .startObject("fielddata")
                        .field("format", "fst")
                        .field("loading", randomBoolean() ? "eager" : "lazy")
                        .endObject()
                        .endObject()
                        .startObject("field2")
                        .field("type", "string")
                        .field("index", "not_analyzed")
                        .startObject("fielddata")
                        .field("format", "fst")
                        .field("loading", randomBoolean() ? "eager" : "lazy")
                        .endObject()
                        .endObject()
                        .startObject("q_field")
                        .field("type", "string")
                        .field("index", "not_analyzed")
                        .endObject()
                        .endObject()
                        .endObject().endObject()
                )
                .execute().actionGet();


        Random random = getRandom();
        int numOfQueryValues = 50;
        String[] queryValues = new String[numOfQueryValues];
        for (int i = 0; i < numOfQueryValues; i++) {
            queryValues[i] = randomAsciiOfLength(5);
        }

        Set<String> uniqueValuesSet = new HashSet<String>();
        int numOfVals = 400;
        for (int i = 0; i < numOfVals; i++) {
            uniqueValuesSet.add(randomAsciiOfLength(10));
        }
        String[] allUniqueFieldValues = uniqueValuesSet.toArray(new String[uniqueValuesSet.size()]);

        Set<String> allField1Values = new HashSet<String>();
        Set<String> allField1AndField2Values = new HashSet<String>();
        Map<String, Map<String, Integer>> queryValToField1FacetEntries = new HashMap<String, Map<String, Integer>>();
        Map<String, Map<String, Integer>> queryValToField1and2FacetEntries = new HashMap<String, Map<String, Integer>>();
        for (int i = 1; i <= numDocs(); i++) {
            int numField1Values = random.nextInt(17);
            Set<String> field1Values = new HashSet<String>(numField1Values);
            for (int j = 0; j <= numField1Values; j++) {
                boolean added = false;
                while (!added) {
                    added = field1Values.add(allUniqueFieldValues[random.nextInt(numOfVals)]);
                }
            }
            allField1Values.addAll(field1Values);
            allField1AndField2Values.addAll(field1Values);
            String field2Val = allUniqueFieldValues[random.nextInt(numOfVals)];
            allField1AndField2Values.add(field2Val);
            String queryVal = queryValues[random.nextInt(numOfQueryValues)];
            client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource(jsonBuilder().startObject()
                            .field("field1_paged", field1Values)
                            .field("field1_fst", field1Values)
                            .field("field2", field2Val)
                            .field("q_field", queryVal)
                            .endObject())
                    .execute().actionGet();

            if (random.nextInt(2000) == 854) {
                client().admin().indices().prepareFlush("test").execute().actionGet();
            }
            addControlValues(queryValToField1FacetEntries, field1Values, queryVal);
            addControlValues(queryValToField1and2FacetEntries, field1Values, queryVal);
            addControlValues(queryValToField1and2FacetEntries, field2Val, queryVal);
        }

        client().admin().indices().prepareRefresh().execute().actionGet();
        String[] facetFields = new String[]{"field1_paged", "field1_fst"};
        TermsFacet.ComparatorType[] compTypes = TermsFacet.ComparatorType.values();
        for (String facetField : facetFields) {
            for (String queryVal : queryValToField1FacetEntries.keySet()) {
                Set<String> allFieldValues;
                Map<String, Integer> queryControlFacets;
                TermsFacet.ComparatorType compType = compTypes[random.nextInt(compTypes.length)];
                TermsFacetBuilder termsFacetBuilder = FacetBuilders.termsFacet("facet1").order(compType);

                boolean useFields;
                if (random.nextInt(4) == 3) {
                    useFields = true;
                    queryControlFacets = queryValToField1and2FacetEntries.get(queryVal);
                    allFieldValues = allField1AndField2Values;
                    termsFacetBuilder.fields(facetField, "field2");
                } else {
                    queryControlFacets = queryValToField1FacetEntries.get(queryVal);
                    allFieldValues = allField1Values;
                    useFields = false;
                    termsFacetBuilder.field(facetField);
                }
                int size;
                if (numberOfShards() == 1 || compType == TermsFacet.ComparatorType.TERM || compType == TermsFacet.ComparatorType.REVERSE_TERM) {
                    size = random.nextInt(queryControlFacets.size());
                } else {
                    size = allFieldValues.size();
                }
                termsFacetBuilder.size(size);

                if (random.nextBoolean()) {
                    termsFacetBuilder.executionHint("map");
                }
                List<String> excludes = new ArrayList<String>();
                if (random.nextBoolean()) {
                    int numExcludes = random.nextInt(5) + 1;
                    List<String> facetValues = new ArrayList<String>(queryControlFacets.keySet());
                    for (int i = 0; i < numExcludes; i++) {
                        excludes.add(facetValues.get(random.nextInt(facetValues.size())));
                    }
                    termsFacetBuilder.exclude(excludes.toArray());
                }
                String regex = null;
                if (random.nextBoolean()) {
                    List<String> facetValues = new ArrayList<String>(queryControlFacets.keySet());
                    regex = facetValues.get(random.nextInt(facetValues.size()));
                    regex = "^" + regex.substring(0, regex.length() / 2) + ".*";
                    termsFacetBuilder.regex(regex);
                }

                boolean allTerms = random.nextInt(10) == 3;
                termsFacetBuilder.allTerms(allTerms);

                SearchResponse response = client().prepareSearch("test")
                        .setQuery(QueryBuilders.termQuery("q_field", queryVal))
                        .addFacet(termsFacetBuilder)
                        .execute().actionGet();
                TermsFacet actualFacetEntries = response.getFacets().facet("facet1");

                List<Tuple<Text, Integer>> expectedFacetEntries = getExpectedFacetEntries(allFieldValues, queryControlFacets, size, compType, excludes, regex, allTerms);
                String reason = String.format(Locale.ROOT, "query: [%s] field: [%s] size: [%d] order: [%s] all_terms: [%s] fields: [%s] regex: [%s] excludes: [%s]", queryVal, facetField, size, compType, allTerms, useFields, regex, excludes);
                assertThat(reason, actualFacetEntries.getEntries().size(), equalTo(expectedFacetEntries.size()));
                for (int i = 0; i < expectedFacetEntries.size(); i++) {
                    assertThat(reason, actualFacetEntries.getEntries().get(i).getTerm(), equalTo(expectedFacetEntries.get(i).v1()));
                    assertThat(reason, actualFacetEntries.getEntries().get(i).getCount(), equalTo(expectedFacetEntries.get(i).v2()));
                }
            }
        }
    }

    private void addControlValues(Map<String, Map<String, Integer>> queryValToFacetFieldEntries, String fieldVal, String queryVal) {
        Map<String, Integer> controlFieldFacets = queryValToFacetFieldEntries.get(queryVal);
        if (controlFieldFacets == null) {
            controlFieldFacets = new HashMap<String, Integer>();
            queryValToFacetFieldEntries.put(queryVal, controlFieldFacets);
        }
        Integer controlCount = controlFieldFacets.get(fieldVal);
        if (controlCount == null) {
            controlCount = 0;
        }
        controlFieldFacets.put(fieldVal, ++controlCount);
    }

    private void addControlValues(Map<String, Map<String, Integer>> queryValToFacetFieldEntries, Set<String> fieldValues, String queryVal) {
        for (String fieldValue : fieldValues) {
            addControlValues(queryValToFacetFieldEntries, fieldValue, queryVal);
        }
    }

    private List<Tuple<Text, Integer>> getExpectedFacetEntries(Set<String> fieldValues,
                                                               Map<String, Integer> controlFacetsField,
                                                               int size,
                                                               TermsFacet.ComparatorType sort,
                                                               List<String> excludes,
                                                               String regex,
                                                               boolean allTerms) {
        Pattern pattern = null;
        if (regex != null) {
            pattern = Regex.compile(regex, null);
        }

        List<Tuple<Text, Integer>> entries = new ArrayList<Tuple<Text, Integer>>();
        for (Map.Entry<String, Integer> e : controlFacetsField.entrySet()) {
            if (excludes.contains(e.getKey())) {
                continue;
            }
            if (pattern != null && !pattern.matcher(e.getKey()).matches()) {
                continue;
            }

            entries.add(new Tuple<Text, Integer>(new StringText(e.getKey()), e.getValue()));
        }

        if (allTerms) {
            for (String fieldValue : fieldValues) {
                if (!controlFacetsField.containsKey(fieldValue)) {
                    if (excludes.contains(fieldValue)) {
                        continue;
                    }
                    if (pattern != null && !pattern.matcher(fieldValue).matches()) {
                        continue;
                    }

                    entries.add(new Tuple<Text, Integer>(new StringText(fieldValue), 0));
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

    private final static Count count = new Count();
    private final static CountReverse count_reverse = new CountReverse();
    private final static Term term = new Term();
    private final static TermReverse term_reverse = new TermReverse();

    private static class Count implements Comparator<Tuple<Text, Integer>> {

        @Override
        public int compare(Tuple<Text, Integer> o1, Tuple<Text, Integer> o2) {
            int cmp = o2.v2() - o1.v2();
            if (cmp != 0) {
                return cmp;
            }
            cmp = o2.v1().compareTo(o1.v1());
            if (cmp != 0) {
                return cmp;
            }
            return System.identityHashCode(o2) - System.identityHashCode(o1);
        }

    }

    private static class CountReverse implements Comparator<Tuple<Text, Integer>> {

        @Override
        public int compare(Tuple<Text, Integer> o1, Tuple<Text, Integer> o2) {
            return -count.compare(o1, o2);
        }

    }

    private static class Term implements Comparator<Tuple<Text, Integer>> {

        @Override
        public int compare(Tuple<Text, Integer> o1, Tuple<Text, Integer> o2) {
            return o1.v1().compareTo(o2.v1());
        }

    }

    private static class TermReverse implements Comparator<Tuple<Text, Integer>> {

        @Override
        public int compare(Tuple<Text, Integer> o1, Tuple<Text, Integer> o2) {
            return -term.compare(o1, o2);
        }

    }

}
