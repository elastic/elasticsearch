/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.wildcard;


import org.apache.lucene.search.WildcardQuery;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.wildcard.mapper.WildcardFieldMapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.SuiteScopeTestCase
public class WildcardFieldTests extends ESIntegTestCase {

    static final int MAX_FIELD_LENGTH = 100;
    static final String TOO_BIG_PREFIX = "toobig";
    static String randomABString(int minLength) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < minLength) {
            if (randomBoolean()) {
                sb.append("a");
            } else {
                sb.append("b");
            }
        }
        return sb.toString();
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        
        assertAcked(prepareCreate("idx").setMapping( buildMapping()).get());
        
        List<IndexRequestBuilder> builders = new ArrayList<>();
        int numDocs = 100;
        for (int i = 0; i < numDocs; i++) {            
            // TODO An issue with array-based matching ... https://github.com/elastic/elasticsearch/pull/49993#issuecomment-567531755
//            if(i%10==0) {
            if(false) {
                // One in ten docs use an array, not a single value
                String data1 = randomABString(1+randomInt(MAX_FIELD_LENGTH));            
                String data2 = randomABString(1+randomInt(MAX_FIELD_LENGTH));            
                XContentBuilder source = jsonBuilder().startObject().array("data", data1, data2)
                        .endObject();
                builders.add(client().prepareIndex("idx").setId("" + i).setSource(source));
            } else {
                String data = randomABString(1+randomInt(MAX_FIELD_LENGTH));            
                XContentBuilder source = jsonBuilder().startObject().field("data", data).endObject();
                builders.add(client().prepareIndex("idx").setId("" + i).setSource(source));                
            }
        }
        // Add a doc to test ignore_above
        String data = TOO_BIG_PREFIX + randomABString(MAX_FIELD_LENGTH);            
        XContentBuilder source = jsonBuilder().startObject().field("data", data).endObject();
        builders.add(client().prepareIndex("idx").setSource(source));
        
        indexRandom(true, builders);
        ensureSearchable();
    }
    
    public static Map<String, Object> buildMapping() {
        Map<String, Object> fields = new HashMap<>();

        Map<String, Object> rootFieldDef = new HashMap<>();
        fields.put("data", rootFieldDef);
        {
            rootFieldDef.put("type", WildcardFieldMapper.CONTENT_TYPE);
            rootFieldDef.put("ignore_above", MAX_FIELD_LENGTH);
            Map<String, Object> subFields = new HashMap<>();
            rootFieldDef.put("fields", subFields);
            {
                Map<String, Object> subFieldDef1 = new HashMap<>();
                subFields.put("asKeyword", subFieldDef1);
                {               
                    subFieldDef1.put("type", "keyword");
                    subFieldDef1.put("ignore_above", MAX_FIELD_LENGTH);
                }
                Map<String, Object> subFieldDef2 = new HashMap<>();
                subFields.put("asPosWildcard", subFieldDef2);
                {               
                    subFieldDef2.put("type", WildcardFieldMapper.CONTENT_TYPE);
                    subFieldDef2.put("ignore_above", MAX_FIELD_LENGTH);
                    subFieldDef2.put("match_type", WildcardFieldMapper.Defaults.MATCH_TYPE_POSITION);
                }   
                Map<String, Object> subFieldDef3 = new HashMap<>();
                subFields.put("asBinaryDV", subFieldDef3);
                {               
                    subFieldDef3.put("type", WildcardFieldMapper.CONTENT_TYPE);
                    subFieldDef3.put("ignore_above", MAX_FIELD_LENGTH);
                    subFieldDef3.put("match_type", WildcardFieldMapper.Defaults.MATCH_TYPE_BINARY_DOC_VALUES);
                }                    
            }
        }
        return Collections.singletonMap("properties", fields);
    }    

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.add(Wildcard.class);
        plugins.add(LocalStateCompositeXPackPlugin.class);
        return plugins;
    }


    public void testKeywordAndWildcardSearchesConcur() throws Exception {
        
        int numSearches = 100;
        for (int i = 0; i < numSearches; i++) {
            String randomWildcardPattern = getRandomWildcardPattern();
            SearchResponse wildcardResponse = client().prepareSearch("idx")
                    .setQuery(new WildcardQueryBuilder("data", randomWildcardPattern))
                    .get();

            assertSearchResponse(wildcardResponse);
            
            SearchResponse keywordResponse = client().prepareSearch("idx")
                    .setQuery(new WildcardQueryBuilder("data.asKeyword", randomWildcardPattern))
                    .get();
            assertSearchResponse(keywordResponse);
            assertThat(wildcardResponse.getHits().getTotalHits().value, equalTo(keywordResponse.getHits().getTotalHits().value));
            
        }
    }
    
    public void testKeywordAndBInaryDvWildcardSearchesConcur() throws Exception {
        
        int numSearches = 100;
        for (int i = 0; i < numSearches; i++) {
            String randomWildcardPattern = getRandomWildcardPattern();
            SearchResponse wildcardResponse = client().prepareSearch("idx")
                    .setQuery(new WildcardQueryBuilder("data.asBinaryDV", randomWildcardPattern))
                    .get();

            assertSearchResponse(wildcardResponse);
            
            SearchResponse keywordResponse = client().prepareSearch("idx")
                    .setQuery(new WildcardQueryBuilder("data.asKeyword", randomWildcardPattern))
                    .get();
            assertSearchResponse(keywordResponse);
            assertThat(wildcardResponse.getHits().getTotalHits().value, equalTo(keywordResponse.getHits().getTotalHits().value));
            
        }
    }
        
    
    public void testWildcardPosAndKeywordSearchesConcur() throws Exception {
        
        int numSearches = 100;
        for (int i = 0; i < numSearches; i++) {
            String randomWildcardPattern = getRandomWildcardPattern();
            SearchResponse wildcardResponse = client().prepareSearch("idx")
                    .setQuery(new WildcardQueryBuilder("data.asPosWildcard", randomWildcardPattern))
                    .get();

            assertSearchResponse(wildcardResponse);
            
            SearchResponse keywordResponse = client().prepareSearch("idx")
                    .setQuery(new WildcardQueryBuilder("data.asKeyword", randomWildcardPattern))
                    .get();
            assertSearchResponse(keywordResponse);
            assertThat(wildcardResponse.getHits().getTotalHits().value, equalTo(keywordResponse.getHits().getTotalHits().value));
            
        }
    }    

    public void testIgnoreAbove() throws Exception {
        SearchResponse wildcardResponse = client().prepareSearch("idx").setQuery(new WildcardQueryBuilder("data", TOO_BIG_PREFIX + "*"))
                .get();
        assertSearchResponse(wildcardResponse);
        assertThat(wildcardResponse.getHits().getTotalHits().value, equalTo(0L));
    }
    
    private void randomSyntaxChar(StringBuilder sb) {
        switch (randomInt(3)) {
        case 0:
            sb.append(WildcardQuery.WILDCARD_CHAR);
            break;
        case 1:
            sb.append(WildcardQuery.WILDCARD_STRING);
            break;
        case 2:
            sb.append(WildcardQuery.WILDCARD_ESCAPE);
            sb.append(WildcardQuery.WILDCARD_STRING);
            break;
        case 3:
            sb.append(WildcardQuery.WILDCARD_ESCAPE);
            sb.append(WildcardQuery.WILDCARD_CHAR);
            break;
        }
    }

    private String getRandomWildcardPattern() {
        StringBuilder sb = new StringBuilder();
        
        int numFragments = 1+randomInt(4);
        
        if(randomInt(10)==1) {
            randomSyntaxChar(sb);
        }
        for (int i = 0; i < numFragments; i++) {
            if(i>0) {
                randomSyntaxChar(sb);
            }
            sb.append(randomABString(1+randomInt(6)));            
        }
        if(randomInt(10)==1) {
            randomSyntaxChar(sb);
        } 
        return sb.toString();
    }

}
