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

package org.elasticsearch.rest.action.search;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class RestSearchActionTests extends ESTestCase {

    public void testSliceQueryParameterAll() throws Exception {
        int idInt = 0;
        int maxInt = 135;
        String id = Integer.toString(idInt);
        String max = Integer.toString(maxInt);
        String field = "FIELD_NAME";
        Object[] sliceValues = {id,max,field}; 
        
        Map<String, String> params = new HashMap<>();
        params.put("slice", Strings.arrayToCommaDelimitedString(sliceValues) );
        
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        SliceBuilder sliceBuilder = RestSearchAction.parseSlice(restRequest.paramAsStringArray("slice", null));
        
        assertNotNull(sliceBuilder);
        assertEquals(idInt, sliceBuilder.getId());
        assertEquals(maxInt, sliceBuilder.getMax());
        assertEquals(field, sliceBuilder.getField());
    }
    
    public void testSliceQueryParameterNoField() throws Exception {
        int idInt = 0;
        int maxInt = 135;
        String id = Integer.toString(idInt);
        String max = Integer.toString(maxInt);
        Object[] sliceValues = {id,max}; 
        
        Map<String, String> params = new HashMap<>();
        params.put("slice", Strings.arrayToCommaDelimitedString(sliceValues) );
        
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        SliceBuilder sliceBuilder = RestSearchAction.parseSlice(restRequest.paramAsStringArray("slice", null));
        
        assertNotNull(sliceBuilder);
        assertEquals(idInt, sliceBuilder.getId());
        assertEquals(maxInt, sliceBuilder.getMax());
        assertEquals(sliceBuilder.getField(), UidFieldMapper.NAME);
    }
    
    public void testSliceQueryParameterIdNotInt() throws Exception {
        String id = "not_int";
        String max = Integer.toString(135);
        Object[] sliceValues = {id,max}; 
        
        Map<String, String> params = new HashMap<>();
        params.put("slice", Strings.arrayToCommaDelimitedString(sliceValues) );
        
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        try {
            RestSearchAction.parseSlice(restRequest.paramAsStringArray("slice", null));
        } catch(IllegalArgumentException e) {
            //expected
        }
        assertTrue(true);
    }
    
    public void testSliceQueryParameterMaxNotInt() throws Exception {
        String id = Integer.toString(0);
        String max = "not_int";
        Object[] sliceValues = {id,max}; 
        
        Map<String, String> params = new HashMap<>();
        params.put("slice", Strings.arrayToCommaDelimitedString(sliceValues) );
        
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        try {
            RestSearchAction.parseSlice(restRequest.paramAsStringArray("slice", null));
        } catch(IllegalArgumentException e) {
            //expected
        }
        assertTrue(true);
    }

}
