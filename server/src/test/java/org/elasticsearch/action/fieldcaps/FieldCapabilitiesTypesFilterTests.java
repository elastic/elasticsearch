/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.plugins.FieldPredicate;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FieldCapabilitiesTypesFilterTests extends MapperServiceTestCase {

    public void testTypesFilterExcludesObjectFields() throws IOException {
        // Create a mapping with an object field containing text and numeric fields
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("properties");
            {
                b.startObject("product");
                {
                    b.field("type", "object");
                    b.startObject("properties");
                    {
                        b.startObject("name");
                        b.field("type", "text");
                        b.endObject();
                        
                        b.startObject("price");
                        b.field("type", "float");
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
                
                b.startObject("description");
                b.field("type", "text");
                b.endObject();
            }
            b.endObject();
        }));

        SearchExecutionContext context = createSearchExecutionContext(mapperService);
        
        // Mock IndexShard for field info
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.getFieldInfos()).thenReturn(null);
        
        // Test with types filter for "text" only
        Predicate<String> fieldNameFilter = field -> true;
        String[] filters = {};
        String[] types = {"text"};
        FieldPredicate fieldPredicate = FieldPredicate.ACCEPT_ALL;
        
        Map<String, IndexFieldCapabilities> result = FieldCapabilitiesFetcher.retrieveFieldCaps(
            context,
            fieldNameFilter,
            filters,
            types,
            fieldPredicate,
            indexShard,
            true
        );
        
        // Should include text fields
        assertTrue("Should include product.name (text field)", result.containsKey("product.name"));
        assertTrue("Should include description (text field)", result.containsKey("description"));
        
        // Should NOT include object field
        assertFalse("Should NOT include product (object field) when filtering for text types", result.containsKey("product"));
        
        // Should NOT include float field
        assertFalse("Should NOT include product.price (float field)", result.containsKey("product.price"));
    }
    
    public void testTypesFilterIncludesObjectWhenRequested() throws IOException {
        // Create a mapping with an object field
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("properties");
            {
                b.startObject("product");
                {
                    b.field("type", "object");
                    b.startObject("properties");
                    {
                        b.startObject("name");
                        b.field("type", "text");
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        SearchExecutionContext context = createSearchExecutionContext(mapperService);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.getFieldInfos()).thenReturn(null);
        
        // Test with types filter including "object"
        Predicate<String> fieldNameFilter = field -> true;
        String[] filters = {};
        String[] types = {"text", "object"};
        FieldPredicate fieldPredicate = FieldPredicate.ACCEPT_ALL;
        
        Map<String, IndexFieldCapabilities> result = FieldCapabilitiesFetcher.retrieveFieldCaps(
            context,
            fieldNameFilter,
            filters,
            types,
            fieldPredicate,
            indexShard,
            true
        );
        
        // Should include both text and object fields
        assertTrue("Should include product.name (text field)", result.containsKey("product.name"));
        assertTrue("Should include product (object field) when object is in types filter", result.containsKey("product"));
    }
    
    public void testTypesFilterWithNestedFields() throws IOException {
        // Create a mapping with nested fields
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("properties");
            {
                b.startObject("comments");
                {
                    b.field("type", "nested");
                    b.startObject("properties");
                    {
                        b.startObject("text");
                        b.field("type", "text");
                        b.endObject();
                        
                        b.startObject("rating");
                        b.field("type", "integer");
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        SearchExecutionContext context = createSearchExecutionContext(mapperService);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.getFieldInfos()).thenReturn(null);
        
        // Test with types filter for "text" only
        Predicate<String> fieldNameFilter = field -> true;
        String[] filters = {};
        String[] types = {"text"};
        FieldPredicate fieldPredicate = FieldPredicate.ACCEPT_ALL;
        
        Map<String, IndexFieldCapabilities> result = FieldCapabilitiesFetcher.retrieveFieldCaps(
            context,
            fieldNameFilter,
            filters,
            types,
            fieldPredicate,
            indexShard,
            true
        );
        
        // Should include text field
        assertTrue("Should include comments.text (text field)", result.containsKey("comments.text"));
        
        // Should NOT include nested field when filtering for text
        assertFalse("Should NOT include comments (nested field) when filtering for text types", result.containsKey("comments"));
        
        // Should NOT include integer field
        assertFalse("Should NOT include comments.rating (integer field)", result.containsKey("comments.rating"));
    }
    
    public void testNoTypesFilterIncludesAllFields() throws IOException {
        // Create a mapping with mixed field types
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("properties");
            {
                b.startObject("product");
                {
                    b.field("type", "object");
                    b.startObject("properties");
                    {
                        b.startObject("name");
                        b.field("type", "text");
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        SearchExecutionContext context = createSearchExecutionContext(mapperService);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.getFieldInfos()).thenReturn(null);
        
        // Test with no types filter (empty array)
        Predicate<String> fieldNameFilter = field -> true;
        String[] filters = {};
        String[] types = {};  // No types filter
        FieldPredicate fieldPredicate = FieldPredicate.ACCEPT_ALL;
        
        Map<String, IndexFieldCapabilities> result = FieldCapabilitiesFetcher.retrieveFieldCaps(
            context,
            fieldNameFilter,
            filters,
            types,
            fieldPredicate,
            indexShard,
            true
        );
        
        // Should include all fields when no types filter
        assertTrue("Should include product.name when no types filter", result.containsKey("product.name"));
        assertTrue("Should include product (object) when no types filter", result.containsKey("product"));
    }
    
    public void testTypesFilterWithExcludeParentFilter() throws IOException {
        // Create a mapping with object fields
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("properties");
            {
                b.startObject("product");
                {
                    b.field("type", "object");
                    b.startObject("properties");
                    {
                        b.startObject("name");
                        b.field("type", "text");
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        SearchExecutionContext context = createSearchExecutionContext(mapperService);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.getFieldInfos()).thenReturn(null);
        
        // Test with types filter and -parent filter combined
        Predicate<String> fieldNameFilter = field -> true;
        String[] filters = {"-parent"};  // Exclude parent objects
        String[] types = {"text", "object"};  // Include both text and object types
        FieldPredicate fieldPredicate = FieldPredicate.ACCEPT_ALL;
        
        Map<String, IndexFieldCapabilities> result = FieldCapabilitiesFetcher.retrieveFieldCaps(
            context,
            fieldNameFilter,
            filters,
            types,
            fieldPredicate,
            indexShard,
            true
        );
        
        // Should include text field
        assertTrue("Should include product.name (text field)", result.containsKey("product.name"));
        
        // Should NOT include object field even though it's in types filter, because -parent overrides
        assertFalse("Should NOT include product when -parent filter is used", result.containsKey("product"));
    }
}