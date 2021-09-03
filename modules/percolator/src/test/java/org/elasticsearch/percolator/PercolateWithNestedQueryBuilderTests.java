/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.percolator;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;

public class PercolateWithNestedQueryBuilderTests extends PercolateQueryBuilderTests {

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        super.initializeAdditionalMappings(mapperService);
        mapperService.merge("_doc", new CompressedXContent(Strings.toString(PutMappingRequest.simpleMapping(
                "some_nested_object", "type=nested"))), MapperService.MergeReason.MAPPING_UPDATE);
    }

    public void testDetectsNestedDocuments() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        PercolateQueryBuilder builder = new PercolateQueryBuilder(queryField,
                new BytesArray("{ \"foo\": \"bar\" }"), XContentType.JSON);
        QueryBuilder rewrittenBuilder = rewriteAndFetch(builder, searchExecutionContext);
        PercolateQuery query = (PercolateQuery) rewrittenBuilder.toQuery(searchExecutionContext);
        assertFalse(query.excludesNestedDocs());

        builder = new PercolateQueryBuilder(queryField,
                new BytesArray("{ \"foo\": \"bar\", \"some_nested_object\": [ { \"baz\": 42 } ] }"), XContentType.JSON);
        rewrittenBuilder = rewriteAndFetch(builder, searchExecutionContext);
        query = (PercolateQuery) rewrittenBuilder.toQuery(searchExecutionContext);
        assertTrue(query.excludesNestedDocs());
    }
}
