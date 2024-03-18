/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.test.AbstractBuilderTestCase;

import java.io.IOException;
import java.util.Arrays;

public class TermsQueryTests extends AbstractBuilderTestCase {

    public void testDuplicateTerms() throws IOException {
        String[] duplicates = new String[1023];
        Arrays.fill(duplicates, "duplicate");
        TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder("mapped_wildcard", duplicates);
        termsQueryBuilder.rewrite(createQueryRewriteContext());
        Query query = termsQueryBuilder.toQuery(createSearchExecutionContext());
        query.toString();
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.getMapperRegistry().mapperParsers.put("wildcard", WildcardFieldMapper.PARSER);
        mapperService.merge("_doc", new CompressedXContent(org.elasticsearch.common.Strings.format("""
            {
              "properties": {
                "mapped_wildcard": {
                  "type": "wildcard"
                }
              }
            }""")), MapperService.MergeReason.MAPPING_UPDATE);
    }
}
