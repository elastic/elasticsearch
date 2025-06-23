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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractBuilderTestCase;
import org.elasticsearch.xpack.wildcard.Wildcard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TermsQueryTests extends AbstractBuilderTestCase {

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(Wildcard.class);
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge("_doc", new CompressedXContent(org.elasticsearch.common.Strings.format("""
            {
              "properties": {
                "mapped_wildcard": {
                  "type": "wildcard"
                }
              }
            }""")), MapperService.MergeReason.MAPPING_UPDATE);
    }

    public void testSingleDuplicateTerms() throws IOException {
        String[] duplicates = new String[1023];
        Arrays.fill(duplicates, "duplicate");
        QueryBuilder termsQueryBuilder = new TermsQueryBuilder("mapped_wildcard", duplicates);
        termsQueryBuilder = termsQueryBuilder.rewrite(createQueryRewriteContext());
        Query actual = termsQueryBuilder.toQuery(createSearchExecutionContext());

        QueryBuilder queryBuilder = new TermsQueryBuilder("mapped_wildcard", "duplicate");
        queryBuilder = queryBuilder.rewrite(createQueryRewriteContext());
        Query expected = queryBuilder.toQuery(createSearchExecutionContext());

        assertEquals(expected, actual);
    }

    public void testMultiDuplicateTerms() throws IOException {
        int numTerms = randomIntBetween(2, 10);
        List<String> randomTerms = new ArrayList<>(numTerms);
        for (int i = 0; i < numTerms; ++i) {
            randomTerms.add(randomAlphaOfLengthBetween(1, 1024));
        }
        int totalTerms = randomIntBetween(numTerms * 5, 1023);
        String[] duplicates = new String[totalTerms];
        for (int i = 0; i < numTerms; ++i) {
            duplicates[i] = randomTerms.get(i);
        }
        for (int i = numTerms; i < totalTerms; ++i) {
            duplicates[i] = randomTerms.get(randomIntBetween(0, numTerms - 1));
        }

        QueryBuilder termsQueryBuilder = new TermsQueryBuilder("mapped_wildcard", duplicates);
        termsQueryBuilder = termsQueryBuilder.rewrite(createQueryRewriteContext());
        Query actual = termsQueryBuilder.toQuery(createSearchExecutionContext());

        Set<String> ordered = new HashSet<>(randomTerms);
        QueryBuilder queryBuilder = new TermsQueryBuilder("mapped_wildcard", ordered.toArray(new String[0]));
        queryBuilder = queryBuilder.rewrite(createQueryRewriteContext());
        Query expected = queryBuilder.toQuery(createSearchExecutionContext());

        assertEquals(expected, actual);
    }
}
