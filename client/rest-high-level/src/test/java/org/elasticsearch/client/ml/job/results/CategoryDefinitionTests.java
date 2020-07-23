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
package org.elasticsearch.client.ml.job.results;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CategoryDefinitionTests extends AbstractXContentTestCase<CategoryDefinition> {

    public static CategoryDefinition createTestInstance(String jobId) {
        CategoryDefinition categoryDefinition = new CategoryDefinition(jobId);
        categoryDefinition.setCategoryId(randomLong());
        if (randomBoolean()) {
            categoryDefinition.setPartitionFieldName(randomAlphaOfLength(10));
            categoryDefinition.setPartitionFieldValue(randomAlphaOfLength(20));
        }
        categoryDefinition.setTerms(randomAlphaOfLength(10));
        categoryDefinition.setRegex(randomAlphaOfLength(10));
        categoryDefinition.setMaxMatchingLength(randomLong());
        categoryDefinition.setExamples(Arrays.asList(generateRandomStringArray(10, 10, false)));
        if (randomBoolean()) {
            categoryDefinition.setGrokPattern(randomAlphaOfLength(50));
        }
        if (randomBoolean()) {
            categoryDefinition.setNumMatches(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            categoryDefinition.setPreferredToCategories(Stream.generate(ESTestCase::randomNonNegativeLong)
                .limit(10)
                .collect(Collectors.toList()));
        }
        return categoryDefinition;
    }

    @Override
    protected CategoryDefinition createTestInstance() {
        return createTestInstance(randomAlphaOfLength(10));
    }

    @Override
    protected CategoryDefinition doParseInstance(XContentParser parser) {
        return CategoryDefinition.PARSER.apply(parser, null);
    }

    public void testEquals_GivenSameObject() {
        CategoryDefinition category = new CategoryDefinition(randomAlphaOfLength(10));

        assertTrue(category.equals(category));
    }

    public void testEquals_GivenObjectOfDifferentClass() {
        CategoryDefinition category = new CategoryDefinition(randomAlphaOfLength(10));

        assertFalse(category.equals("a string"));
    }

    public void testEquals_GivenEqualCategoryDefinitions() {
        CategoryDefinition category1 = createFullyPopulatedCategoryDefinition();
        CategoryDefinition category2 = createFullyPopulatedCategoryDefinition();

        assertTrue(category1.equals(category2));
        assertTrue(category2.equals(category1));
        assertEquals(category1.hashCode(), category2.hashCode());
    }

    public void testEquals_GivenCategoryDefinitionsWithDifferentIds() {
        CategoryDefinition category1 = createFullyPopulatedCategoryDefinition();
        CategoryDefinition category2 = createFullyPopulatedCategoryDefinition();
        category2.setCategoryId(category1.getCategoryId() + 1);

        assertFalse(category1.equals(category2));
        assertFalse(category2.equals(category1));
    }

    public void testEquals_GivenCategoryDefinitionsWithDifferentTerms() {
        CategoryDefinition category1 = createFullyPopulatedCategoryDefinition();
        CategoryDefinition category2 = createFullyPopulatedCategoryDefinition();
        category2.setTerms(category1.getTerms() + " additional");

        assertFalse(category1.equals(category2));
        assertFalse(category2.equals(category1));
    }

    public void testEquals_GivenCategoryDefinitionsWithDifferentRegex() {
        CategoryDefinition category1 = createFullyPopulatedCategoryDefinition();
        CategoryDefinition category2 = createFullyPopulatedCategoryDefinition();
        category2.setRegex(category1.getRegex() + ".*additional.*");

        assertFalse(category1.equals(category2));
        assertFalse(category2.equals(category1));
    }

    public void testEquals_GivenCategoryDefinitionsWithDifferentMaxMatchingLength() {
        CategoryDefinition category1 = createFullyPopulatedCategoryDefinition();
        CategoryDefinition category2 = createFullyPopulatedCategoryDefinition();
        category2.setMaxMatchingLength(category1.getMaxMatchingLength() + 1);

        assertFalse(category1.equals(category2));
        assertFalse(category2.equals(category1));
    }

    public void testEquals_GivenCategoryDefinitionsWithDifferentExamples() {
        CategoryDefinition category1 = createFullyPopulatedCategoryDefinition();
        CategoryDefinition category2 = createFullyPopulatedCategoryDefinition();
        category2.addExample("additional");

        assertFalse(category1.equals(category2));
        assertFalse(category2.equals(category1));
    }

    private static CategoryDefinition createFullyPopulatedCategoryDefinition() {
        CategoryDefinition category = new CategoryDefinition("jobName");
        category.setCategoryId(42);
        category.setPartitionFieldName("p");
        category.setPartitionFieldValue("v");
        category.setTerms("foo bar");
        category.setRegex(".*?foo.*?bar.*");
        category.setMaxMatchingLength(120L);
        category.addExample("foo");
        category.addExample("bar");
        return category;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
