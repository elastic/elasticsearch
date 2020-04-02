/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.containsString;

public class CategoryDefinitionTests extends AbstractBWCSerializationTestCase<CategoryDefinition> {

    public CategoryDefinition createTestInstance(String jobId) {
        CategoryDefinition categoryDefinition = new CategoryDefinition(jobId);
        categoryDefinition.setCategoryId(randomLong());
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
            categoryDefinition.setPreferredToCategories(LongStream.generate(ESTestCase::randomNonNegativeLong).limit(10).toArray());
        }
        return categoryDefinition;
    }

    @Override
    protected CategoryDefinition createTestInstance() {
        return createTestInstance(randomAlphaOfLength(10));
    }

    @Override
    protected Writeable.Reader<CategoryDefinition> instanceReader() {
        return CategoryDefinition::new;
    }

    @Override
    protected CategoryDefinition doParseInstance(XContentParser parser) {
        return CategoryDefinition.STRICT_PARSER.apply(parser, null);
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

    public void testId() {
        CategoryDefinition category = new CategoryDefinition("job-foo");
        category.setCategoryId(5L);
        assertEquals("job-foo_category_definition_5", category.getId());
    }

    private static CategoryDefinition createFullyPopulatedCategoryDefinition() {
        CategoryDefinition category = new CategoryDefinition("jobName");
        category.setCategoryId(42);
        category.setTerms("foo bar");
        category.setRegex(".*?foo.*?bar.*");
        category.setMaxMatchingLength(120L);
        category.addExample("foo");
        category.addExample("bar");
        return category;
    }

    public void testStrictParser() throws IOException {
        String json = "{\"job_id\":\"job_1\", \"foo\":\"bar\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    () -> CategoryDefinition.STRICT_PARSER.apply(parser, null));

            assertThat(e.getMessage(), containsString("unknown field [foo]"));
        }
    }

    public void testLenientParser() throws IOException {
        String json = "{\"job_id\":\"job_1\", \"foo\":\"bar\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            CategoryDefinition.LENIENT_PARSER.apply(parser, null);
        }
    }

    @Override
    protected CategoryDefinition mutateInstanceForVersion(CategoryDefinition instance, Version version) {
        if (version.before(Version.V_7_8_0)) {
            instance.setPreferredToCategories(new long[0]);
            instance.setNumMatches(0L);
        }
        return instance;
    }
}
