/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.support.filtering;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.support.AbstractFilteringTestCase;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.IntStream;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.joining;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public abstract class AbstractXContentFilteringTestCase extends AbstractFilteringTestCase {
    public void testSingleFieldObject() throws IOException {
        Builder sample = builder -> builder.startObject().startObject("foo").field("bar", "test").endObject().endObject();
        Builder expected = builder -> builder.startObject().startObject("foo").field("bar", "test").endObject().endObject();
        testFilter(expected, sample, singleton("foo.bar"), emptySet());
        testFilter(expected, sample, emptySet(), singleton("foo.baz"));
        testFilter(expected, sample, singleton("foo"), singleton("foo.baz"));

        expected = builder -> builder.startObject().endObject();
        testFilter(expected, sample, emptySet(), singleton("foo.bar"));
        testFilter(expected, sample, singleton("foo"), singleton("foo.b*"));
    }

    public void testDotInIncludedFieldNameUnconfigured() throws IOException {
        testFilter(
            builder -> builder.startObject().endObject(),
            builder -> builder.startObject().field("foo.bar", "test").endObject(),
            singleton("foo.bar"),
            emptySet(),
            false
        );
    }

    public void testDotInIncludedFieldNameConfigured() throws IOException {
        testFilter(
            builder -> builder.startObject().field("foo.bar", "test").endObject(),
            builder -> builder.startObject().field("foo.bar", "test").endObject(),
            singleton("foo.bar"),
            emptySet(),
            true
        );
    }

    public void testDotInExcludedFieldNameUnconfigured() throws IOException {
        testFilter(
            builder -> builder.startObject().field("foo.bar", "test").endObject(),
            builder -> builder.startObject().field("foo.bar", "test").endObject(),
            emptySet(),
            singleton("foo.bar"),
            false
        );
    }

    public void testDotInExcludedFieldNameConfigured() throws IOException {
        testFilter(
            builder -> builder.startObject().endObject(),
            builder -> builder.startObject().field("foo.bar", "test").endObject(),
            emptySet(),
            singleton("foo.bar"),
            true
        );
    }

    public void testDotInIncludedObjectNameUnconfigured() throws IOException {
        testFilter(
            builder -> builder.startObject().endObject(),
            builder -> builder.startObject().startObject("foo.bar").field("baz", "test").endObject().endObject(),
            singleton("foo.bar"),
            emptySet(),
            false
        );
    }

    public void testDotInIncludedObjectNameConfigured() throws IOException {
        testFilter(
            builder -> builder.startObject().startObject("foo.bar").field("baz", "test").endObject().endObject(),
            builder -> builder.startObject().startObject("foo.bar").field("baz", "test").endObject().endObject(),
            singleton("foo.bar"),
            emptySet(),
            true
        );
    }

    public void testDotInExcludedObjectNameUnconfigured() throws IOException {
        testFilter(
            builder -> builder.startObject().startObject("foo.bar").field("baz", "test").endObject().endObject(),
            builder -> builder.startObject().startObject("foo.bar").field("baz", "test").endObject().endObject(),
            emptySet(),
            singleton("foo.bar"),
            false
        );
    }

    public void testDotInExcludedObjectNameConfigured() throws IOException {
        testFilter(
            builder -> builder.startObject().endObject(),
            builder -> builder.startObject().startObject("foo.bar").field("baz", "test").endObject().endObject(),
            emptySet(),
            singleton("foo.bar"),
            true
        );
    }

    public void testDotInIncludedFieldNamePatternUnconfigured() throws IOException {
        testFilter(
            builder -> builder.startObject().endObject(),
            builder -> builder.startObject().field("foo.bar", "test").endObject(),
            singleton(randomFrom("foo.*", "*.bar", "f*.bar", "foo.*ar", "*.*")),
            emptySet(),
            false
        );
    }

    public void testDotInIncludedFieldNamePatternConfigured() throws IOException {
        testFilter(
            builder -> builder.startObject().field("foo.bar", "test").endObject(),
            builder -> builder.startObject().field("foo.bar", "test").endObject(),
            singleton(randomFrom("foo.*", "*.bar", "f*.bar", "foo.*ar", "*.*")),
            emptySet(),
            true
        );
    }

    public void testDotInExcludedFieldNamePatternUnconfigured() throws IOException {
        testFilter(
            builder -> builder.startObject().field("foo.bar", "test").endObject(),
            builder -> builder.startObject().field("foo.bar", "test").endObject(),
            emptySet(),
            singleton(randomFrom("foo.*", "foo.*ar")),
            false
        );
    }

    public void testDotInExcludedFieldNamePatternConfigured() throws IOException {
        testFilter(
            builder -> builder.startObject().endObject(),
            builder -> builder.startObject().field("foo.bar", "test").endObject(),
            emptySet(),
            singleton(randomFrom("foo.*", "*.bar", "f*.bar", "foo.*ar", "*.*")),
            true
        );
    }

    public void testDotInIncludedObjectNamePatternUnconfigured() throws IOException {
        testFilter(
            builder -> builder.startObject().endObject(),
            builder -> builder.startObject().startObject("foo.bar").field("baz", "test").endObject().endObject(),
            singleton(randomFrom("foo.*", "f*.bar", "foo.*ar")),
            emptySet(),
            false
        );
    }

    public void testDotInIncludedObjectNamePatternConfigured() throws IOException {
        testFilter(
            builder -> builder.startObject().startObject("foo.bar").field("baz", "test").endObject().endObject(),
            builder -> builder.startObject().startObject("foo.bar").field("baz", "test").endObject().endObject(),
            singleton(randomFrom("foo.*", "*.bar", "f*.bar", "foo.*ar")),
            emptySet(),
            true
        );
    }

    public void testDotInExcludedObjectNamePatternUnconfigured() throws IOException {
        testFilter(
            builder -> builder.startObject().startObject("foo.bar").field("baz", "test").endObject().endObject(),
            builder -> builder.startObject().startObject("foo.bar").field("baz", "test").endObject().endObject(),
            emptySet(),
            singleton(randomFrom("foo.*", "*.bar", "f*.bar", "foo.*ar")),
            false
        );
    }

    public void testDotInExcludedObjectNamePatternConfigured() throws IOException {
        testFilter(
            builder -> builder.startObject().endObject(),
            builder -> builder.startObject().startObject("foo.bar").field("baz", "test").endObject().endObject(),
            emptySet(),
            singleton(randomFrom("foo.*", "*.bar", "f*.bar", "foo.*ar")),
            true
        );
    }

    public void testDotInStarMatchDotsInNamesUnconfigured() throws IOException {
        testFilter(
            builder -> builder.startObject().field("foo.bar", "test").endObject(),
            builder -> builder.startObject().field("foo.bar", "test").endObject(),
            singleton("f*r"),
            emptySet(),
            false
        );
    }

    public void testDotInStarMatchDotsInNamesConfigured() throws IOException {
        testFilter(
            builder -> builder.startObject().endObject(),
            builder -> builder.startObject().field("foo.bar", "test").endObject(),
            singleton("f*r"),
            emptySet(),
            true
        );
    }

    public void testTwoDotsInIncludedFieldNameUnconfigured() throws IOException {
        testFilter(
            builder -> builder.startObject().endObject(),
            builder -> builder.startObject().field("foo.bar.baz", "test").endObject(),
            singleton("foo.bar.baz"),
            emptySet(),
            false
        );
    }

    public void testTwoDotsInIncludedFieldNameConfigured() throws IOException {
        testFilter(
            builder -> builder.startObject().field("foo.bar.baz", "test").endObject(),
            builder -> builder.startObject().field("foo.bar.baz", "test").endObject(),
            singleton("foo.bar.baz"),
            emptySet(),
            true
        );
    }

    public void testManyDotsInIncludedFieldName() throws IOException {
        String name = IntStream.rangeClosed(1, 100).mapToObj(i -> "a").collect(joining("."));
        testFilter(
            builder -> builder.startObject().field(name, "test").endObject(),
            builder -> builder.startObject().field(name, "test").endObject(),
            singleton(name),
            emptySet(),
            true
        );
    }

    public void testDotsInIncludedFieldNamePrefixMatch() throws IOException {
        testFilter(
            builder -> builder.startObject().field("foo.bar.baz", "test").endObject(),
            builder -> builder.startObject().field("foo.bar.baz", "test").endObject(),
            singleton("foo.bar"),
            emptySet(),
            true
        );
    }

    public void testDotsInExcludedFieldNamePrefixMatch() throws IOException {
        testFilter(
            builder -> builder.startObject().endObject(),
            builder -> builder.startObject().field("foo.bar.baz", "test").endObject(),
            emptySet(),
            singleton("foo.bar"),
            true
        );
    }

    public void testDotsInIncludedFieldNamePatternPrefixMatch() throws IOException {
        testFilter(
            builder -> builder.startObject().field("foo.bar.baz", "test").endObject(),
            builder -> builder.startObject().field("foo.bar.baz", "test").endObject(),
            singleton("f*.*r"),
            emptySet(),
            true
        );
    }

    public void testDotsInExcludedFieldNamePatternPrefixMatch() throws IOException {
        testFilter(
            builder -> builder.startObject().endObject(),
            builder -> builder.startObject().field("foo.bar.baz", "test").endObject(),
            emptySet(),
            singleton("f*.*r"),
            true
        );
    }

    public void testDotsAndDoubleWildcardInIncludedFieldName() throws IOException {
        testFilter(
            builder -> builder.startObject().field("foo.bar.baz", "test").endObject(),
            builder -> builder.startObject().field("foo.bar.baz", "test").endObject(),
            singleton("**.baz"),
            emptySet(),
            true
        );
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/80160")
    public void testDotsAndDoubleWildcardInExcludedFieldName() throws IOException {
        testFilter(
            builder -> builder.startObject().endObject(),
            builder -> builder.startObject().field("foo.bar.baz", "test").endObject(),
            emptySet(),
            singleton("**.baz"),
            true
        );
        // bug of double wildcard in excludes report in https://github.com/FasterXML/jackson-core/issues/700
        testFilter(
            builder -> builder.startObject().startObject("foo").field("baz", "test").endObject().endObject(),
            builder -> builder.startObject().startObject("foo").field("bar", "test").field("baz", "test").endObject().endObject(),
            emptySet(),
            singleton("**.bar"),
            true
        );
    }

    @Override
    protected final void testFilter(Builder expected, Builder sample, Collection<String> includes, Collection<String> excludes)
        throws IOException {
        testFilter(expected, sample, Set.copyOf(includes), Set.copyOf(excludes), false);
    }

    private void testFilter(Builder expected, Builder sample, Set<String> includes, Set<String> excludes, boolean matchFieldNamesWithDots)
        throws IOException {
        assertFilterResult(expected.apply(createBuilder()), filter(sample, includes, excludes, matchFieldNamesWithDots));
    }

    protected abstract void assertFilterResult(XContentBuilder expected, XContentBuilder actual);

    protected abstract XContentType getXContentType();

    @Override
    protected boolean removesEmptyArrays() {
        return true;
    }

    private XContentBuilder createBuilder() throws IOException {
        return XContentBuilder.builder(getXContentType().xContent());
    }

    private XContentBuilder filter(Builder sample, Set<String> includes, Set<String> excludes, boolean matchFieldNamesWithDots)
        throws IOException {
        if (matchFieldNamesWithDots == false && randomBoolean()) {
            return filterOnBuilder(sample, includes, excludes);
        }
        FilterPath[] excludesFilter = FilterPath.compile(excludes);
        if (excludesFilter != null && Arrays.stream(excludesFilter).anyMatch(FilterPath::hasDoubleWildcard)) {
            /*
             * If there are any double wildcard filters the parser based
             * filtering produced weird invalid json. Just field names
             * and no objects?! Weird. Anyway, we can't use it.
             */
            assertFalse("can't filter on builder with dotted wildcards in exclude", matchFieldNamesWithDots);
            return filterOnBuilder(sample, includes, excludes);
        }
        return filterOnParser(sample, includes, excludes, matchFieldNamesWithDots);
    }

    private XContentBuilder filterOnBuilder(Builder sample, Set<String> includes, Set<String> excludes) throws IOException {
        return sample.apply(XContentBuilder.builder(getXContentType(), includes, excludes));
    }

    private XContentBuilder filterOnParser(Builder sample, Set<String> includes, Set<String> excludes, boolean matchFieldNamesWithDots)
        throws IOException {
        try (XContentBuilder builtSample = sample.apply(createBuilder())) {
            BytesReference sampleBytes = BytesReference.bytes(builtSample);
            try (
                XContentParser parser = getXContentType().xContent()
                    .createParser(
                        XContentParserConfiguration.EMPTY.withFiltering(includes, excludes, matchFieldNamesWithDots),
                        sampleBytes.streamInput()
                    )
            ) {
                XContentBuilder result = createBuilder();
                if (sampleBytes.get(sampleBytes.length() - 1) == '\n') {
                    result.lfAtEnd();
                }
                if (parser.nextToken() == null) {
                    // If the filter removed everything then emit an open/close
                    return result.startObject().endObject();
                }
                return result.copyCurrentStructure(parser);
            }
        }
    }

    static void assertXContentBuilderAsString(final XContentBuilder expected, final XContentBuilder actual) {
        assertThat(Strings.toString(actual), is(Strings.toString(expected)));
    }

    static void assertXContentBuilderAsBytes(final XContentBuilder expected, final XContentBuilder actual) {
        XContent xContent = XContentFactory.xContent(actual.contentType());
        try (
            XContentParser jsonParser = xContent.createParser(
                XContentParserConfiguration.EMPTY,
                BytesReference.bytes(expected).streamInput()
            );
            XContentParser testParser = xContent.createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(actual).streamInput())
        ) {
            while (true) {
                XContentParser.Token token1 = jsonParser.nextToken();
                XContentParser.Token token2 = testParser.nextToken();
                if (token1 == null) {
                    assertThat(token2, nullValue());
                    return;
                }
                assertThat(token1, equalTo(token2));
                switch (token1) {
                    case FIELD_NAME -> assertThat(jsonParser.currentName(), equalTo(testParser.currentName()));
                    case VALUE_STRING -> assertThat(jsonParser.text(), equalTo(testParser.text()));
                    case VALUE_NUMBER -> {
                        assertThat(jsonParser.numberType(), equalTo(testParser.numberType()));
                        assertThat(jsonParser.numberValue(), equalTo(testParser.numberValue()));
                    }
                }
            }
        } catch (Exception e) {
            fail("Fail to verify the result of the XContentBuilder: " + e.getMessage());
        }
    }
}
