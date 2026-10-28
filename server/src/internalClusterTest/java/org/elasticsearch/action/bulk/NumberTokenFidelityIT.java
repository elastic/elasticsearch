/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 * Sends numbers to Elasticsearch in a variety of spellings and checks that they come back the way the client wrote them.
 *
 * <p>Each combination of scenario, {@link SourceMode} and {@link Check} is its own test, so the test report lists exactly which ones
 * pass and which fail. A scenario indexes one document whose only content is a single number (or an array of them), written exactly as
 * the client would write it. The number is mapped both as a number and as a {@code keyword} multi-field, so the spelling is visible to
 * {@code term} queries and aggregations as well as in {@code _source}.
 *
 * <p>There are two groups of scenarios:
 * <ul>
 *   <li>JSON numbers in spellings other than the one a double prints as, such as {@code 12.40} or {@code 1e3}.
 *   <li>float32 tokens, which only exist in the binary formats (SMILE and CBOR). JSON can't express them: every JSON decimal is a
 *       double, even one that happens to be exact as a float32.
 * </ul>
 *
 * <p>Synthetic source is covered because a batch can't be rendered back to the client's bytes there either, and because it exercises
 * different code than stored source does. Which checks make sense depends on the {@link SourceMode}, because some differences are
 * expected even without batch indexing: synthetic source rebuilds {@code _source} from doc values, and a field kept in ignored source
 * ({@code synthetic_source_keep: all}) has its keyword sub-field indexed from the parsed value, not from the client's spelling.
 *
 * <p>The tests deliberately do not set {@code indices.batch_indexing}; they run with the default. They are expected to pass when batch
 * indexing is off. When it is on, the ESCF row keeps only a canonical value and a coarse type tag for each number, so the original
 * spelling, and whether it was a float32, is lost before the stored source and keyword terms are built.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class NumberTokenFidelityIT extends ESIntegTestCase {

    private static final String INDEX = "numbers";

    private enum SourceMode {
        /** The default: {@code _source} is stored as the client sent it. */
        STORED(Check.SOURCE, Check.KEYWORD_TERMS),
        /** {@code _source} is rebuilt from doc values, so its spelling is not expected to match what was sent. */
        SYNTHETIC(Check.KEYWORD_TERMS),
        /** {@code _source} is synthetic, but the number is kept in ignored source as it was parsed. */
        SYNTHETIC_KEEP_ALL(Check.SOURCE);

        private final List<Check> checks;

        SourceMode(Check... checks) {
            this.checks = List.of(checks);
        }
    }

    private enum Check {
        /** {@code _source} still says what the client sent. */
        SOURCE,
        /** The keyword multi-field was indexed from the token the client sent. */
        KEYWORD_TERMS
    }

    /**
     * One document to index, and what should be true of it afterwards.
     *
     * @param field         the mapped field the number is written to: {@code n} (a double) or {@code f} (a float)
     * @param contentType   the format of {@code source}
     * @param source        the bytes the client sends
     * @param expectedTerms the terms the field's {@code keyword} multi-field should end up with
     */
    private record Scenario(String field, XContentType contentType, BytesReference source, List<String> expectedTerms) {

        /** A JSON document whose only content is {@code token}, written exactly as given, to the double field {@code n}. */
        static Scenario json(String token) {
            return new Scenario("n", XContentType.JSON, new BytesArray("{\"n\":" + token + "}"), List.of(token));
        }

        /** A document in a binary format with float32 tokens in the float field {@code f}: one value, or an array of several. */
        static Scenario float32(XContentType contentType, float... values) {
            try (XContentBuilder builder = XContentBuilder.builder(contentType.xContent())) {
                builder.startObject();
                if (values.length == 1) {
                    builder.field("f", values[0]);
                } else {
                    builder.array("f", values);
                }
                builder.endObject();
                List<String> expectedTerms = new ArrayList<>();
                for (float value : values) {
                    expectedTerms.add(Float.toString(value));
                }
                return new Scenario("f", contentType, BytesReference.bytes(builder), expectedTerms);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
    }

    private static final Map<String, Scenario> SCENARIOS = new LinkedHashMap<>();
    static {
        // Controls: these already have a canonical spelling, so they should survive on any path.
        SCENARIOS.put("json_canonical_fraction", Scenario.json("123.42"));
        SCENARIOS.put("json_canonical_whole_double", Scenario.json("100.0"));
        SCENARIOS.put("json_canonical_long", Scenario.json("9223372036854775807"));
        // A double that is also exact as a float32, but is not written the way a float32 is.
        SCENARIOS.put("json_double_that_fits_in_a_float", Scenario.json("123.41999816894531"));
        // The spelling is not the one a double prints as.
        SCENARIOS.put("json_trailing_zero", Scenario.json("12.40"));
        SCENARIOS.put("json_lowercase_exponent", Scenario.json("1e3"));
        SCENARIOS.put("json_uppercase_exponent", Scenario.json("1E5"));
        SCENARIOS.put("json_more_digits_than_a_double_holds", Scenario.json("0.1000000000000000055511151231257827"));
        // Larger than a long: the number should stay a number.
        SCENARIOS.put("json_integer_larger_than_a_long", Scenario.json("12345678901234567890"));
        // A float32 token should stay a float32, not turn into the double with the same value.
        SCENARIOS.put("smile_float32", Scenario.float32(XContentType.SMILE, 123.42f));
        SCENARIOS.put("cbor_float32", Scenario.float32(XContentType.CBOR, 123.42f));
        SCENARIOS.put("smile_float32_array", Scenario.float32(XContentType.SMILE, 1.1f, 2.2f));
        SCENARIOS.put("cbor_float32_array", Scenario.float32(XContentType.CBOR, 1.1f, 2.2f));
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<Object[]> parameters = new ArrayList<>();
        for (SourceMode sourceMode : SourceMode.values()) {
            for (Check check : sourceMode.checks) {
                for (String scenarioName : SCENARIOS.keySet()) {
                    parameters.add(new Object[] { scenarioName, sourceMode, check });
                }
            }
        }
        return parameters;
    }

    private final String scenarioName;
    private final Scenario scenario;
    private final SourceMode sourceMode;
    private final Check check;

    public NumberTokenFidelityIT(
        @Name("scenario") String scenarioName,
        @Name("sourceMode") SourceMode sourceMode,
        @Name("check") Check check
    ) {
        this.scenarioName = scenarioName;
        this.scenario = SCENARIOS.get(scenarioName);
        this.sourceMode = sourceMode;
        this.check = check;
    }

    @Before
    public void indexTheScenario() {
        Settings.Builder settings = indexSettings(1, 0);
        if (sourceMode != SourceMode.STORED) {
            settings.put("index.mapping.source.mode", "synthetic");
        }
        String keep = sourceMode == SourceMode.SYNTHETIC_KEEP_ALL ? ", \"synthetic_source_keep\": \"all\"" : "";
        assertAcked(prepareCreate(INDEX).setSettings(settings).setMapping(Strings.format("""
            {
              "properties": {
                "n": { "type": "double", "fields": { "k": { "type": "keyword" } }%s },
                "f": { "type": "float", "fields": { "k": { "type": "keyword" } }%s }
              }
            }
            """, keep, keep)));
        prepareIndex(INDEX).setId(scenarioName).setSource(scenario.source(), scenario.contentType()).get();
        refresh(INDEX);
    }

    public void test() throws IOException {
        switch (check) {
            case SOURCE -> assertSourceKeepsTheNumber();
            case KEYWORD_TERMS -> assertKeywordTermsKeepTheSpelling();
        }
    }

    /**
     * {@code _source} should say what the client sent: its spelling in stored source, its parsed value in ignored source. Both sides are
     * shown as JSON so that a failure is readable, and so that a float32 token ({@code 123.42}) is distinguishable from the double with
     * the same value ({@code 123.41999816894531}).
     */
    private void assertSourceKeepsTheNumber() throws IOException {
        BytesReference actual = client().prepareGet(INDEX, scenarioName).get().getSourceAsBytesRef();
        String expected = switch (sourceMode) {
            case STORED -> toJson(scenario.source(), scenario.contentType());
            case SYNTHETIC_KEEP_ALL -> parsedValuesAsJson(scenario.source(), scenario.contentType());
            case SYNTHETIC -> throw new AssertionError("synthetic source is not expected to keep the spelling");
        };
        assertThat(toJson(actual, XContentHelper.xContentType(actual)), equalTo(expected));
    }

    /** The keyword multi-field should be indexed from the token the client sent, so that is the term it should end up with. */
    private void assertKeywordTermsKeepTheSpelling() {
        var terms = AggregationBuilders.terms("terms").field(scenario.field() + ".k").size(10);
        assertResponse(prepareSearch(INDEX).setSize(0).addAggregation(terms), response -> {
            TreeSet<String> actualTerms = new TreeSet<>();
            for (Terms.Bucket bucket : ((Terms) response.getAggregations().get("terms")).getBuckets()) {
                actualTerms.add(bucket.getKeyAsString());
            }
            assertThat(actualTerms, equalTo(new TreeSet<>(scenario.expectedTerms())));
        });
    }

    private static String toJson(BytesReference source, XContentType contentType) throws IOException {
        return XContentHelper.convertToJson(source, false, contentType);
    }

    /** The source as JSON after each number has been parsed to a Java value, which is how ignored source holds it. */
    private static String parsedValuesAsJson(BytesReference source, XContentType contentType) throws IOException {
        Map<String, Object> parsed = XContentHelper.convertToMap(source, false, contentType).v2();
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            return BytesReference.bytes(builder.map(parsed)).utf8ToString();
        }
    }
}
