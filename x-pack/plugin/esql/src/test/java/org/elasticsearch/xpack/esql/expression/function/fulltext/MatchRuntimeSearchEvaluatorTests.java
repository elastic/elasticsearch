/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ConstantEvaluators;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

/**
 * End-to-end execution tests for runtime {@code match}, where the field is
 * not a Lucene-mapped index field). Unlike {@link MatchTests}, which only checks type resolution and serialization,
 * this builds the actual runtime evaluators and runs them over real {@link Block}s.
 * <p>
 * It covers the two behaviors of runtime match: analyzed full-text matching on a {@code text} field (the
 * {@code to_text(...)} case), and exact value matching on every other type. Multivalue (any-value match), null/missing
 * positions, and the thread-safety of the shared per-thread scratch {@link BytesRef} are exercised too.
 */
public class MatchRuntimeSearchEvaluatorTests extends AbstractRuntimeSearchEvaluatorTests {

    private static Match runtimeMatch(DataType fieldType, Object queryValue, DataType queryType) {
        ReferenceAttribute field = new ReferenceAttribute(Source.EMPTY, "field", fieldType);
        Literal query = new Literal(Source.EMPTY, queryValue, queryType);
        Match match = new Match(Source.EMPTY, field, query, null);
        assertTrue("expected a runtime search, not a pushed-down query", match.isRuntimeSearch());
        return match;
    }

    private static Match runtimeMatchWithOptions(DataType fieldType, Object queryValue, DataType queryType, MapExpression options) {
        ReferenceAttribute field = new ReferenceAttribute(Source.EMPTY, "field", fieldType);
        Literal query = new Literal(Source.EMPTY, queryValue, queryType);
        Match match = new Match(Source.EMPTY, field, query, options);
        assertTrue("expected a runtime search, not a pushed-down query", match.isRuntimeSearch());
        return match;
    }

    // ---- text: analyzed full-text matching (the to_text case) ----

    public void testTextIsAnalyzed() {
        // "brown" matches "Brown" (standard analyzer lowercases) on the first row; no row mentions a dog.
        Boolean[] result = evaluate(runtimeMatch(TEXT, new BytesRef("brown"), KEYWORD), factory -> bytesRefBlock(factory, builder -> {
            builder.appendBytesRef(new BytesRef("This is a Brown fox"));
            builder.appendBytesRef(new BytesRef("The cat sat on the mat"));
        }));
        assertArrayEquals(new Boolean[] { true, false }, result);
    }

    public void testTextMatchesAnyTokenWithOrSemantics() {
        // Multi-term query uses OR semantics on the runtime path, so a single shared token is enough to match.
        Boolean[] result = evaluate(
            runtimeMatch(TEXT, new BytesRef("quick turtle"), KEYWORD),
            factory -> bytesRefBlock(factory, builder -> {
                builder.appendBytesRef(new BytesRef("a quick fox"));
                builder.appendBytesRef(new BytesRef("a slow turtle"));
            })
        );
        assertArrayEquals(new Boolean[] { true, true }, result);
    }

    public void testTextMultiValueAndNull() {
        // Matches if any value in the position matches; a missing value never matches.
        Boolean[] result = evaluate(runtimeMatch(TEXT, new BytesRef("cat"), KEYWORD), factory -> bytesRefBlock(factory, builder -> {
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("brown fox"));
            builder.appendBytesRef(new BytesRef("white cat"));
            builder.endPositionEntry();
            builder.appendNull();
        }));
        assertArrayEquals(new Boolean[] { true, false }, result);
    }

    public void testTextWithZeroQueryTerms() {
        Boolean[] result = evaluate(runtimeMatch(TEXT, new BytesRef("! ! !"), TEXT), factory -> bytesRefBlock(factory, builder -> {
            builder.appendBytesRef(new BytesRef("This is a Brown fox"));
            builder.appendBytesRef(new BytesRef("The cat sat on the mat"));
        }));
        assertArrayEquals(new Boolean[] { false, false }, result);
    }

    public void testTextAndTermNormalization() {
        Boolean[] result = evaluate(runtimeMatch(TEXT, new BytesRef("cat dog"), TEXT), factory -> bytesRefBlock(factory, builder -> {
            builder.appendBytesRef(new BytesRef("The CAT sat on the mat"));
            builder.appendBytesRef(new BytesRef("LAZY DOG"));
        }));
        assertArrayEquals(new Boolean[] { true, true }, result);
    }

    public void testTextAndMultiTermQuery() {
        Boolean[] result = evaluate(runtimeMatch(TEXT, new BytesRef("fox dog"), TEXT), factory -> bytesRefBlock(factory, builder -> {
            builder.appendBytesRef(new BytesRef("This is a brown fox"));
            builder.appendBytesRef(new BytesRef("This is a brown dog"));
            builder.appendBytesRef(new BytesRef("Just a turtle"));
            builder.appendBytesRef(new BytesRef("This dog is really brown"));
        }));
        assertArrayEquals(new Boolean[] { true, true, false, true }, result);
    }

    public void testTextWithZeroTermsValues() {
        Boolean[] result = evaluate(runtimeMatch(TEXT, new BytesRef("fox"), TEXT), factory -> bytesRefBlock(factory, builder -> {
            builder.appendBytesRef(new BytesRef("! !"));
            builder.appendBytesRef(new BytesRef(""));
        }));
        assertArrayEquals(new Boolean[] { false, false }, result);
    }

    // ---- keyword: exact (unanalyzed) matching ----

    public void testKeywordIsExactAndCaseSensitive() {
        // Unlike text, keyword compares the whole value byte-for-byte: only the exact "hello" matches.
        Boolean[] result = evaluate(runtimeMatch(KEYWORD, new BytesRef("hello"), KEYWORD), factory -> bytesRefBlock(factory, builder -> {
            builder.appendBytesRef(new BytesRef("Hello"));
            builder.appendBytesRef(new BytesRef("hello"));
            builder.appendBytesRef(new BytesRef("hell"));
        }));
        assertArrayEquals(new Boolean[] { false, true, false }, result);
    }

    public void testKeywordMultiValueAndNull() {
        Boolean[] result = evaluate(runtimeMatch(KEYWORD, new BytesRef("b"), KEYWORD), factory -> bytesRefBlock(factory, builder -> {
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("a"));
            builder.appendBytesRef(new BytesRef("b"));
            builder.endPositionEntry();
            builder.appendNull();
        }));
        assertArrayEquals(new Boolean[] { true, false }, result);
    }

    // ---- numeric / boolean: exact value matching ----

    public void testLong() {
        Boolean[] result = evaluate(runtimeMatch(LONG, 30L, LONG), factory -> {
            try (var builder = factory.newLongBlockBuilder(3)) {
                builder.appendLong(10L);
                builder.beginPositionEntry();
                builder.appendLong(20L);
                builder.appendLong(30L);
                builder.endPositionEntry();
                builder.appendNull();
                return builder.build();
            }
        });
        assertArrayEquals(new Boolean[] { false, true, false }, result);
    }

    public void testInteger() {
        Boolean[] result = evaluate(runtimeMatch(INTEGER, 7, INTEGER), factory -> {
            try (var builder = factory.newIntBlockBuilder(2)) {
                builder.appendInt(7);
                builder.appendInt(8);
                return builder.build();
            }
        });
        assertArrayEquals(new Boolean[] { true, false }, result);
    }

    public void testDouble() {
        Boolean[] result = evaluate(runtimeMatch(DOUBLE, 2.5d, DOUBLE), factory -> {
            try (var builder = factory.newDoubleBlockBuilder(2)) {
                builder.appendDouble(1.5);
                builder.appendDouble(2.5);
                return builder.build();
            }
        });
        assertArrayEquals(new Boolean[] { false, true }, result);
    }

    public void testBoolean() {
        Boolean[] result = evaluate(runtimeMatch(BOOLEAN, true, BOOLEAN), factory -> {
            try (var builder = factory.newBooleanBlockBuilder(3)) {
                builder.appendBoolean(true);
                builder.appendBoolean(false);
                builder.appendNull();
                return builder.build();
            }
        });
        assertArrayEquals(new Boolean[] { true, false, false }, result);
    }

    public void testTextWithZeroTermsQueryUsesConstantBlock() {
        Match match = runtimeMatch(TEXT, new BytesRef("! ! !"), TEXT);
        assertThat(match.toEvaluator(toEvaluator()), instanceOf(ConstantEvaluators.CONSTANT_FALSE_FACTORY.getClass()));
    }

    /**
     * {@code match(field, "! ! !", {"zero_terms_query": "none"})} produces a Lucene {@code MatchNoDocsQuery}
     * after analysis, so {@code textEvaluatorForQuery} returns the constant-false factory directly rather
     * than wrapping a per-row {@link RuntimeSearchTextWithLuceneQueryEvaluator}.
     */
    public void testTextWithZeroTermsQueryNoneAndNoTokensUsesConstantFalse() {
        Match match = runtimeMatchWithOptions(TEXT, new BytesRef("! ! !"), TEXT, mapOptions("zero_terms_query", "none"));
        assertThat(match.toEvaluator(toEvaluator()), instanceOf(ConstantEvaluators.CONSTANT_FALSE_FACTORY.getClass()));
    }

    /**
     * {@code match(field, "! ! !", {"zero_terms_query": "all"})} produces a Lucene {@code MatchAllDocsQuery}
     * after analysis, so {@code textEvaluatorForQuery} returns the constant-true factory directly rather
     * than wrapping a per-row {@link RuntimeSearchTextWithLuceneQueryEvaluator}.
     */
    public void testTextWithZeroTermsQueryAllAndNoTokensUsesConstantTrue() {
        Match match = runtimeMatchWithOptions(TEXT, new BytesRef("! ! !"), TEXT, mapOptions("zero_terms_query", "all"));
        assertThat(match.toEvaluator(toEvaluator()), instanceOf(ConstantEvaluators.CONSTANT_TRUE_FACTORY.getClass()));
    }

    /**
     * Without options, runtime {@code match} on a {@code text} field uses the optimized
     * {@link RuntimeSearchTextEvaluator.Factory} path: the query is analyzed once into terms and each row's
     * token stream is matched directly — no per-row Lucene index overhead.
     */
    public void testTextWithoutOptionsUsesOptimizedEvaluator() {
        Match match = runtimeMatch(TEXT, new BytesRef("quick fox"), KEYWORD);
        assertThat(match.toEvaluator(toEvaluator()), instanceOf(RuntimeSearchTextEvaluator.Factory.class));
    }

    /**
     * With options, runtime {@code match} on a {@code text} field falls back to
     * {@link RuntimeSearchTextWithLuceneQueryEvaluator.Factory}: a full Lucene query is built so that options
     * such as {@code fuzziness} or {@code operator} are honoured per row.
     */
    public void testTextWithOptionsUsesLuceneQueryEvaluator() {
        Match match = runtimeMatchWithOptions(TEXT, new BytesRef("quick fox"), KEYWORD, mapOptions("operator", "AND"));
        assertThat(match.toEvaluator(toEvaluator()), instanceOf(RuntimeSearchTextWithLuceneQueryEvaluator.Factory.class));
    }

    // ---- text with options: Lucene-query evaluator path ----

    /**
     * With {@code operator: AND} every query term must appear in the value; the default OR requires only one.
     */
    public void testTextWithOperatorAndRequiresAllTerms() {
        Boolean[] result = evaluate(
            runtimeMatchWithOptions(TEXT, new BytesRef("quick fox"), KEYWORD, mapOptions("operator", "AND")),
            factory -> bytesRefBlock(factory, builder -> {
                builder.appendBytesRef(new BytesRef("the quick brown fox")); // has both "quick" and "fox"
                builder.appendBytesRef(new BytesRef("the quick dog"));       // "quick" but no "fox"
                builder.appendBytesRef(new BytesRef("a cunning fox"));       // "fox" but no "quick"
            })
        );
        assertArrayEquals(new Boolean[] { true, false, false }, result);
    }

    /**
     * {@code fuzziness: 1} matches values within edit distance 1 of the query term.
     */
    public void testTextWithFuzzinessMatchesNearbyTerms() {
        // "bron" is one deletion away from "brown"
        Boolean[] result = evaluate(
            runtimeMatchWithOptions(TEXT, new BytesRef("bron"), KEYWORD, mapOptions("fuzziness", "1")),
            factory -> bytesRefBlock(factory, builder -> {
                builder.appendBytesRef(new BytesRef("the quick brown fox"));
                builder.appendBytesRef(new BytesRef("the lazy dog"));
            })
        );
        assertArrayEquals(new Boolean[] { true, false }, result);
    }

    /**
     * {@code minimum_should_match: 2} requires at least two of the query terms to match.
     */
    public void testTextWithMinimumShouldMatch() {
        Boolean[] result = evaluate(
            runtimeMatchWithOptions(TEXT, new BytesRef("quick lazy fox"), KEYWORD, mapOptions("minimum_should_match", "2")),
            factory -> bytesRefBlock(factory, builder -> {
                builder.appendBytesRef(new BytesRef("the quick brown fox")); // "quick" + "fox" = 2
                builder.appendBytesRef(new BytesRef("the lazy dog"));        // only "lazy" = 1
                builder.appendBytesRef(new BytesRef("the quick lazy cat")); // "quick" + "lazy" = 2
            })
        );
        assertArrayEquals(new Boolean[] { true, false, true }, result);
    }

    /**
     * With {@code operator: AND}, query terms spread across the values of a multivalued position still match:
     * as with an indexed multi-valued text field, all values belong to one document.
     */
    public void testTextWithOperatorAndMatchesAcrossValues() {
        Boolean[] result = evaluate(
            runtimeMatchWithOptions(TEXT, new BytesRef("quick dog"), KEYWORD, mapOptions("operator", "AND")),
            factory -> bytesRefBlock(factory, builder -> {
                builder.beginPositionEntry();
                builder.appendBytesRef(new BytesRef("quick fox")); // "quick" here...
                builder.appendBytesRef(new BytesRef("lazy dog"));  // ..."dog" here: together they satisfy the AND
                builder.endPositionEntry();
                builder.beginPositionEntry();
                builder.appendBytesRef(new BytesRef("quick fox")); // "quick" but no value has "dog"
                builder.appendBytesRef(new BytesRef("lazy cat"));
                builder.endPositionEntry();
            })
        );
        assertArrayEquals(new Boolean[] { true, false }, result);
    }

    /**
     * {@code minimum_should_match} counts matched terms across all values of a multivalued position.
     */
    public void testTextWithMinimumShouldMatchAcrossValues() {
        Boolean[] result = evaluate(
            runtimeMatchWithOptions(TEXT, new BytesRef("quick lazy fox"), KEYWORD, mapOptions("minimum_should_match", "2")),
            factory -> bytesRefBlock(factory, builder -> {
                builder.beginPositionEntry();
                builder.appendBytesRef(new BytesRef("the quick cat")); // "quick" = 1
                builder.appendBytesRef(new BytesRef("a lazy dog"));    // + "lazy" = 2
                builder.endPositionEntry();
                builder.beginPositionEntry();
                builder.appendBytesRef(new BytesRef("the quick cat")); // only "quick" = 1
                builder.appendBytesRef(new BytesRef("a brown dog"));
                builder.endPositionEntry();
            })
        );
        assertArrayEquals(new Boolean[] { true, false }, result);
    }

    /**
     * {@code zero_terms_query: none} returns no results when the query analyzes to zero tokens (the default).
     */
    public void testTextWithZeroTermsQueryNoneMatchesNothing() {
        Boolean[] result = evaluate(
            runtimeMatchWithOptions(TEXT, new BytesRef(""), KEYWORD, mapOptions("zero_terms_query", "none")),
            factory -> bytesRefBlock(factory, builder -> {
                builder.appendBytesRef(new BytesRef("the quick brown fox"));
                builder.appendBytesRef(new BytesRef("the lazy dog"));
            })
        );
        assertArrayEquals(new Boolean[] { false, false }, result);
    }

    /**
     * {@code zero_terms_query: all} returns all rows when the query analyzes to zero tokens.
     */
    public void testTextWithZeroTermsQueryAllMatchesEverything() {
        Boolean[] result = evaluate(
            runtimeMatchWithOptions(TEXT, new BytesRef(""), KEYWORD, mapOptions("zero_terms_query", "all")),
            factory -> bytesRefBlock(factory, builder -> {
                builder.appendBytesRef(new BytesRef("the quick brown fox"));
                builder.appendBytesRef(new BytesRef("the lazy dog"));
            })
        );
        assertArrayEquals(new Boolean[] { true, true }, result);
    }

    /**
     * {@code fuzzy_transpositions: true} (the default) counts adjacent-character swaps as a single edit,
     * so "brwon" (r↔w transposed) matches "brown" within fuzziness 1.
     */
    public void testTextWithFuzzyTranspositionsEnabled() {
        Boolean[] result = evaluate(
            runtimeMatchWithOptions(TEXT, new BytesRef("brwon"), KEYWORD, mapOptions("fuzziness", "1", "fuzzy_transpositions", "true")),
            factory -> bytesRefBlock(factory, builder -> builder.appendBytesRef(new BytesRef("the quick brown fox")))
        );
        assertArrayEquals(new Boolean[] { true }, result);
    }

    /**
     * {@code fuzzy_transpositions: false} does not treat adjacent-character swaps as a single edit,
     * so "brwon" requires more than 1 edit to reach "brown" and does not match.
     */
    public void testTextWithFuzzyTranspositionsDisabled() {
        Boolean[] result = evaluate(
            runtimeMatchWithOptions(TEXT, new BytesRef("brwon"), KEYWORD, mapOptions("fuzziness", "1", "fuzzy_transpositions", "false")),
            factory -> bytesRefBlock(factory, builder -> builder.appendBytesRef(new BytesRef("the quick brown fox")))
        );
        assertArrayEquals(new Boolean[] { false }, result);
    }

    /**
     * {@code prefix_length: 1} keeps the first character fixed; "bron" still matches "brown" because they share
     * the initial "b" and the remaining distance is within fuzziness 1.
     */
    public void testTextWithPrefixLengthStillMatches() {
        Boolean[] result = evaluate(
            runtimeMatchWithOptions(TEXT, new BytesRef("bron"), KEYWORD, mapOptions("fuzziness", "1", "prefix_length", "1")),
            factory -> bytesRefBlock(factory, builder -> builder.appendBytesRef(new BytesRef("the quick brown fox")))
        );
        assertArrayEquals(new Boolean[] { true }, result);
    }

    /**
     * {@code prefix_length: 3} locks the first three characters; "bron" and "brown" share only "bro" (3 chars),
     * so the constraint is satisfied and fuzziness 1 covers the remaining difference.
     */
    public void testTextWithPrefixLengthExcludesNonMatchingPrefix() {
        Boolean[] result = evaluate(
            runtimeMatchWithOptions(
                TEXT,
                new BytesRef("froq"),  // starts with "fro", not "bro"
                KEYWORD,
                mapOptions("fuzziness", "1", "prefix_length", "3")
            ),
            factory -> bytesRefBlock(factory, builder -> builder.appendBytesRef(new BytesRef("the quick brown fox")))
        );
        assertArrayEquals(new Boolean[] { false }, result);
    }

    public void testTextWithWhitespaceAnalyzerIsCaseSensitive() {
        // The whitespace analyzer does not lowercase, unlike the standard analyzer.
        Boolean[] result = evaluate(
            runtimeMatchWithOptions(TEXT, new BytesRef("Fox"), KEYWORD, mapOptions("analyzer", "whitespace")),
            factory -> bytesRefBlock(factory, builder -> {
                builder.appendBytesRef(new BytesRef("the Fox jumped"));
                builder.appendBytesRef(new BytesRef("the fox jumped"));
            })
        );
        assertArrayEquals(new Boolean[] { true, false }, result);
    }

    public void testTextWithKeywordAnalyzerMatchesWholeValueOnly() {
        // The keyword analyzer emits the whole value as a single token.
        Boolean[] result = evaluate(
            runtimeMatchWithOptions(TEXT, new BytesRef("brown fox"), KEYWORD, mapOptions("analyzer", "keyword")),
            factory -> bytesRefBlock(factory, builder -> {
                builder.appendBytesRef(new BytesRef("brown fox"));
                builder.appendBytesRef(new BytesRef("a brown fox"));
            })
        );
        assertArrayEquals(new Boolean[] { true, false }, result);
    }

    public void testTextWithAnalyzerAndOperatorCombined() {
        Boolean[] result = evaluate(
            runtimeMatchWithOptions(TEXT, new BytesRef("Quick Fox"), KEYWORD, mapOptions("analyzer", "whitespace", "operator", "AND")),
            factory -> bytesRefBlock(factory, builder -> {
                builder.appendBytesRef(new BytesRef("Quick brown Fox"));
                builder.appendBytesRef(new BytesRef("quick brown fox"));
                builder.appendBytesRef(new BytesRef("Quick brown dog"));
            })
        );
        assertArrayEquals(new Boolean[] { true, false, false }, result);
    }

    // ---- scoring: runtime match contributes boost x matched-query-term count to _score ----

    public void testScoreTextSingleTerm() {
        Double[] result = score(runtimeMatch(TEXT, new BytesRef("fox"), KEYWORD), factory -> bytesRefBlock(factory, builder -> {
            builder.appendBytesRef(new BytesRef("This is a brown fox"));
            builder.appendBytesRef(new BytesRef("nothing here"));
        }));
        assertArrayEquals(new Double[] { 1.0, 0.0 }, result);
    }

    public void testScoreTextCountsMatchedTerms() {
        Double[] result = score(runtimeMatch(TEXT, new BytesRef("fox dog"), KEYWORD), factory -> bytesRefBlock(factory, builder -> {
            builder.appendBytesRef(new BytesRef("a brown fox"));
            builder.appendBytesRef(new BytesRef("fox and dog"));
            builder.appendBytesRef(new BytesRef("nothing here"));
        }));
        assertArrayEquals(new Double[] { 1.0, 2.0, 0.0 }, result);
    }

    /**
     * A query term repeated N times weighs N.
     */
    public void testScoreTextDuplicateQueryTerms() {
        Double[] result = score(runtimeMatch(TEXT, new BytesRef("fox fox"), KEYWORD), factory -> bytesRefBlock(factory, builder -> {
            builder.appendBytesRef(new BytesRef("a fox"));
            builder.appendBytesRef(new BytesRef("nothing here"));
        }));
        assertArrayEquals(new Double[] { 2.0, 0.0 }, result);
    }

    public void testScoreTextRepeatedValueTermCountedOnce() {
        Double[] result = score(runtimeMatch(TEXT, new BytesRef("fox"), KEYWORD), factory -> bytesRefBlock(factory, builder -> {
            builder.appendBytesRef(new BytesRef("fox fox fox"));
        }));
        assertArrayEquals(new Double[] { 1.0 }, result);
    }

    /**
     * All values of a multivalued position form one document: matched terms are the union across values, and a term
     * found in several values only counts once.
     */
    public void testScoreTextMultiValueUnionAndDedup() {
        Double[] result = score(runtimeMatch(TEXT, new BytesRef("fox dog"), KEYWORD), factory -> bytesRefBlock(factory, builder -> {
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("quick fox"));
            builder.appendBytesRef(new BytesRef("lazy dog"));
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("fox a"));
            builder.appendBytesRef(new BytesRef("fox b"));
            builder.endPositionEntry();
        }));
        assertArrayEquals(new Double[] { 2.0, 1.0 }, result);
    }

    public void testScoreTextNullAndEmptyValues() {
        Double[] result = score(runtimeMatch(TEXT, new BytesRef("fox"), KEYWORD), factory -> bytesRefBlock(factory, builder -> {
            builder.appendNull();
            builder.appendBytesRef(new BytesRef(""));
        }));
        assertArrayEquals(new Double[] { 0.0, 0.0 }, result);
    }

    public void testScoreTextIsAnalyzed() {
        Double[] result = score(runtimeMatch(TEXT, new BytesRef("brown"), KEYWORD), factory -> bytesRefBlock(factory, builder -> {
            builder.appendBytesRef(new BytesRef("a Brown fox"));
        }));
        assertArrayEquals(new Double[] { 1.0 }, result);
    }

    public void testScoreTextZeroQueryTerms() {
        Double[] result = score(runtimeMatch(TEXT, new BytesRef("! ! !"), TEXT), factory -> bytesRefBlock(factory, builder -> {
            builder.appendBytesRef(new BytesRef("a brown fox"));
        }));
        assertArrayEquals(new Double[] { 0.0 }, result);
    }

    public void testScoreTextWithOperatorAnd() {
        Double[] result = score(
            runtimeMatchWithOptions(TEXT, new BytesRef("quick fox"), KEYWORD, mapOptions("operator", "AND")),
            factory -> bytesRefBlock(factory, builder -> {
                builder.appendBytesRef(new BytesRef("the quick brown fox"));
                builder.appendBytesRef(new BytesRef("the quick dog"));
            })
        );
        assertArrayEquals(new Double[] { 2.0, 0.0 }, result);
    }

    public void testScoreTextWithBoost() {
        Double[] result = score(
            runtimeMatchWithOptions(TEXT, new BytesRef("fox"), KEYWORD, mapOptions("boost", "2.0")),
            factory -> bytesRefBlock(factory, builder -> {
                builder.appendBytesRef(new BytesRef("a fox"));
                builder.appendBytesRef(new BytesRef("nothing here"));
            })
        );
        assertArrayEquals(new Double[] { 2.0, 0.0 }, result);
    }

    public void testScoreTextWithOperatorAndBoost() {
        Double[] result = score(
            runtimeMatchWithOptions(TEXT, new BytesRef("fox dog"), KEYWORD, mapOptions("operator", "AND", "boost", "1.5")),
            factory -> bytesRefBlock(factory, builder -> {
                builder.appendBytesRef(new BytesRef("fox and dog"));
            })
        );
        assertArrayEquals(new Double[] { 3.0 }, result);
    }

    public void testScoreTextWithMinimumShouldMatch() {
        Double[] result = score(
            runtimeMatchWithOptions(TEXT, new BytesRef("quick lazy fox"), KEYWORD, mapOptions("minimum_should_match", "2")),
            factory -> bytesRefBlock(factory, builder -> {
                builder.appendBytesRef(new BytesRef("the quick brown fox"));
                builder.appendBytesRef(new BytesRef("the lazy dog"));
            })
        );
        assertArrayEquals(new Double[] { 2.0, 0.0 }, result);
    }

    public void testScoreTextWithAnalyzer() {
        Double[] result = score(
            runtimeMatchWithOptions(TEXT, new BytesRef("Fox"), KEYWORD, mapOptions("analyzer", "whitespace")),
            factory -> bytesRefBlock(factory, builder -> {
                builder.appendBytesRef(new BytesRef("the Fox jumped"));
                builder.appendBytesRef(new BytesRef("the fox jumped"));
            })
        );
        assertArrayEquals(new Double[] { 1.0, 0.0 }, result);
    }

    public void testScoreTextWithZeroTermsQuery() {
        Double[] all = score(
            runtimeMatchWithOptions(TEXT, new BytesRef(""), KEYWORD, mapOptions("zero_terms_query", "all")),
            factory -> bytesRefBlock(factory, builder -> {
                builder.appendBytesRef(new BytesRef("a fox"));
                builder.appendBytesRef(new BytesRef("nothing here"));
            })
        );
        assertArrayEquals(new Double[] { 1.0, 1.0 }, all);

        Double[] none = score(
            runtimeMatchWithOptions(TEXT, new BytesRef(""), KEYWORD, mapOptions("zero_terms_query", "none")),
            factory -> bytesRefBlock(factory, builder -> {
                builder.appendBytesRef(new BytesRef("a fox"));
                builder.appendBytesRef(new BytesRef("nothing here"));
            })
        );
        assertArrayEquals(new Double[] { 0.0, 0.0 }, none);
    }

    /**
     * A boosted match-all (zero_terms_query: all) rewrites to a BoostQuery and takes the generic Lucene-query
     * scoring path rather than the constant-score shortcut.
     */
    public void testScoreTextWithZeroTermsQueryAllAndBoost() {
        Double[] result = score(
            runtimeMatchWithOptions(TEXT, new BytesRef(""), KEYWORD, mapOptions("zero_terms_query", "all", "boost", "3.0")),
            factory -> bytesRefBlock(factory, builder -> {
                builder.appendBytesRef(new BytesRef("anything"));
            })
        );
        assertArrayEquals(new Double[] { 3.0 }, result);
    }

    public void testScoreTextWithFuzziness() {
        // FuzzyQuery scales the boost by edit distance
        Double[] result = score(
            runtimeMatchWithOptions(TEXT, new BytesRef("bron"), KEYWORD, mapOptions("fuzziness", "1")),
            factory -> bytesRefBlock(factory, builder -> {
                builder.appendBytesRef(new BytesRef("the quick brown fox"));
                builder.appendBytesRef(new BytesRef("the lazy dog"));
            })
        );
        assertThat(result[0], greaterThan(0.0));
        assertEquals(0.0, result[1], 0.0);
    }

    public void testScoreTextMultiValueSpanningAnd() {
        Double[] result = score(
            runtimeMatchWithOptions(TEXT, new BytesRef("quick dog"), KEYWORD, mapOptions("operator", "AND")),
            factory -> bytesRefBlock(factory, builder -> {
                builder.beginPositionEntry();
                builder.appendBytesRef(new BytesRef("quick fox"));
                builder.appendBytesRef(new BytesRef("lazy dog"));
                builder.endPositionEntry();
            })
        );
        assertArrayEquals(new Double[] { 2.0 }, result);
    }

    /**
     * The token-stream scorer (no options) and the Lucene-query scorer (options) must agree: the default operator
     * is OR, so making it explicit must not change any score, including for duplicate query terms and multivalues.
     */
    public void testScoreTextConsistentAcrossScorerImplementations() {
        for (String query : new String[] { "fox dog", "fox fox" }) {
            Function<BlockFactory, Block> data = factory -> bytesRefBlock(factory, builder -> {
                builder.appendBytesRef(new BytesRef("a brown fox"));
                builder.appendBytesRef(new BytesRef("fox and dog"));
                builder.appendBytesRef(new BytesRef("nothing here"));
                builder.beginPositionEntry();
                builder.appendBytesRef(new BytesRef("quick fox"));
                builder.appendBytesRef(new BytesRef("lazy dog"));
                builder.endPositionEntry();
                builder.appendNull();
            });
            Double[] withoutOptions = score(runtimeMatch(TEXT, new BytesRef(query), KEYWORD), data);
            Double[] withOrOption = score(runtimeMatchWithOptions(TEXT, new BytesRef(query), KEYWORD, mapOptions("operator", "OR")), data);
            assertArrayEquals("query [" + query + "]", withoutOptions, withOrOption);
        }
    }

    public void testScoreKeywordExact() {
        Double[] result = score(runtimeMatch(KEYWORD, new BytesRef("hello"), KEYWORD), factory -> bytesRefBlock(factory, builder -> {
            builder.appendBytesRef(new BytesRef("hello"));
            builder.appendBytesRef(new BytesRef("Hello"));
        }));
        assertArrayEquals(new Double[] { 1.0, 0.0 }, result);
    }

    public void testScoreInteger() {
        Double[] result = score(runtimeMatch(INTEGER, 7, INTEGER), factory -> {
            try (var builder = factory.newIntBlockBuilder(2)) {
                builder.appendInt(7);
                builder.appendInt(8);
                return builder.build();
            }
        });
        assertArrayEquals(new Double[] { 1.0, 0.0 }, result);
    }

    /**
     * The options-path {@link org.apache.lucene.index.memory.MemoryIndex} is allocated per evaluator (via a
     * {@code THREAD_LOCAL}-scoped {@code @Fixed}) and reset per row: terms from an earlier row — or an earlier page
     * of the same evaluator — must not leak into later ones.
     */
    public void testMemoryIndexIsResetBetweenRowsAndPages() {
        Match match = runtimeMatchWithOptions(TEXT, new BytesRef("quick dog"), KEYWORD, mapOptions("operator", "AND"));
        DriverContext context = driverContext();
        try (ExpressionEvaluator evaluator = match.toEvaluator(toEvaluator()).get(context)) {
            // If the first row's terms leaked, they would satisfy the AND for every row after it.
            assertPage(evaluator, context, builder -> {
                builder.appendBytesRef(new BytesRef("quick dog"));
                builder.appendBytesRef(new BytesRef("quick fox"));
            }, new Boolean[] { true, false });
            // A later page through the same evaluator reuses the same MemoryIndex.
            assertPage(evaluator, context, builder -> builder.appendBytesRef(new BytesRef("lazy dog")), new Boolean[] { false });
        }
    }

    private static void assertPage(
        ExpressionEvaluator evaluator,
        DriverContext context,
        Consumer<BytesRefBlock.Builder> build,
        Boolean[] expected
    ) {
        Page page = new Page(bytesRefBlock(context.blockFactory(), build));
        try (BooleanBlock result = (BooleanBlock) evaluator.eval(page)) {
            Boolean[] out = new Boolean[result.getPositionCount()];
            for (int p = 0; p < out.length; p++) {
                out[p] = result.isNull(p) ? null : result.getBoolean(result.getFirstValueIndex(p));
            }
            assertArrayEquals(expected, out);
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * The {@code bytes_ref} evaluator uses a per-evaluator scratch {@link BytesRef} (created per {@link DriverContext}
     * via a {@code THREAD_LOCAL}-scoped {@code @Fixed}). Many threads sharing one factory must each get an independent
     * scratch, so concurrent evaluation cannot corrupt the comparison. Run the same match on many threads and check
     * every result.
     */
    public void testScratchIsThreadSafe() {
        Match match = runtimeMatch(KEYWORD, new BytesRef("needle"), KEYWORD);
        ExpressionEvaluator.Factory factory = match.toEvaluator(toEvaluator());

        runInParallel(64, task -> {
            boolean expectMatch = (task & 1) == 0;
            DriverContext context = driverContext();
            Block field = bytesRefBlock(
                context.blockFactory(),
                builder -> builder.appendBytesRef(new BytesRef(expectMatch ? "needle" : "haystack"))
            );
            try (ExpressionEvaluator evaluator = factory.get(context)) {
                Page page = new Page(field);
                try (BooleanBlock result = (BooleanBlock) evaluator.eval(page)) {
                    assertEquals("task " + task, expectMatch, result.getBoolean(0));
                } finally {
                    page.releaseBlocks();
                }
            }
        });
    }
}
