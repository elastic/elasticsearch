/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.Vocabulary;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.parser.EsqlBaseLexer;
import org.junit.AfterClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Generates reserved-keywords.md from the tokenizer (best-effort).
 * Every detected keyword must be in {@code KEYWORDS_BY_VERSION} under the version that introduced it
 * ({@code null} if it has always been reserved, otherwise {@code 9.6+} and the like).
 * A keyword the tokenizer misses must also be added to {@code TOKENS_WHITELIST}.
 * Tech preview and parser-snapshot commands are still reserved when the lexer rule is unconditional.
 */
public class ReservedKeywordsDocsTests extends ESTestCase {
    private static final Pattern IDENTIFIER_SHAPED = Pattern.compile("[A-Za-z][A-Za-z0-9_]*");
    /**
     * Snapshot-only tokens; omit from release docs.
     */
    private static final Pattern DEV_TOKEN = Pattern.compile("^DEV_");
    /**
     * Extra keywords the tokenizer misses. Also add each one to {@code KEYWORDS_BY_VERSION}.
     */
    private static final Set<String> TOKENS_WHITELIST = Set.of();
    /**
     * Tokens to omit from the generated list.
     */
    private static final Set<String> TOKENS_BLACKLIST = Set.of("UNKNOWN_CMD");
    /**
     * Keywords grouped by introduction version, in display order.
     * {@code null} is unversioned.
     */
    private static final Map<String, Set<String>> KEYWORDS_BY_VERSION = new LinkedHashMap<>();

    static {
        KEYWORDS_BY_VERSION.put(
            null,
            Set.of(
                "AND",
                "AS",
                "ASC",
                "BY",
                "CHANGE_POINT",
                "COMPLETION",
                "DESC",
                "DISSECT",
                "DROP",
                "ENRICH",
                "EVAL",
                "FALSE",
                "FIRST",
                "FORK",
                "FROM",
                "FUSE",
                "GROK",
                "GROUP",
                "IN",
                "INFO",
                "INLINE",
                "INLINESTATS",
                "IP_LOCATION",
                "IS",
                "JOIN",
                "KEEP",
                "KEY",
                "LAST",
                "LIKE",
                "LIMIT",
                "LOOKUP",
                "METADATA",
                "METRICS_INFO",
                "MMR",
                "MV_EXPAND",
                "NOT",
                "NULL",
                "NULLS",
                "ON",
                "OR",
                "PROMQL",
                "REGISTERED_DOMAIN",
                "RENAME",
                "RERANK",
                "RLIKE",
                "ROW",
                "SAMPLE",
                "SCORE",
                "SET",
                "SHOW",
                "SORT",
                "STATS",
                "TRUE",
                "TS",
                "TS_COLLAPSE",
                "TS_INFO",
                "URI_PARTS",
                "USER_AGENT",
                "USING",
                "WHERE",
                "WITH"
            )
        );
        KEYWORDS_BY_VERSION.put("9.6+", Set.of("DEDUP", "HIGHLIGHT"));
    }

    /**
     * Identifier-shaped keywords from the tokenizer, plus {@code TOKENS_WHITELIST}.
     */
    private static final List<String> RESERVED_KEYWORDS = detectReservedKeywords();

    public void testReservedKeywordsAreIdentifierShapedAndSorted() {
        assertFalse("lexer vocabulary should yield identifier-shaped keywords", RESERVED_KEYWORDS.isEmpty());
        assertTrue("expression keyword IN should be in the dump", RESERVED_KEYWORDS.contains("IN"));
        assertTrue("command name FROM should be in the dump", RESERVED_KEYWORDS.contains("FROM"));
        assertTrue("STATS is recovered by lexing its symbolic name", RESERVED_KEYWORDS.contains("STATS"));
        assertFalse("WS is not a keyword", RESERVED_KEYWORDS.contains("WS"));
        assertFalse("UNKNOWN_CMD is a catch-all regex, not a keyword", RESERVED_KEYWORDS.contains("UNKNOWN_CMD"));
        for (int i = 0; i < RESERVED_KEYWORDS.size(); i++) {
            String keyword = RESERVED_KEYWORDS.get(i);
            assertTrue("not identifier-shaped: " + keyword, IDENTIFIER_SHAPED.matcher(keyword).matches());
            assertEquals("keywords should be upper-case: " + keyword, keyword.toUpperCase(Locale.ROOT), keyword);
            if (i > 0) {
                assertTrue(
                    "keywords should be alphabetical: " + RESERVED_KEYWORDS.get(i - 1) + " before " + keyword,
                    RESERVED_KEYWORDS.get(i - 1).compareTo(keyword) < 0
                );
            }
        }
    }

    public void testDevTokensAreOmitted() {
        Vocabulary vocabulary = EsqlBaseLexer.VOCABULARY;
        for (int tokenType = 0; tokenType <= vocabulary.getMaxTokenType(); tokenType++) {
            String symbolic = vocabulary.getSymbolicName(tokenType);
            if (symbolic == null || DEV_TOKEN.matcher(symbolic).matches() == false) {
                continue;
            }
            String literal = vocabulary.getLiteralName(tokenType);
            if (literal == null) {
                continue;
            }
            String word = unquoteAntlrLiteral(literal).toUpperCase(Locale.ROOT);
            assertFalse(symbolic + " is snapshot-only", RESERVED_KEYWORDS.contains(word));
        }
    }

    public void testDetectedKeywordsAreVersioned() {
        Set<String> detected = new TreeSet<>(RESERVED_KEYWORDS);
        Set<String> versioned = new TreeSet<>();
        for (Set<String> group : KEYWORDS_BY_VERSION.values()) {
            for (String keyword : group) {
                assertTrue("keyword listed more than once: " + keyword, versioned.add(keyword.toUpperCase(Locale.ROOT)));
            }
        }
        Set<String> missing = new TreeSet<>(detected);
        missing.removeAll(versioned);
        assertTrue(
            "add to KEYWORDS_BY_VERSION with the version that introduced them"
                + " (and TOKENS_WHITELIST if the tokenizer misses them): "
                + missing,
            missing.isEmpty()
        );
        Set<String> extra = new TreeSet<>(versioned);
        extra.removeAll(detected);
        assertTrue("not detected; add to TOKENS_WHITELIST or remove from KEYWORDS_BY_VERSION: " + extra, extra.isEmpty());
    }

    public void testRenderedSnippetGroupsKeywordsByVersion() {
        String rendered = renderSnippet();
        assertTrue(rendered.startsWith("% This is generated by ESQL's ReservedKeywordsDocsTests."));
        assertTrue(rendered.contains("* {applies_to}`stack: ga` {applies_to}`serverless: ga`"));
        assertTrue(rendered.contains("* {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga`"));
        assertTrue(rendered.contains("`IN`"));
        assertTrue(rendered.contains("`FROM`"));
        assertTrue(rendered.contains("`STATS`"));
        int unversioned = rendered.indexOf("{applies_to}`stack: ga`");
        int versioned = rendered.indexOf("{applies_to}`stack: ga 9.6+`");
        assertTrue(unversioned >= 0 && unversioned < versioned);
        assertTrue(rendered.substring(versioned).contains("`DEDUP`"));
    }

    @AfterClass
    public static void renderDocs() throws IOException {
        // Gradle sets generateDocs to write or assert. Without it, do not touch the snippet.
        if (System.getProperty("generateDocs") == null) {
            return;
        }
        new ReservedKeywordsDocsSupport().renderDocs();
    }

    private static List<String> detectReservedKeywords() {
        Vocabulary vocabulary = EsqlBaseLexer.VOCABULARY;
        Set<String> keywords = new TreeSet<>();
        for (int tokenType = 0; tokenType <= vocabulary.getMaxTokenType(); tokenType++) {
            String symbolic = vocabulary.getSymbolicName(tokenType);
            if (symbolic != null && (DEV_TOKEN.matcher(symbolic).matches() || TOKENS_BLACKLIST.contains(symbolic))) {
                continue;
            }
            String word = keywordText(vocabulary, tokenType, symbolic);
            if (word != null && IDENTIFIER_SHAPED.matcher(word).matches()) {
                keywords.add(word.toUpperCase(Locale.ROOT));
            }
        }
        for (String extra : TOKENS_WHITELIST) {
            keywords.add(extra.toUpperCase(Locale.ROOT));
        }
        return new ArrayList<>(keywords);
    }

    static String renderSnippet() {
        StringBuilder builder = new StringBuilder();
        builder.append("% This is generated by ESQL's ")
            .append(ReservedKeywordsDocsTests.class.getSimpleName())
            .append(". Do not edit it. See docs/reference/query-languages/esql/README.md for how to regenerate it.\n\n");
        Set<String> detected = new TreeSet<>(RESERVED_KEYWORDS);
        boolean any = false;
        for (Map.Entry<String, Set<String>> entry : KEYWORDS_BY_VERSION.entrySet()) {
            List<String> words = new ArrayList<>();
            for (String keyword : entry.getValue()) {
                String normalized = keyword.toUpperCase(Locale.ROOT);
                if (detected.contains(normalized)) {
                    words.add(normalized);
                }
            }
            words.sort(null);
            if (words.isEmpty()) {
                continue;
            }
            if (any) {
                builder.append('\n');
            }
            any = true;
            builder.append("* ").append(appliesTo(entry.getKey())).append('\n');
            builder.append("  ");
            for (int i = 0; i < words.size(); i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                builder.append('`').append(words.get(i)).append('`');
            }
            builder.append('\n');
        }
        return builder.toString();
    }

    private static String appliesTo(String version) {
        String stack = version == null ? "stack: ga" : "stack: ga " + version;
        return "{applies_to}`" + stack + "` {applies_to}`serverless: ga`";
    }

    private static String keywordText(Vocabulary vocabulary, int tokenType, String symbolic) {
        String literal = vocabulary.getLiteralName(tokenType);
        if (literal != null) {
            return unquoteAntlrLiteral(literal);
        }
        // ANTLR drops a literal when two rules share it (STATS and INLINE_STATS both use 'stats').
        if (symbolic != null && IDENTIFIER_SHAPED.matcher(symbolic).matches() && lexesAs(symbolic, tokenType)) {
            return symbolic;
        }
        return null;
    }

    private static boolean lexesAs(String word, int tokenType) {
        EsqlBaseLexer lexer = new EsqlBaseLexer(CharStreams.fromString(word.toLowerCase(Locale.ROOT)));
        lexer.removeErrorListeners();
        Token token = lexer.nextToken();
        if (token.getType() != tokenType) {
            return false;
        }
        return lexer.nextToken().getType() == Token.EOF;
    }

    private static String unquoteAntlrLiteral(String literal) {
        if (literal.length() >= 2 && literal.charAt(0) == '\'' && literal.charAt(literal.length() - 1) == '\'') {
            return literal.substring(1, literal.length() - 1);
        }
        return literal;
    }

    /**
     * Writes the reserved-keyword snippet through the generateDocs callbacks.
     */
    public static class ReservedKeywordsDocsSupport extends DocsV3Support {
        private static final Logger logger = LogManager.getLogger(ReservedKeywordsDocsSupport.class);

        public ReservedKeywordsDocsSupport() {
            super("syntax", "reserved-keywords", ReservedKeywordsDocsTests.class, Set::of, callbacksFromSystemProperty());
        }

        @Override
        protected void renderDocs() throws IOException {
            String rendered = renderSnippet();
            logger.info("Writing reserved keywords snippet:\n{}", rendered);
            Path dir = PathUtils.get(System.getProperty("java.io.tmpdir")).resolve("esql").resolve("_snippets").resolve(category);
            callbacks.write(dir, name, "md", rendered, false);
        }
    }
}
