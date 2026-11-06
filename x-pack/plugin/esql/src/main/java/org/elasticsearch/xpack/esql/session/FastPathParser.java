/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * A hand-written recognizer for the simplest "search" shaped ES|QL queries:
 * <pre>
 *   FROM pattern[, pattern]* [METADATA m[, m]*] followed by at most one each of
 *   | WHERE cond, | KEEP f, ..., | SORT f [ASC|DESC] [NULLS FIRST|LAST], ..., | LIMIT n, in any order that keeps the meaning
 *   of the canonical WHERE, KEEP, SORT, LIMIT order: WHERE and SORT must precede LIMIT (after a LIMIT they would apply to the
 *   truncated rows), and a WHERE or SORT after KEEP may only reference kept fields.
 * </pre>
 * where {@code m} is one of {@code _source}, {@code _id} and {@code _score}, and {@code cond} is a boolean combination
 * ({@code AND}, {@code OR}, {@code NOT}, parentheses) of {@code field op literal} comparisons, {@code MATCH(field, "text")} and
 * {@code field : "text"} (or a number in place of the text, for numeric fields), {@code MATCH_PHRASE(field, "text")}, and
 * {@code field [NOT] LIKE "pattern"} with a single pattern.
 * <p>
 * Anything else returns {@code null} so the caller falls back to the full ANTLR parser. The recognizer only has to be
 * precise about what it accepts: the result is a tiny AST that the session turns into the same optimized plan the full
 * pipeline would produce for these shapes, skipping analysis and optimization. It must never accept a query whose full
 * pipeline semantics differ from the plan built from this AST.
 */
public final class FastPathParser {

    public enum CmpOp {
        EQ,
        NEQ,
        LT,
        LTE,
        GT,
        GTE
    }

    /** A literal as written: {@code text} is the unquoted string, numbers keep their lexeme in {@code text}. */
    public record Lit(Kind kind, String text) {
        public enum Kind {
            STRING,
            INTEGER,
            DECIMAL,
            BOOLEAN
        }
    }

    public sealed interface Cond permits Cmp, MatchPred, LikePred, And, Or, Not {}

    public record Cmp(String field, Source fieldSource, CmpOp op, Lit lit, Source source) implements Cond {}

    /**
     * A full text predicate: the {@code field : query} operator form, {@code MATCH(field, query)} or
     * {@code MATCH_PHRASE(field, "text")}. The query is a string literal, or a number for the first two forms.
     */
    public record MatchPred(String field, Source fieldSource, Lit lit, MatchKind kind, Source source) implements Cond {
        public enum MatchKind {
            OPERATOR,
            MATCH,
            MATCH_PHRASE
        }
    }

    /** {@code field LIKE "pattern"}: the pattern as written, unescaped (patterns with escapes are left to the full parser). */
    public record LikePred(String field, Source fieldSource, String pattern, Source source) implements Cond {}

    public record And(Cond left, Cond right, Source source) implements Cond {}

    public record Or(Cond left, Cond right, Source source) implements Cond {}

    public record Not(Cond child, Source source) implements Cond {}

    public record SortKey(String field, boolean ascending, Boolean nullsFirst) {}

    /**
     * @param metadata the {@code METADATA} fields in the order requested, without duplicates; empty when none are requested
     * @param keep null when there is no KEEP command (all fields are output); may name requested metadata fields
     * @param limit null when there is no LIMIT command (the implicit default applies)
     */
    public record FastQuery(String indexPattern, List<String> metadata, Cond where, List<String> keep, List<SortKey> sort, Integer limit) {
        /** The mapped fields the query references, in first-use order; the requested metadata fields are not mapped fields. */
        public Set<String> referencedFields() {
            Set<String> fields = new LinkedHashSet<>();
            if (where != null) {
                collect(where, fields);
            }
            if (keep != null) {
                fields.addAll(keep);
            }
            for (SortKey key : sort) {
                fields.add(key.field());
            }
            fields.removeAll(metadata);
            return fields;
        }

        private static void collect(Cond cond, Set<String> into) {
            switch (cond) {
                case Cmp c -> into.add(c.field());
                case MatchPred m -> into.add(m.field());
                case LikePred l -> into.add(l.field());
                case And a -> {
                    collect(a.left(), into);
                    collect(a.right(), into);
                }
                case Or o -> {
                    collect(o.left(), into);
                    collect(o.right(), into);
                }
                case Not n -> collect(n.child(), into);
            }
        }
    }

    private enum T {
        IDENT,
        STRING,
        NUMBER,
        PIPE,
        COMMA,
        LP,
        RP,
        OP,
        EOF
    }

    private record Tok(T type, String text, int offset) {}

    /** The stored source: the metadata field search style queries commonly return. */
    public static final String SOURCE_METADATA = "_source";
    /** The document id, a keyword loaded from the stored fields. */
    public static final String ID_METADATA = "_id";
    /** The relevance score, which the data nodes compute for the full text predicates when the query asks for it. */
    public static final String SCORE_METADATA = "_score";
    /** The metadata fields the fast path plans; the rest are left to the full pipeline. */
    public static final Set<String> METADATA_FIELDS = Set.of(SOURCE_METADATA, ID_METADATA, SCORE_METADATA);

    private final String query;
    private final List<Tok> toks;
    private int pos;

    private FastPathParser(String query, List<Tok> toks) {
        this.query = query;
        this.toks = toks;
    }

    /** Returns the recognized query or {@code null} when the text is outside the supported subset. */
    public static FastQuery parse(String query) {
        if (query == null || query.length() > 4096) {
            return null;
        }
        List<Tok> toks = tokenize(query);
        if (toks == null) {
            return null;
        }
        try {
            return new FastPathParser(query, toks).statement();
        } catch (NoMatch e) {
            return null;
        }
    }

    private static final class NoMatch extends RuntimeException {
        NoMatch() {
            super(null, null, false, false);
        }
    }

    private static final NoMatch NO_MATCH = new NoMatch();

    // ---- tokenizer

    private static List<Tok> tokenize(String q) {
        List<Tok> out = new ArrayList<>();
        int i = 0;
        int n = q.length();
        while (i < n) {
            char c = q.charAt(i);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                i++;
                continue;
            }
            int start = i;
            if (c == '|') {
                out.add(new Tok(T.PIPE, "|", start));
                i++;
            } else if (c == ',') {
                out.add(new Tok(T.COMMA, ",", start));
                i++;
            } else if (c == '(') {
                out.add(new Tok(T.LP, "(", start));
                i++;
            } else if (c == ')') {
                out.add(new Tok(T.RP, ")", start));
                i++;
            } else if (c == '"') {
                i++;
                // Triple quotes and escapes are left to the full parser.
                if (i < n && q.charAt(i) == '"') {
                    return null;
                }
                int s = i;
                while (i < n && q.charAt(i) != '"') {
                    if (q.charAt(i) == '\\') {
                        return null;
                    }
                    i++;
                }
                if (i >= n) {
                    return null;
                }
                out.add(new Tok(T.STRING, q.substring(s, i), start));
                i++;
            } else if (c == '=' || c == '!' || c == '<' || c == '>' || c == ':') {
                if (c == '=' || c == '!') {
                    if (i + 1 < n && q.charAt(i + 1) == '=') {
                        out.add(new Tok(T.OP, c + "=", start));
                        i += 2;
                    } else {
                        return null;
                    }
                } else if (c == ':') {
                    out.add(new Tok(T.OP, ":", start));
                    i++;
                } else if (i + 1 < n && q.charAt(i + 1) == '=') {
                    out.add(new Tok(T.OP, c + "=", start));
                    i += 2;
                } else {
                    out.add(new Tok(T.OP, String.valueOf(c), start));
                    i++;
                }
            } else if (isDigit(c) || (c == '-' && i + 1 < n && isDigit(q.charAt(i + 1)))) {
                i++;
                while (i < n && (isDigit(q.charAt(i)) || q.charAt(i) == '.')) {
                    i++;
                }
                if (i < n && isIdentChar(q.charAt(i))) {
                    return null; // 1h, 2d, 1e5 etc.
                }
                out.add(new Tok(T.NUMBER, q.substring(start, i), start));
            } else if (isIdentStart(c)) {
                i++;
                // Index patterns allow '-', '*' and '.'; identifiers allow '.' for sub-fields. Validate per use site.
                while (i < n && (isIdentChar(q.charAt(i)) || q.charAt(i) == '.' || q.charAt(i) == '-' || q.charAt(i) == '*')) {
                    i++;
                }
                out.add(new Tok(T.IDENT, q.substring(start, i), start));
            } else {
                return null;
            }
        }
        out.add(new Tok(T.EOF, "", n));
        return out;
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static boolean isIdentStart(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == '@';
    }

    private static boolean isIdentChar(char c) {
        return isIdentStart(c) || isDigit(c);
    }

    // ---- recognizer

    private Tok peek() {
        return toks.get(pos);
    }

    private Tok next() {
        return toks.get(pos++);
    }

    private boolean isKeyword(Tok t, String kw) {
        return t.type() == T.IDENT && t.text().equalsIgnoreCase(kw);
    }

    private void expectKeyword(String kw) {
        if (isKeyword(next(), kw) == false) {
            throw NO_MATCH;
        }
    }

    private void expect(T type) {
        if (next().type() != type) {
            throw NO_MATCH;
        }
    }

    private FastQuery statement() {
        expectKeyword("FROM");
        String pattern = indexPatterns();
        List<String> metadata = List.of();
        if (isKeyword(peek(), "METADATA")) {
            next();
            metadata = metadataFields();
        }
        Cond where = null;
        List<String> keep = null;
        List<SortKey> sort = List.of();
        Integer limit = null;
        // Commands come in any order, each at most once; their positions decide whether the canonical order means the same.
        int command = 0;
        int wherePos = -1;
        int keepPos = -1;
        int sortPos = -1;
        int limitPos = -1;
        while (peek().type() == T.PIPE) {
            next();
            Tok cmd = next();
            command++;
            if (isKeyword(cmd, "WHERE") && wherePos < 0) {
                where = or();
                wherePos = command;
            } else if (isKeyword(cmd, "KEEP") && keepPos < 0) {
                keep = fieldList();
                keepPos = command;
            } else if (isKeyword(cmd, "SORT") && sortPos < 0) {
                sort = sortKeys();
                sortPos = command;
            } else if (isKeyword(cmd, "LIMIT") && limitPos < 0) {
                Tok n = next();
                if (n.type() != T.NUMBER || n.text().indexOf('.') >= 0 || n.text().startsWith("-")) {
                    throw NO_MATCH;
                }
                try {
                    limit = Integer.parseInt(n.text());
                } catch (NumberFormatException e) {
                    throw NO_MATCH;
                }
                limitPos = command;
            } else {
                throw NO_MATCH;
            }
        }
        expect(T.EOF);
        if (limitPos >= 0 && (wherePos > limitPos || sortPos > limitPos)) {
            throw NO_MATCH; // filtering or sorting the already truncated rows is not the canonical shape
        }
        if (keep != null) {
            if (sortPos > keepPos) {
                for (SortKey key : sort) {
                    if (keep.contains(key.field()) == false) {
                        throw NO_MATCH; // SORT after KEEP may only reference kept fields
                    }
                }
            }
            if (wherePos > keepPos) {
                Set<String> whereFields = new LinkedHashSet<>();
                FastQuery.collect(where, whereFields);
                if (keep.containsAll(whereFields) == false) {
                    throw NO_MATCH; // WHERE after KEEP may only reference kept fields
                }
            }
        }
        return new FastQuery(pattern, metadata, where, keep, sort, limit);
    }

    private String indexPatterns() {
        StringBuilder sb = new StringBuilder();
        while (true) {
            Tok t = next();
            if (t.type() != T.IDENT || t.text().indexOf(':') >= 0 || isReserved(t.text())) {
                throw NO_MATCH;
            }
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(t.text());
            if (peek().type() == T.COMMA) {
                next();
            } else {
                return sb.toString();
            }
        }
    }

    private static boolean isReserved(String ident) {
        return switch (ident.toUpperCase(Locale.ROOT)) {
            case "FROM", "WHERE", "KEEP", "SORT", "LIMIT", "AND", "OR", "NOT", "ASC", "DESC", "NULLS", "FIRST", "LAST", "TRUE", "FALSE",
                "NULL", "MATCH", "MATCH_PHRASE", "METADATA", "IN", "IS", "LIKE", "RLIKE" -> true;
            default -> false;
        };
    }

    private String fieldName() {
        Tok t = next();
        if (t.type() != T.IDENT || isReserved(t.text()) || t.text().indexOf('*') >= 0 || t.text().indexOf('-') >= 0) {
            throw NO_MATCH;
        }
        return t.text();
    }

    /**
     * The {@code METADATA} list, limited to the fields the fast path plans. Repeating a field is left to the full pipeline
     * (the analyzer keeps the last occurrence), as is any other metadata field or a pattern.
     */
    private List<String> metadataFields() {
        List<String> fields = new ArrayList<>();
        while (true) {
            Tok t = next();
            if (t.type() != T.IDENT || METADATA_FIELDS.contains(t.text()) == false || fields.contains(t.text())) {
                throw NO_MATCH;
            }
            fields.add(t.text());
            if (peek().type() == T.COMMA) {
                next();
            } else {
                return fields;
            }
        }
    }

    private List<String> fieldList() {
        List<String> fields = new ArrayList<>();
        while (true) {
            String f = fieldName();
            if (fields.contains(f)) {
                throw NO_MATCH;
            }
            fields.add(f);
            if (peek().type() == T.COMMA) {
                next();
            } else {
                return fields;
            }
        }
    }

    private List<SortKey> sortKeys() {
        List<SortKey> keys = new ArrayList<>();
        while (true) {
            String f = fieldName();
            boolean asc = true;
            Boolean nullsFirst = null;
            if (isKeyword(peek(), "ASC")) {
                next();
            } else if (isKeyword(peek(), "DESC")) {
                next();
                asc = false;
            }
            if (isKeyword(peek(), "NULLS")) {
                next();
                Tok t = next();
                if (isKeyword(t, "FIRST")) {
                    nullsFirst = true;
                } else if (isKeyword(t, "LAST")) {
                    nullsFirst = false;
                } else {
                    throw NO_MATCH;
                }
            }
            keys.add(new SortKey(f, asc, nullsFirst));
            if (peek().type() == T.COMMA) {
                next();
            } else {
                return keys;
            }
        }
    }

    private Cond or() {
        int start = peek().offset();
        Cond left = and();
        while (isKeyword(peek(), "OR")) {
            next();
            Cond right = and();
            left = new Or(left, right, source(start));
        }
        return left;
    }

    private Cond and() {
        int start = peek().offset();
        Cond left = unary();
        while (isKeyword(peek(), "AND")) {
            next();
            Cond right = unary();
            left = new And(left, right, source(start));
        }
        return left;
    }

    private Cond unary() {
        int start = peek().offset();
        if (isKeyword(peek(), "NOT")) {
            next();
            return new Not(unary(), source(start));
        }
        return primary();
    }

    private Cond primary() {
        int start = peek().offset();
        if (peek().type() == T.LP) {
            next();
            Cond c = or();
            expect(T.RP);
            return c;
        }
        if (isKeyword(peek(), "MATCH") || isKeyword(peek(), "MATCH_PHRASE")) {
            MatchPred.MatchKind kind = isKeyword(next(), "MATCH") ? MatchPred.MatchKind.MATCH : MatchPred.MatchKind.MATCH_PHRASE;
            expect(T.LP);
            int fieldStart = peek().offset();
            String field = fieldName();
            Source fieldSource = source(fieldStart);
            expect(T.COMMA);
            Lit lit = matchLiteral();
            if (kind == MatchPred.MatchKind.MATCH_PHRASE && lit.kind() != Lit.Kind.STRING) {
                throw NO_MATCH;
            }
            expect(T.RP);
            return new MatchPred(field, fieldSource, lit, kind, source(start));
        }
        String field = fieldName();
        Source fieldSource = source(start);
        if (isKeyword(peek(), "LIKE") || isKeyword(peek(), "NOT")) {
            boolean negated = isKeyword(next(), "NOT");
            if (negated) {
                expectKeyword("LIKE");
            }
            Tok pattern = next();
            if (pattern.type() != T.STRING) {
                throw NO_MATCH; // LIKE ("a", "b") and non-literal patterns
            }
            Cond like = new LikePred(field, fieldSource, pattern.text(), source(start));
            return negated ? new Not(like, source(start)) : like;
        }
        Tok op = next();
        if (op.type() != T.OP) {
            throw NO_MATCH;
        }
        if (op.text().equals(":")) {
            return new MatchPred(field, fieldSource, matchLiteral(), MatchPred.MatchKind.OPERATOR, source(start));
        }
        CmpOp cmpOp = switch (op.text()) {
            case "==" -> CmpOp.EQ;
            case "!=" -> CmpOp.NEQ;
            case "<" -> CmpOp.LT;
            case "<=" -> CmpOp.LTE;
            case ">" -> CmpOp.GT;
            case ">=" -> CmpOp.GTE;
            default -> throw NO_MATCH;
        };
        Tok v = next();
        Lit lit = switch (v.type()) {
            case STRING -> new Lit(Lit.Kind.STRING, v.text());
            case NUMBER -> new Lit(v.text().indexOf('.') >= 0 ? Lit.Kind.DECIMAL : Lit.Kind.INTEGER, v.text());
            case IDENT -> {
                if (v.text().equalsIgnoreCase("true") || v.text().equalsIgnoreCase("false")) {
                    yield new Lit(Lit.Kind.BOOLEAN, v.text().toLowerCase(Locale.ROOT));
                }
                throw NO_MATCH;
            }
            default -> throw NO_MATCH;
        };
        return new Cmp(field, fieldSource, cmpOp, lit, source(start));
    }

    /** The query of a match predicate: a string, or a number (which the full pipeline accepts on numeric fields). */
    private Lit matchLiteral() {
        Tok v = next();
        return switch (v.type()) {
            case STRING -> new Lit(Lit.Kind.STRING, v.text());
            case NUMBER -> new Lit(v.text().indexOf('.') >= 0 ? Lit.Kind.DECIMAL : Lit.Kind.INTEGER, v.text());
            default -> throw NO_MATCH;
        };
    }

    /** Source spanning from {@code start} to the end of the previously consumed token, on line 1 like the ANTLR parser reports. */
    private Source source(int start) {
        Tok last = toks.get(pos - 1);
        int end = last.offset() + last.text().length() + (last.type() == T.STRING ? 2 : 0);
        // The same line and column the ANTLR parser reports: 1-based line, 0-based position in that line.
        int line = 1;
        int lineStart = 0;
        for (int i = 0; i < start; i++) {
            if (query.charAt(i) == '\n') {
                line++;
                lineStart = i + 1;
            }
        }
        return new Source(line, start - lineStart, query.substring(start, Math.min(end, query.length())));
    }
}
