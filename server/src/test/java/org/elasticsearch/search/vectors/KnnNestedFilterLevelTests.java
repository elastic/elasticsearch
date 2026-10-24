/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.InnerHitsRewriteContext;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.NestedHelper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Checks that knn filters select the vector documents whose ancestor at the level of the filter's fields matches, for
 * every query type and any mix of levels, by comparing the documents the knn query returns with a model of the corpus.
 * <p>
 * Books have chapters, which have paragraphs. Books also have authors, a nested object that is not on the path to any
 * vector. One vector is on paragraphs, two levels deep, one on chapters, one level deep, and one on books, which knn
 * filters can search.
 */
public class KnnNestedFilterLevelTests extends MapperServiceTestCase {

    private static String mapping() {
        return Strings.format("""
            { "_doc": { "properties": {
              "genre": { "type": "keyword" },
              "year": { "type": "long" },
              "vector": { "type": "dense_vector", "dims": 2, "similarity": "l2_norm", "index_options": { "type": "%s" } },
              "authors": { "type": "nested", "properties": { "name": { "type": "keyword" } } },
              "chapters": { "type": "nested", "properties": {
                "cid": { "type": "keyword" },
                "language": { "type": "keyword" },
                "title": { "type": "keyword" },
                "num": { "type": "long" },
                "vector": { "type": "dense_vector", "dims": 2, "similarity": "l2_norm", "index_options": { "type": "%s" } },
                "paragraphs": { "type": "nested", "properties": {
                  "pid": { "type": "keyword" },
                  "text": { "type": "keyword" },
                  "body": { "type": "text" },
                  "vector": { "type": "dense_vector", "dims": 2, "similarity": "l2_norm", "index_options": { "type": "%s" } }
                } }
              } }
            } } }
            """, randomIndexType(), randomIndexType(), randomIndexType());
    }

    /** Knn queries on exact and approximate vectors build different Lucene queries. */
    private static String randomIndexType() {
        return randomFrom("flat", "int8_flat", "hnsw", "int8_hnsw");
    }

    private static final String[] GENRES = { "nature", "novel", "history" };
    private static final String[] AUTHORS = { "ann", "bob", "cid" };
    private static final String[] LANGUAGES = { "fr", "en", "de" };
    private static final String[] TITLES = { "intro", "middle", "outro" };
    private static final String[] TEXTS = { "alpha", "beta", "gamma" };
    private static final String[] WORDS = { "red", "green", "blue" };

    record Paragraph(String pid, String text, List<String> body) {}

    record Chapter(String cid, String language, String title, long num, List<Paragraph> paragraphs) {}

    record Book(String genre, long year, List<String> authors, List<Chapter> chapters) {}

    /** A vector document with its ancestors; {@code paragraph} is null for a chapter vector. */
    record Hit(Book book, Chapter chapter, Paragraph paragraph) {}

    /**
     * A filter and the hits it should select. {@code levels} are the levels it reads: 0 for books, 1 for chapters and
     * 2 for paragraphs.
     */
    record Filter(String description, QueryBuilder query, Predicate<Hit> matches, Set<Integer> levels) {
        @Override
        public String toString() {
            return description;
        }
    }

    /** Where the knn query runs: the vector's level and the nested queries that enclose it. */
    enum Target {
        PARAGRAPHS_TOP_LEVEL("chapters.paragraphs.vector", 2, 0),
        PARAGRAPHS_IN_NESTED("chapters.paragraphs.vector", 2, 2),
        CHAPTERS_TOP_LEVEL("chapters.vector", 1, 0),
        CHAPTERS_IN_NESTED("chapters.vector", 1, 1);

        final String field;
        final int depth;
        final int scopeDepth;

        Target(String field, int depth, int scopeDepth) {
            this.field = field;
            this.depth = depth;
            this.scopeDepth = scopeDepth;
        }
    }

    public void testQueryTypesOnFieldsOfEachLevel() throws IOException {
        List<Filter> filters = List.of(
            paragraphs("prefix", QueryBuilders.prefixQuery("chapters.paragraphs.text", "al"), p -> "alpha".equals(p.text())),
            paragraphs("wildcard", QueryBuilders.wildcardQuery("chapters.paragraphs.text", "*eta"), p -> "beta".equals(p.text())),
            paragraphs("regexp", QueryBuilders.regexpQuery("chapters.paragraphs.text", "g.*"), p -> "gamma".equals(p.text())),
            paragraphs("exists", QueryBuilders.existsQuery("chapters.paragraphs.text"), p -> p.text() != null),
            paragraphs(
                "match_phrase",
                QueryBuilders.matchPhraseQuery("chapters.paragraphs.body", "red green"),
                p -> containsPhrase(p.body(), "red", "green")
            ),
            chapters("prefix on chapters", QueryBuilders.prefixQuery("chapters.title", "in"), c -> "intro".equals(c.title())),
            chapters("exists on chapters", QueryBuilders.existsQuery("chapters.title"), c -> c.title() != null),
            chapters(
                "fuzzy on chapters",
                QueryBuilders.fuzzyQuery("chapters.language", "fz").fuzziness(Fuzziness.ONE),
                c -> "fr".equals(c.language())
            ),
            books("term on books", QueryBuilders.termQuery("genre", "nature"), b -> "nature".equals(b.genre())),
            books("prefix on books", QueryBuilders.prefixQuery("genre", "no"), b -> "novel".equals(b.genre())),
            books("range on books", QueryBuilders.rangeQuery("year").gte(1995), b -> b.year() >= 1995)
        );
        assertFilters(filters);
    }

    public void testCompoundFilters() throws IOException {
        Filter genre = books("genre", QueryBuilders.termQuery("genre", "nature"), b -> "nature".equals(b.genre()));
        Filter language = chapters("language", QueryBuilders.termQuery("chapters.language", "fr"), c -> "fr".equals(c.language()));
        Filter title = chapters("title", QueryBuilders.prefixQuery("chapters.title", "in"), c -> "intro".equals(c.title()));
        Filter text = paragraphs("text", QueryBuilders.termQuery("chapters.paragraphs.text", "alpha"), p -> "alpha".equals(p.text()));
        assertFilters(
            List.of(
                bool(List.of(genre, language), List.of(), List.of(), List.of()),
                bool(List.of(), List.of(), List.of(genre, title), List.of()),
                bool(List.of(genre), List.of(), List.of(), List.of(language)),
                bool(List.of(genre, language, text), List.of(), List.of(), List.of()),
                bool(List.of(), List.of(), List.of(genre, text), List.of(language)),
                bool(List.of(), List.of(text), List.of(), List.of()),
                bool(List.of(), List.of(language), List.of(), List.of()),
                bool(List.of(), List.of(genre), List.of(), List.of()),
                bool(List.of(), List.of(text, genre), List.of(), List.of()),
                new Filter(
                    "dis_max(genre, language)",
                    QueryBuilders.disMaxQuery().add(genre.query()).add(language.query()),
                    genre.matches().or(language.matches()),
                    Set.of(0, 1)
                ),
                new Filter(
                    "multi_match(genre, chapters.language)",
                    QueryBuilders.multiMatchQuery("fr", "genre", "chapters.language"),
                    language.matches(),
                    Set.of(0, 1)
                ),
                new Filter(
                    "query_string",
                    QueryBuilders.queryStringQuery("genre:nature AND chapters.language:fr"),
                    genre.matches().and(language.matches()),
                    Set.of(0, 1)
                )
            )
        );
    }

    public void testNestedQueryFilters() throws IOException {
        assertFilters(
            List.of(
                new Filter(
                    "nested(authors)",
                    QueryBuilders.nestedQuery("authors", QueryBuilders.termQuery("authors.name", "ann"), ScoreMode.None),
                    h -> h.book().authors().contains("ann"),
                    Set.of(0)
                ),
                new Filter(
                    "nested(chapters)",
                    QueryBuilders.nestedQuery("chapters", QueryBuilders.termQuery("chapters.language", "fr"), ScoreMode.None),
                    h -> h.book().chapters().stream().anyMatch(c -> "fr".equals(c.language())),
                    Set.of(0)
                )
            )
        );
    }

    /** Filters that combine levels at random, for every query type, every vector and every enclosing nesting. */
    public void testRandomFilters() throws IOException {
        List<Book> books = randomBooks();
        withIndex(books, (mapperService, searcher) -> {
            for (Target target : Target.values()) {
                for (int i = 0; i < 50; i++) {
                    Filter filter = randomFilter(target, 3);
                    assertFilter(mapperService, searcher, books, target, filter);
                }
            }
        });
    }

    private void assertFilters(List<Filter> filters) throws IOException {
        List<Book> books = randomBooks();
        withIndex(books, (mapperService, searcher) -> {
            for (Target target : Target.values()) {
                for (Filter filter : filters) {
                    if (filter.levels().stream().allMatch(level -> level <= target.depth)) {
                        assertFilter(mapperService, searcher, books, target, filter);
                    }
                }
            }
        });
    }

    private void assertFilter(MapperService mapperService, IndexSearcher searcher, List<Book> books, Target target, Filter filter)
        throws IOException {
        Map<String, Hit> hits = new HashMap<>();
        Set<String> expected = new HashSet<>();
        for (Hit hit : hits(books, target)) {
            hits.put(id(hit), hit);
            if (filter.matches().test(hit)) {
                expected.add(id(hit));
            }
        }
        String message = target + " " + filter;
        String path = target.depth == 1 ? "chapters" : "chapters.paragraphs";

        // the filters as the knn query applies them
        SearchExecutionContext context = createSearchExecutionContext(mapperService, searcher);
        enterScope(context, target.scopeDepth);
        QueryBuilder filterQuery = Rewriteable.rewrite(filter.query(), context, true);
        String fallbackParentPath = target.scopeDepth == 2 ? "chapters" : null;
        Query joined = NestedHelper.joinToNestedLevel(filterQuery.toQuery(context), path, fallbackParentPath, context);
        assertEquals(message, expected, search(searcher, levelOnly(context, path, joined), target));

        // the knn query keeps the best vector of each parent, the parent being one level above the enclosing nested query
        KnnVectorQueryBuilder knn = new KnnVectorQueryBuilder(target.field, new float[] { 1, 1 }, 10_000, 10_000, null, null, null);
        knn.addFilterQuery(filterQuery);
        Set<String> knnHits = search(searcher, knn.toQuery(context), target);
        assertTrue("knn " + message, expected.containsAll(knnHits));
        Function<String, Object> parent = id -> target.scopeDepth == 2 ? hits.get(id).chapter() : hits.get(id).book();
        assertEquals(
            "knn " + message,
            expected.stream().map(parent).collect(Collectors.toSet()),
            knnHits.stream().map(parent).collect(Collectors.toSet())
        );

        // inner hits rewrite the knn query to an exact one with the same filters, in the scope of the enclosing nested queries
        // plus the vector's level; a top level knn search is wrapped in a single nested query
        context = createSearchExecutionContext(mapperService, searcher);
        QueryBuilder innerHits;
        if (target.scopeDepth == 0) {
            context.nestedScope().nextLevel(context.nestedLookup().getNestedMappers().get(path));
            innerHits = new KnnScoreDocQueryBuilder(
                new ScoreDoc[] { new ScoreDoc(0, 1f) },
                target.field,
                VectorData.fromFloats(new float[] { 1, 1 }),
                null,
                List.of(filterQuery)
            );
        } else {
            enterScope(context, target.depth);
            innerHits = knn;
        }
        innerHits = Rewriteable.rewrite(innerHits, new InnerHitsRewriteContext(context.getParserConfig(), System::currentTimeMillis), true);
        assertEquals("inner hits " + message, expected, search(searcher, innerHits.toQuery(context), target));
    }

    private static Query levelOnly(SearchExecutionContext context, String path, Query query) {
        return new BooleanQuery.Builder().add(context.nestedLookup().getNestedMappers().get(path).nestedTypeFilter(), Occur.FILTER)
            .add(query, Occur.FILTER)
            .build();
    }

    private static void enterScope(SearchExecutionContext context, int depth) {
        String[] paths = { "chapters", "chapters.paragraphs" };
        for (int i = 0; i < depth; i++) {
            NestedObjectMapper mapper = context.nestedLookup().getNestedMappers().get(paths[i]);
            context.nestedScope().nextLevel(mapper);
        }
    }

    private static Set<String> search(IndexSearcher searcher, Query query, Target target) throws IOException {
        String idField = target.depth == 1 ? "chapters.cid" : "chapters.paragraphs.pid";
        Set<String> ids = new HashSet<>();
        List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
        for (ScoreDoc scoreDoc : searcher.search(query, 10_000).scoreDocs) {
            LeafReaderContext leaf = leaves.get(ReaderUtil.subIndex(scoreDoc.doc, leaves));
            SortedSetDocValues values = leaf.reader().getSortedSetDocValues(idField);
            assertTrue(values.advanceExact(scoreDoc.doc - leaf.docBase));
            ids.add(values.lookupOrd(values.nextOrd()).utf8ToString());
        }
        return ids;
    }

    private static List<Hit> hits(List<Book> books, Target target) {
        List<Hit> hits = new ArrayList<>();
        for (Book book : books) {
            for (Chapter chapter : book.chapters()) {
                if (target.depth == 1) {
                    hits.add(new Hit(book, chapter, null));
                } else {
                    for (Paragraph paragraph : chapter.paragraphs()) {
                        hits.add(new Hit(book, chapter, paragraph));
                    }
                }
            }
        }
        return hits;
    }

    private static String id(Hit hit) {
        return hit.paragraph() == null ? hit.chapter().cid() : hit.paragraph().pid();
    }

    // Random filters

    private Filter randomFilter(Target target, int depth) {
        if (depth == 0 || randomBoolean()) {
            return randomLeaf(target);
        }
        List<List<Filter>> clauses = List.of(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        int count = randomIntBetween(1, 4);
        for (int i = 0; i < count; i++) {
            clauses.get(randomIntBetween(0, 3)).add(randomFilter(target, depth - 1));
        }
        return bool(clauses.get(0), clauses.get(1), clauses.get(2), clauses.get(3));
    }

    private Filter randomLeaf(Target target) {
        int level = randomIntBetween(0, target.depth);
        return switch (level) {
            case 0 -> randomBookLeaf(target);
            case 1 -> randomChapterLeaf(target);
            case 2 -> randomParagraphLeaf();
            default -> throw new AssertionError(level);
        };
    }

    private Filter randomBookLeaf(Target target) {
        String genre = randomFrom(GENRES);
        String author = randomFrom(AUTHORS);
        long year = randomLongBetween(1990, 2000);
        return switch (randomIntBetween(0, 12)) {
            case 0 -> books("term genre", QueryBuilders.termQuery("genre", genre), b -> genre.equals(b.genre()));
            case 1 -> books("terms genre", QueryBuilders.termsQuery("genre", genre, "none"), b -> genre.equals(b.genre()));
            case 2 -> books("range year", QueryBuilders.rangeQuery("year").gte(year), b -> b.year() >= year);
            case 3 -> books("prefix genre", QueryBuilders.prefixQuery("genre", genre.substring(0, 3)), b -> genre.equals(b.genre()));
            case 4 -> books("wildcard genre", QueryBuilders.wildcardQuery("genre", "*" + genre.substring(2)), b -> genre.equals(b.genre()));
            case 5 -> books("exists genre", QueryBuilders.existsQuery("genre"), b -> b.genre() != null);
            case 6 -> books("query_string genre", QueryBuilders.queryStringQuery("genre:" + genre), b -> genre.equals(b.genre()));
            case 7 -> new Filter(
                "nested authors",
                QueryBuilders.nestedQuery("authors", QueryBuilders.termQuery("authors.name", author), ScoreMode.None),
                h -> h.book().authors().contains(author),
                Set.of(0)
            );
            case 8 -> new Filter("match_all", QueryBuilders.matchAllQuery(), h -> true, Set.of());
            case 9 -> new Filter("match_none", new MatchNoneQueryBuilder(), h -> false, Set.of());
            case 10 -> {
                // joins to the innermost enclosing level that contains the path
                Predicate<Hit> matches = target.scopeDepth >= 1
                    ? h -> h.chapter().paragraphs().stream().anyMatch(p -> "alpha".equals(p.text()))
                    : h -> h.book().chapters().stream().flatMap(c -> c.paragraphs().stream()).anyMatch(p -> "alpha".equals(p.text()));
                yield new Filter(
                    "nested paragraphs",
                    QueryBuilders.nestedQuery(
                        "chapters.paragraphs",
                        QueryBuilders.termQuery("chapters.paragraphs.text", "alpha"),
                        ScoreMode.None
                    ),
                    matches,
                    Set.of(0)
                );
            }
            case 11 -> new Filter(
                "multi_match genre language",
                QueryBuilders.multiMatchQuery(genre, "genre", "chapters.language"),
                b -> genre.equals(b.book().genre()),
                Set.of(0, 1)
            );
            case 12 -> {
                // every book has a vector and the similarity keeps them all, so the knn query matches the books its filter matches
                Float similarity = randomBoolean() ? null : 10f;
                KnnVectorQueryBuilder knn = new KnnVectorQueryBuilder(
                    "vector",
                    new float[] { 1, 1 },
                    10_000,
                    10_000,
                    null,
                    null,
                    similarity
                );
                if (randomBoolean()) {
                    yield books("knn vector", knn, b -> true);
                }
                knn.addFilterQuery(QueryBuilders.termQuery("genre", genre));
                yield books("knn vector filtered on genre", knn, b -> genre.equals(b.genre()));
            }
            default -> throw new AssertionError();
        };
    }

    private Filter randomChapterLeaf(Target target) {
        String language = randomFrom(LANGUAGES);
        String title = randomFrom(TITLES);
        long num = randomLongBetween(0, 5);
        return switch (randomIntBetween(0, 6)) {
            case 0 -> chapters("term language", QueryBuilders.termQuery("chapters.language", language), c -> language.equals(c.language()));
            case 1 -> chapters(
                "prefix title",
                QueryBuilders.prefixQuery("chapters.title", title.substring(0, 2)),
                c -> title.equals(c.title())
            );
            case 2 -> chapters("exists title", QueryBuilders.existsQuery("chapters.title"), c -> c.title() != null);
            case 3 -> chapters("range num", QueryBuilders.rangeQuery("chapters.num").lte(num), c -> c.num() <= num);
            case 4 -> chapters(
                "regexp language",
                QueryBuilders.regexpQuery("chapters.language", language.charAt(0) + ".*"),
                c -> language.equals(c.language())
            );
            case 5 -> new Filter(
                "query_string genre or language",
                QueryBuilders.queryStringQuery("genre:nature OR chapters.language:" + language),
                h -> "nature".equals(h.book().genre()) || language.equals(h.chapter().language()),
                Set.of(0, 1)
            );
            case 6 -> new Filter(
                "nested chapters",
                QueryBuilders.nestedQuery("chapters", QueryBuilders.termQuery("chapters.language", language), ScoreMode.None),
                h -> h.book().chapters().stream().anyMatch(c -> language.equals(c.language())),
                Set.of(0)
            );
            default -> throw new AssertionError();
        };
    }

    private Filter randomParagraphLeaf() {
        String text = randomFrom(TEXTS);
        String word = randomFrom(WORDS);
        String next = randomFrom(WORDS);
        return switch (randomIntBetween(0, 6)) {
            case 0 -> paragraphs("term text", QueryBuilders.termQuery("chapters.paragraphs.text", text), p -> text.equals(p.text()));
            case 1 -> paragraphs(
                "prefix text",
                QueryBuilders.prefixQuery("chapters.paragraphs.text", text.substring(0, 2)),
                p -> text.equals(p.text())
            );
            case 2 -> paragraphs(
                "wildcard text",
                QueryBuilders.wildcardQuery("chapters.paragraphs.text", "*" + text.substring(1)),
                p -> text.equals(p.text())
            );
            case 3 -> paragraphs("exists text", QueryBuilders.existsQuery("chapters.paragraphs.text"), p -> p.text() != null);
            case 4 -> paragraphs("match body", QueryBuilders.matchQuery("chapters.paragraphs.body", word), p -> p.body().contains(word));
            case 5 -> paragraphs(
                "match_phrase body",
                QueryBuilders.matchPhraseQuery("chapters.paragraphs.body", word + " " + next),
                p -> containsPhrase(p.body(), word, next)
            );
            case 6 -> new Filter(
                "dis_max genre text",
                QueryBuilders.disMaxQuery()
                    .add(QueryBuilders.termQuery("genre", "nature"))
                    .add(QueryBuilders.termQuery("chapters.paragraphs.text", text)),
                h -> "nature".equals(h.book().genre()) || text.equals(h.paragraph().text()),
                Set.of(0, 2)
            );
            default -> throw new AssertionError();
        };
    }

    private static Filter books(String description, QueryBuilder query, Predicate<Book> matches) {
        return new Filter(description, query, h -> matches.test(h.book()), Set.of(0));
    }

    private static Filter chapters(String description, QueryBuilder query, Predicate<Chapter> matches) {
        return new Filter(description, query, h -> matches.test(h.chapter()), Set.of(1));
    }

    private static Filter paragraphs(String description, QueryBuilder query, Predicate<Paragraph> matches) {
        return new Filter(description, query, h -> matches.test(h.paragraph()), Set.of(2));
    }

    /** A bool query with the semantics of Elasticsearch's {@code bool}: should clauses are optional next to required ones. */
    private static Filter bool(List<Filter> must, List<Filter> mustNot, List<Filter> should, List<Filter> filter) {
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        must.forEach(f -> query.must(f.query()));
        mustNot.forEach(f -> query.mustNot(f.query()));
        should.forEach(f -> query.should(f.query()));
        filter.forEach(f -> query.filter(f.query()));
        Predicate<Hit> matches = h -> {
            if (must.stream().anyMatch(f -> f.matches().test(h) == false) || filter.stream().anyMatch(f -> f.matches().test(h) == false)) {
                return false;
            }
            if (mustNot.stream().anyMatch(f -> f.matches().test(h))) {
                return false;
            }
            if (must.isEmpty() && filter.isEmpty() && should.isEmpty() == false) {
                return should.stream().anyMatch(f -> f.matches().test(h));
            }
            return true;
        };
        Set<Integer> levels = new HashSet<>();
        Function<List<Filter>, String> describe = fs -> fs.stream().map(Filter::description).toList().toString();
        for (List<Filter> clauses : List.of(must, mustNot, should, filter)) {
            clauses.forEach(f -> levels.addAll(f.levels()));
        }
        String description = "bool(must="
            + describe.apply(must)
            + ", must_not="
            + describe.apply(mustNot)
            + ", should="
            + describe.apply(should)
            + ", filter="
            + describe.apply(filter)
            + ")";
        return new Filter(description, query, matches, levels);
    }

    private static boolean containsPhrase(List<String> body, String first, String second) {
        for (int i = 0; i + 1 < body.size(); i++) {
            if (body.get(i).equals(first) && body.get(i + 1).equals(second)) {
                return true;
            }
        }
        return false;
    }

    // Corpus

    private List<Book> randomBooks() {
        List<Book> books = new ArrayList<>();
        int cid = 0;
        int pid = 0;
        int numBooks = randomIntBetween(20, 40);
        for (int b = 0; b < numBooks; b++) {
            List<Chapter> chapters = new ArrayList<>();
            int numChapters = randomIntBetween(0, 3);
            for (int c = 0; c < numChapters; c++) {
                List<Paragraph> paragraphs = new ArrayList<>();
                int numParagraphs = randomIntBetween(0, 3);
                for (int p = 0; p < numParagraphs; p++) {
                    List<String> body = randomList(1, 4, () -> randomFrom(WORDS));
                    paragraphs.add(new Paragraph("p" + pid++, randomBoolean() ? randomFrom(TEXTS) : null, body));
                }
                chapters.add(
                    new Chapter(
                        "c" + cid++,
                        randomFrom(LANGUAGES),
                        randomBoolean() ? randomFrom(TITLES) : null,
                        randomLongBetween(0, 5),
                        paragraphs
                    )
                );
            }
            books.add(
                new Book(
                    randomBoolean() ? randomFrom(GENRES) : null,
                    randomLongBetween(1990, 2000),
                    randomSubsetOf(List.of(AUTHORS)),
                    chapters
                )
            );
        }
        return books;
    }

    private static XContentBuilder toSource(Book book) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        if (book.genre() != null) {
            builder.field("genre", book.genre());
        }
        builder.field("year", book.year());
        builder.array("vector", randomFloat(), randomFloat());
        builder.startArray("authors");
        for (String author : book.authors()) {
            builder.startObject().field("name", author).endObject();
        }
        builder.endArray();
        builder.startArray("chapters");
        for (Chapter chapter : book.chapters()) {
            builder.startObject().field("cid", chapter.cid()).field("language", chapter.language()).field("num", chapter.num());
            if (chapter.title() != null) {
                builder.field("title", chapter.title());
            }
            builder.array("vector", randomFloat(), randomFloat());
            builder.startArray("paragraphs");
            for (Paragraph paragraph : chapter.paragraphs()) {
                builder.startObject().field("pid", paragraph.pid()).field("body", String.join(" ", paragraph.body()));
                if (paragraph.text() != null) {
                    builder.field("text", paragraph.text());
                }
                builder.array("vector", randomFloat(), randomFloat());
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
        }
        builder.endArray();
        return builder.endObject();
    }

    @FunctionalInterface
    interface IndexTest {
        void accept(MapperService mapperService, IndexSearcher searcher) throws IOException;
    }

    private void withIndex(List<Book> books, IndexTest test) throws IOException {
        MapperService mapperService = createMapperService(mapping());
        withLuceneIndex(mapperService, iw -> {
            for (Book book : books) {
                iw.addDocuments(mapperService.documentMapper().parse(source(Strings.toString(toSource(book)))).docs());
            }
        }, reader -> {
            DirectoryReader esReader = ElasticsearchDirectoryReader.wrap(reader, new ShardId("index", "_na_", 0));
            test.accept(mapperService, newSearcher(esReader, false, false));
        });
    }
}
