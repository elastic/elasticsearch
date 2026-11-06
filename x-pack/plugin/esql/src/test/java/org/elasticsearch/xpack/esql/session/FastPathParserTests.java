/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.session.FastPathParser.And;
import org.elasticsearch.xpack.esql.session.FastPathParser.Cmp;
import org.elasticsearch.xpack.esql.session.FastPathParser.CmpOp;
import org.elasticsearch.xpack.esql.session.FastPathParser.FastQuery;
import org.elasticsearch.xpack.esql.session.FastPathParser.LikePred;
import org.elasticsearch.xpack.esql.session.FastPathParser.Lit;
import org.elasticsearch.xpack.esql.session.FastPathParser.MatchPred;
import org.elasticsearch.xpack.esql.session.FastPathParser.Not;
import org.elasticsearch.xpack.esql.session.FastPathParser.Or;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * The recognizer only has to be exact about what it accepts: every accepted query must have the same meaning under the
 * full parser, and anything it is unsure about must be rejected so the full pipeline handles it.
 */
public class FastPathParserTests extends ESTestCase {

    public void testFromOnly() {
        FastQuery q = FastPathParser.parse("FROM docs");
        assertThat(q.indexPattern(), equalTo("docs"));
        assertThat(q.metadata(), equalTo(List.of()));
        assertThat(q.where(), nullValue());
        assertThat(q.keep(), nullValue());
        assertThat(q.sort(), equalTo(List.of()));
        assertThat(q.limit(), nullValue());
        assertThat(q.referencedFields(), equalTo(Set.of()));
    }

    public void testFullShape() {
        FastQuery q = FastPathParser.parse(
            "from docs-*, other | where match(body, \"lucene\") and (id > 5 or not category == \"x\") "
                + "| keep id, category | sort id desc nulls last, category | limit 10"
        );
        assertThat(q.indexPattern(), equalTo("docs-*,other"));
        assertThat(q.where(), instanceOf(And.class));
        And and = (And) q.where();
        assertThat(and.left(), instanceOf(MatchPred.class));
        assertThat(((MatchPred) and.left()).field(), equalTo("body"));
        assertThat(((MatchPred) and.left()).lit(), equalTo(new Lit(Lit.Kind.STRING, "lucene")));
        assertThat(and.right(), instanceOf(Or.class));
        Or or = (Or) and.right();
        Cmp gt = (Cmp) or.left();
        assertThat(gt.op(), equalTo(CmpOp.GT));
        assertThat(gt.lit(), equalTo(new Lit(Lit.Kind.INTEGER, "5")));
        assertThat(or.right(), instanceOf(Not.class));
        assertThat(q.keep(), equalTo(List.of("id", "category")));
        assertThat(q.sort().size(), equalTo(2));
        assertThat(q.sort().get(0).ascending(), equalTo(false));
        assertThat(q.sort().get(0).nullsFirst(), equalTo(false));
        assertThat(q.sort().get(1).ascending(), equalTo(true));
        assertThat(q.sort().get(1).nullsFirst(), nullValue());
        assertThat(q.limit(), equalTo(10));
        assertThat(q.referencedFields(), contains("body", "id", "category"));
    }

    public void testColonMatchAndSourceText() {
        FastQuery q = FastPathParser.parse("FROM docs | WHERE title : \"fox\" | LIMIT 1");
        MatchPred match = (MatchPred) q.where();
        assertThat(match.field(), equalTo("title"));
        assertThat(match.source().text(), equalTo("title : \"fox\""));
        assertThat(match.fieldSource().text(), equalTo("title"));
        assertThat(match.kind(), equalTo(MatchPred.MatchKind.OPERATOR));
        MatchPred phrase = (MatchPred) FastPathParser.parse("FROM docs | WHERE match_phrase(body, \"quick fox\")").where();
        assertThat(phrase.kind(), equalTo(MatchPred.MatchKind.MATCH_PHRASE));
        assertThat(phrase.lit(), equalTo(new Lit(Lit.Kind.STRING, "quick fox")));
        assertThat(phrase.source().text(), equalTo("match_phrase(body, \"quick fox\")"));
        Cmp cmp = (Cmp) FastPathParser.parse("FROM docs | WHERE category == \"cat3\"").where();
        assertThat(cmp.source().text(), equalTo("category == \"cat3\""));
        assertThat(cmp.fieldSource().text(), equalTo("category"));
        assertThat(cmp.source().source().getColumnNumber(), equalTo(19));
    }

    public void testRejected() {
        String[] rejected = {
            "ROW a = 1",
            "FROM docs | STATS c = COUNT(*)",
            "FROM docs | EVAL x = 1",
            "FROM docs METADATA _index",
            "FROM remote:docs",
            "FROM docs | WHERE id > 5 | WHERE id < 10",       // repeated command
            "FROM docs | KEEP id | WHERE category == \"x\"",  // filter on a dropped field
            "FROM docs | LIMIT 10 | SORT id",                 // sorts the truncated rows
            "FROM docs | LIMIT 10 | WHERE id > 5",            // filters the truncated rows
            "FROM docs | KEEP id | SORT category",            // sort on a dropped field
            "FROM docs METADATA _source, _index",
            "FROM docs METADATA _source, _source",            // the analyzer keeps the last occurrence
            "FROM docs METADATA _*",
            "FROM docs METADATA _version",
            "FROM docs | WHERE title : true",
            "FROM docs | WHERE title : id",
            "FROM docs | WHERE id > 5 | LIMIT 10 | LIMIT 5",
            "FROM docs | WHERE id IN (1, 2)",
            "FROM docs | WHERE id IS NULL",
            "FROM docs | WHERE name LIKE (\"a*\", \"b*\")",
            "FROM docs | WHERE name LIKE pattern",
            "FROM docs | WHERE name NOT == \"a\"",
            "FROM docs | WHERE name RLIKE \"a.*\"",
            "FROM docs | WHERE ts > 1h",
            "FROM docs | WHERE ts > NOW()",
            "FROM docs | WHERE id == ?",
            "FROM docs | WHERE id = 5",
            "FROM docs | WHERE `name` == \"x\"",
            "FROM docs | WHERE name == \"a\\\"b\"",
            "FROM docs | WHERE name == \"\"\"x\"\"\"",
            "FROM docs | KEEP id*",
            "FROM docs | KEEP id, id",
            "FROM docs | LIMIT -1",
            "FROM docs | LIMIT 1.5",
            "FROM docs | WHERE match(body, query)",
            "FROM docs | WHERE match(body, \"x\", {\"boost\": 2})",
            "FROM docs | WHERE match_phrase(body, \"x\", {\"slop\": 2})",
            "FROM docs | WHERE match_phrase(status, 404)",
            "SET a = 1; FROM docs",
            "EXPLAIN (FROM docs)",
            "FROM docs // comment",
            "FROM docs |",
            "" };
        for (String query : rejected) {
            assertThat(query, FastPathParser.parse(query), nullValue());
        }
    }

    public void testSearchStyleQuery() {
        FastQuery q = FastPathParser.parse(
            "FROM logs-* METADATA _source | WHERE event.dataset: \"apache.access\" AND http.response.status_code: 404 "
                + "AND user_agent.name: \"Firefox\" AND @timestamp >= \"2024-01-01T09:10:05.477Z\" "
                + "AND @timestamp <= \"2024-01-02T21:10:05.477Z\" | SORT @timestamp DESC | LIMIT 500 | KEEP _source"
        );
        assertNotNull(q);
        assertThat(q.indexPattern(), equalTo("logs-*"));
        assertThat(q.metadata(), equalTo(List.of("_source")));
        assertThat(q.keep(), equalTo(List.of("_source")));
        assertThat(q.sort().size(), equalTo(1));
        assertThat(q.sort().get(0).field(), equalTo("@timestamp"));
        assertThat(q.limit(), equalTo(500));
        // _source is metadata, not something field caps resolves
        assertThat(q.referencedFields(), contains("event.dataset", "http.response.status_code", "user_agent.name", "@timestamp"));
        And and = (And) q.where();
        Cmp upper = (Cmp) and.right();
        assertThat(upper.op(), equalTo(CmpOp.LTE));
        assertThat(upper.lit(), equalTo(new Lit(Lit.Kind.STRING, "2024-01-02T21:10:05.477Z")));
        MatchPred status = (MatchPred) ((And) ((And) ((And) and.left()).left()).left()).right();
        assertThat(status.field(), equalTo("http.response.status_code"));
        assertThat(status.lit(), equalTo(new Lit(Lit.Kind.INTEGER, "404")));
        assertThat(status.kind(), equalTo(MatchPred.MatchKind.OPERATOR));
    }

    public void testCommandOrder() {
        // Equivalent spellings of the same query are all accepted
        for (String query : new String[] {
            "FROM docs | WHERE id > 5 | KEEP id, category | SORT id | LIMIT 10",
            "FROM docs | KEEP id, category | WHERE id > 5 | SORT id | LIMIT 10",
            "FROM docs | SORT id | WHERE id > 5 | KEEP id, category | LIMIT 10",
            "FROM docs | WHERE id > 5 | SORT id | LIMIT 10 | KEEP id, category",
            "FROM docs | KEEP id, category | SORT id | WHERE id > 5 | LIMIT 10" }) {
            FastQuery q = FastPathParser.parse(query);
            assertNotNull(query, q);
            assertThat(query, q.keep(), equalTo(List.of("id", "category")));
            assertThat(query, q.sort().get(0).field(), equalTo("id"));
            assertThat(query, q.limit(), equalTo(10));
            assertThat(query, ((Cmp) q.where()).field(), equalTo("id"));
        }
        // SORT before KEEP may use a field KEEP then drops
        FastQuery q = FastPathParser.parse("FROM docs | SORT category | KEEP id");
        assertThat(q.sort().get(0).field(), equalTo("category"));
        assertThat(q.referencedFields(), contains("id", "category"));
        // SORT _source is recognized here and rejected when planned, where the field's type is known
        assertThat(FastPathParser.parse("FROM docs METADATA _source | SORT _source").sort().get(0).field(), equalTo("_source"));
        // MATCH with a numeric query
        MatchPred match = (MatchPred) FastPathParser.parse("FROM docs | WHERE match(status, 4.5)").where();
        assertThat(match.lit(), equalTo(new Lit(Lit.Kind.DECIMAL, "4.5")));
        assertThat(match.kind(), equalTo(MatchPred.MatchKind.MATCH));
    }

    /** The shape the wikipedia rally track sends: scored full text search returning the id, the score and the source. */
    public void testScoredSearch() {
        FastQuery q = FastPathParser.parse(
            "FROM wikipedia METADATA _id, _score, _source | WHERE MATCH(title, \"Cleopatra\") OR MATCH(content, \"Cleopatra\")"
                + " | KEEP _id, _score, _source | SORT _score DESC | LIMIT 20"
        );
        assertNotNull(q);
        assertThat(q.metadata(), equalTo(List.of("_id", "_score", "_source")));
        assertThat(q.keep(), equalTo(List.of("_id", "_score", "_source")));
        assertThat(q.sort().get(0).field(), equalTo("_score"));
        assertThat(q.sort().get(0).ascending(), equalTo(false));
        assertThat(q.limit(), equalTo(20));
        // metadata fields are not mapped fields, so field caps only asks for the searched ones
        assertThat(q.referencedFields(), contains("title", "content"));
        assertThat(q.where(), instanceOf(Or.class));
        // the ids-only variant
        q = FastPathParser.parse(
            "FROM wikipedia METADATA _id, _score | WHERE MATCH(title, \"x\") | KEEP _id, _score | SORT _score DESC | LIMIT 20"
        );
        assertThat(q.metadata(), equalTo(List.of("_id", "_score")));
        assertThat(q.referencedFields(), contains("title"));
        // any order, and sorting on _id
        q = FastPathParser.parse("FROM docs METADATA _score, _id | SORT _id | KEEP _score");
        assertThat(q.metadata(), equalTo(List.of("_score", "_id")));
        assertThat(q.sort().get(0).field(), equalTo("_id"));
        // KEEP of a metadata field that was not requested is recognized here and rejected when planned
        assertThat(FastPathParser.parse("FROM docs | KEEP _score").keep(), equalTo(List.of("_score")));
    }

    public void testLike() {
        FastQuery q = FastPathParser.parse(
            "FROM logs-* METADATA _source | WHERE event.dataset == \"kafka.log\" AND message LIKE \"Connection * disconnected\" "
                + "AND @timestamp >= \"2024-01-01T09:10:05.477Z\" | SORT @timestamp DESC | LIMIT 500 | KEEP _source"
        );
        assertNotNull(q);
        LikePred like = (LikePred) ((And) ((And) q.where()).left()).right();
        assertThat(like.field(), equalTo("message"));
        assertThat(like.pattern(), equalTo("Connection * disconnected"));
        assertThat(like.source().text(), equalTo("message LIKE \"Connection * disconnected\""));
        assertThat(q.referencedFields(), contains("event.dataset", "message", "@timestamp"));
        Not not = (Not) FastPathParser.parse("FROM docs | WHERE name NOT LIKE \"a*\"").where();
        assertThat(((LikePred) not.child()).pattern(), equalTo("a*"));
    }

    public void testMultiLineSourcePositions() {
        FastPathParser.FastQuery q = FastPathParser.parse("FROM employees\n| WHERE salary_change.int > 1 AND emp_no < 10\n| LIMIT 5");
        assertNotNull(q);
        FastPathParser.And and = (FastPathParser.And) q.where();
        FastPathParser.Cmp first = (FastPathParser.Cmp) and.left();
        // "salary_change.int > 1" starts on line 2 at 0-based column 8, which the analyzer reports as 2:9
        assertEquals(2, first.source().source().getLineNumber());
        assertEquals(9, first.source().source().getColumnNumber());
        assertEquals("salary_change.int > 1", first.source().text());
        assertEquals(2, first.fieldSource().source().getLineNumber());
        assertEquals(9, first.fieldSource().source().getColumnNumber());
    }

}
