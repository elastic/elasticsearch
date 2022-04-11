/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.ql.expression.predicate.regex.LikePattern;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;

import static java.util.Collections.singletonList;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class LikeEscapingParsingTests extends ESTestCase {

    private final SqlParser parser = new SqlParser();

    private static LikePattern patternOfLike(Expression exp) {
        assertThat(exp, instanceOf(Like.class));
        Like l = (Like) exp;
        return l.pattern();
    }

    private String error(String pattern) {
        ParsingException ex = expectThrows(ParsingException.class, () -> parser.createExpression(format(null, "exp LIKE {}", pattern)));

        return ex.getMessage();
    }

    private LikePattern like(String pattern) {
        Expression exp = null;
        boolean parameterized = randomBoolean();
        if (parameterized) {
            exp = parser.createExpression("exp LIKE ?", singletonList(new SqlTypedParamValue(KEYWORD.typeName(), pattern)));
        } else {
            exp = parser.createExpression(format(null, "exp LIKE '{}'", pattern));
        }
        return patternOfLike(exp);
    }

    private LikePattern like(String pattern, Character escapeChar) {
        return patternOfLike(parser.createExpression(format(null, "exp LIKE '{}' ESCAPE '{}'", pattern, escapeChar)));
    }

    public void testNoEscaping() {
        LikePattern like = like("string");
        assertThat(like.pattern(), is("string"));
        assertThat(like.asJavaRegex(), is("^string$"));
        assertThat(like.asLuceneWildcard(), is("string"));
    }

    public void testEscapingLastChar() {
        assertThat(
            error("'string|' ESCAPE '|'"),
            is("line 1:11: Pattern [string|] is invalid as escape char [|] at position 6 does not escape anything")
        );
    }

    public void testEscapingWrongChar() {
        assertThat(
            error("'|string' ESCAPE '|'"),
            is(
                "line 1:11: Pattern [|string] is invalid as escape char [|] at position 0 can only escape "
                    + "wildcard chars [%_]; found [s]"
            )
        );
    }

    public void testEscapingTheEscapeCharacter() {
        assertThat(
            error("'||string' ESCAPE '|'"),
            is("line 1:11: Pattern [||string] is invalid as escape char [|] at position 0 can only escape wildcard chars [%_]; found [|]")
        );
    }

    public void testEscapingWildcards() {
        assertThat(
            error("'string' ESCAPE '%'"),
            is("line 1:27: Char [%] cannot be used for escaping as it's one of the wildcard chars [%_]")
        );
        assertThat(
            error("'string' ESCAPE '_'"),
            is("line 1:27: Char [_] cannot be used for escaping as it's one of the wildcard chars [%_]")
        );
    }

    public void testCanUseStarWithoutEscaping() {
        LikePattern like = like("%string*");
        assertThat(like.pattern(), is("%string*"));
        assertThat(like.asJavaRegex(), is("^.*string\\*$"));
        assertThat(like.asLuceneWildcard(), is("*string\\*"));
    }

    public void testEscapingWithStar() {
        LikePattern like = like("*%%*__string", '*');
        assertThat(like.pattern(), is("*%%*__string"));
        assertThat(like.asJavaRegex(), is("^%.*_.string$"));
        assertThat(like.asLuceneWildcard(), is("%*_?string"));
    }

}
