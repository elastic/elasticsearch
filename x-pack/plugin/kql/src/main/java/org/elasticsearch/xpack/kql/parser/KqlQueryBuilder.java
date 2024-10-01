/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.ql.parser.ParserUtils;

public class KqlQueryBuilder extends KqlBaseBaseVisitor<QueryBuilder> {

    private static final Logger log = LogManager.getLogger(KqlQueryBuilder.class);

    private final SearchExecutionContext searchExecutionContext;

    public KqlQueryBuilder(SearchExecutionContext searchExecutionContext) {
        this.searchExecutionContext = searchExecutionContext;
    }

    public QueryBuilder query(ParserRuleContext ctx) {
        if (ctx instanceof KqlBaseParser.TopLevelQueryContext topLeveQueryContext) {
            if (topLeveQueryContext.query() == null) {
                // In KQL, empty query matches all docs.
                return new MatchAllQueryBuilder();
            }

            return ParserUtils.typedParsing(this, topLeveQueryContext.query(), QueryBuilder.class);
        }

        throw new IllegalArgumentException("context should be of type TopLevelQueryContext");
    }

    @Override
    public QueryBuilder visitLogicalNot(KqlBaseParser.LogicalNotContext ctx) {
        BoolQueryBuilder builder = QueryBuilders.boolQuery();
        return builder.mustNot(ParserUtils.typedParsing(this, ctx.simpleQuery(), QueryBuilder.class));
    }

    @Override
    public QueryBuilder visitLogicalAnd(KqlBaseParser.LogicalAndContext ctx) {
        BoolQueryBuilder builder = QueryBuilders.boolQuery();

        // TODO: KQLContext has an option to wrap the clauses into a filter instead of a must clause. Do we need it?

        for (ParserRuleContext subQuery : ctx.query()) {
            if (subQuery instanceof KqlBaseParser.LogicalAndContext) {
                ParserUtils.typedParsing(this, subQuery, BoolQueryBuilder.class).must().forEach(builder::must);
            } else {
                builder.must(ParserUtils.typedParsing(this, subQuery, QueryBuilder.class));
            }
        }

        return builder;
    }

    @Override
    public QueryBuilder visitLogicalOr(KqlBaseParser.LogicalOrContext ctx) {
        BoolQueryBuilder builder = QueryBuilders.boolQuery();

        for (ParserRuleContext subQuery : ctx.query()) {
            if (subQuery instanceof KqlBaseParser.LogicalOrContext) {
                ParserUtils.typedParsing(this, subQuery, BoolQueryBuilder.class).should().forEach(builder::should);
            } else {
                builder.should(ParserUtils.typedParsing(this, subQuery, QueryBuilder.class));
            }
        }

        return builder;
    }

    @Override
    public QueryBuilder visitParenthesizedQuery(KqlBaseParser.ParenthesizedQueryContext ctx) {
        return ParserUtils.typedParsing(this, ctx.query(), QueryBuilder.class);
    }

    @Override
    public QueryBuilder visitFieldTermQuery(KqlBaseParser.FieldTermQueryContext ctx) {
        boolean isExistsQuery = ctx.termValue.wildcard() != null;
        boolean isAllFieldsQuery = ctx.fieldName() != null && ctx.fieldName().wildcard() != null;

        if ((ctx.fieldName() == null || isAllFieldsQuery) && isExistsQuery) {
            return QueryBuilders.matchAllQuery();
        }

        boolean isPhraseQuery = ctx.termValue.quotedString() != null;

        if (ctx.fieldName() == null) {
            return QueryBuilders.multiMatchQuery(ctx.termValue.getText())
                .type(isPhraseQuery ? MultiMatchQueryBuilder.Type.PHRASE : MultiMatchQueryBuilder.Type.BEST_FIELDS)
                .lenient(true);
        }

        return new MatchNoneQueryBuilder();
    }

    @Override
    public QueryBuilder visitFieldRangeQuery(KqlBaseParser.FieldRangeQueryContext ctx) {
        return super.visitFieldRangeQuery(ctx);
    }

    @Override
    public QueryBuilder visitNestedQuery(KqlBaseParser.NestedQueryContext ctx) {
        return super.visitNestedQuery(ctx);
    }

//    private Iterable<Map.Entry<String, MappedFieldType>> resolveFields(KqlBaseParser.FieldNameContext fieldNameContext) {
//
//        Iterable<Map.Entry<String, MappedFieldType>> fields = null;
//
//        if (fieldNameContext == null || fieldNameContext.wildcard() != null) {
//            // Wildcard: get all fields:
//            log.trace("Wildcard field name : {}");
//            fields = searchExecutionContext.getAllFields();
//        } else if (fieldNameContext.quotedString() != null) {
//            log.trace("Quoted field name : {}", unquote(fieldNameContext));
//            searchExecutionContext.getFieldType(unquote(fieldNameContext));
//        } else {
//            log.trace("Literal field name : {}", unescapeLiteral(fieldNameContext));
//            fields = searchExecutionContext.getMatchingFieldNames(unescapeLiteral(fieldNameContext))
//                .stream()
//                .map(fieldName -> Map.entry(fieldName, searchExecutionContext.getFieldType(fieldName)))
//                .collect(Collectors.toList());;
//        }
//
//        return fields;
//    }


    private String extract

    private String unescapeLiteral(KqlBaseParser.UnquotedLiteralContext ctx) {
        String inputText = ctx.getText();
        // TOOD: implement

        return inputText;
    }

    private String unquote(ParserRuleContext ctx) {
        String inputText = ctx.getText();

        assert inputText.length() > 2 && inputText.charAt(0) == '\"' && inputText.charAt(inputText.length() -1) == '\"' ;
        StringBuilder sb = new StringBuilder();

        for (int i = 1; i < inputText.length() - 1;) {
            if (inputText.charAt(i) == '\\') {
                // ANTLR4 Grammar guarantees there is always a character after the `\`
                switch (inputText.charAt(++i)) {
                    case 't' -> sb.append('\t');
                    case 'b' -> sb.append('\b');
                    case 'f' -> sb.append('\f');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case '"' -> sb.append('\"');
                    case '\'' -> sb.append('\'');
                    case 'u' -> i = handleUnicodePoints(ctx, sb, inputText, ++i);
                    case '\\' -> sb.append('\\');

                    // will be interpreted as regex, so we have to escape it
                    default ->
                        // unknown escape sequence, pass through as-is, e.g: `...\w...`
                        sb.append('\\').append(inputText.charAt(i));
                }
                i++;
            } else {
                sb.append(inputText.charAt(i++));
            }
        }
        return sb.toString();
    }

    private static int handleUnicodePoints(ParserRuleContext ctx, StringBuilder sb, String text, int startIdx) {
        int endIdx = startIdx + 4;
        sb.append(hexToUnicode(ctx, text.substring(startIdx, endIdx)));
        return endIdx;
    }

    private static String hexToUnicode(ParserRuleContext ctx, String hex) {
        try {
            int code = Integer.parseInt(hex, 16);
            // U+D800—U+DFFF can only be used as surrogate pairs and therefore are not valid character codes
            if (code >= 0xD800 && code <= 0xDFFF) {
                throw new ParsingException(
                    ctx.start.getLine(),
                    ctx.start.getCharPositionInLine(),
                    LoggerMessageFormat.format("Invalid unicode character code, [{}] is a surrogate code", hex),
                    null
                );
            }
            return String.valueOf(Character.toChars(code));
        } catch (IllegalArgumentException e) {
            throw new ParsingException(
                ctx.start.getLine(),
                ctx.start.getCharPositionInLine(),
                LoggerMessageFormat.format("Invalid unicode character code [{}]", hex),
                null
            );
        }
    }
}
