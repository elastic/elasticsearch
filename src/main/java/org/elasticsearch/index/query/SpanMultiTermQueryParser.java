package org.elasticsearch.index.query;

import java.io.IOException;

import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

import sun.swing.plaf.synth.Paint9Painter.PaintType;

public class SpanMultiTermQueryParser implements QueryParser {

	public static final String NAME = "span_multi_term";
	public static final String MATCH_NAME = "match";

	@Inject
	public SpanMultiTermQueryParser() {
	}

	@Override
	public String[] names() {
		return new String[] { NAME, Strings.toCamelCase(NAME) };
	}

	@Override
	@Nullable
	public Query parse(QueryParseContext parseContext) throws IOException,
			QueryParsingException {
		XContentParser parser = parseContext.parser();
		Token token = parser.nextToken();
		checkCorrectField(parseContext, parser, token);
		token = parser.nextToken();
		checkHasObject(parseContext, parser.currentToken());
		Query ret = new SpanMultiTermQueryWrapper<MultiTermQuery>(getSubQuery(
				parseContext, parser));
		parser.nextToken();
		return ret;
	}


	private void checkCorrectField(QueryParseContext parseContext,
			XContentParser parser, Token token) throws IOException {
		if (!MATCH_NAME.equals(parser.currentName())
				|| token != XContentParser.Token.FIELD_NAME) {
			throwInvalidClause(parseContext);
		}
	}

	private MultiTermQuery getSubQuery(QueryParseContext parseContext,
			XContentParser parser) throws IOException {
		return tryParseSubQuery(parseContext, parser);
	}

	private MultiTermQuery tryParseSubQuery(QueryParseContext parseContext,
			XContentParser parser) throws IOException {
		Query subQuery = parseContext.parseInnerQuery();
		if (!(subQuery instanceof MultiTermQuery)) {
			throwInvalidSub(parseContext);
		}
		return (MultiTermQuery) subQuery;
	}

	private void throwInvalidSub(QueryParseContext parseContext) {
		throw new QueryParsingException(parseContext.index(), "spanMultiTerm ["
				+ MATCH_NAME + "] must be of type multi term query");
	}

	private void checkHasObject(QueryParseContext parseContext, Token token) {
		if (token != XContentParser.Token.START_OBJECT) {
			throwInvalidClause(parseContext);
		}
	}

	private void throwInvalidClause(QueryParseContext parseContext) {
		throw new QueryParsingException(parseContext.index(),
				"spanMultiTerm must have [" + MATCH_NAME
						+ "] multi term query clause");
	}
}
