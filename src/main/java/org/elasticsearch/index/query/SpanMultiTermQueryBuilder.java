package org.elasticsearch.index.query;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;

public class SpanMultiTermQueryBuilder extends BaseQueryBuilder implements SpanQueryBuilder{

	private MultiTermQueryBuilder multiTermQueryBuilder;
	
	public SpanMultiTermQueryBuilder(MultiTermQueryBuilder multiTermQueryBuilder) {
		this.multiTermQueryBuilder = multiTermQueryBuilder;
	}

	@Override
	protected void doXContent(XContentBuilder builder, Params params)
			throws IOException {
        builder.startObject(SpanMultiTermQueryParser.NAME);
        builder.field(SpanMultiTermQueryParser.MATCH_NAME);
        multiTermQueryBuilder.toXContent(builder, params);
        builder.endObject();		
	}

}
