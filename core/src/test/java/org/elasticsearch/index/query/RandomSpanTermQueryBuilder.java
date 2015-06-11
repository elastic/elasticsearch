package org.elasticsearch.index.query;

public class RandomSpanTermQueryBuilder extends RandomBaseTermQueryBuilder<SpanTermQueryBuilder> {

    @Override
    protected SpanTermQueryBuilder createQueryBuilder(String fieldName, Object value) {
        return new SpanTermQueryBuilder(fieldName, value);
    }
}
