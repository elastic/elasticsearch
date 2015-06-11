package org.elasticsearch.index.query;

public class RandomTermQueryBuilder extends RandomBaseTermQueryBuilder<TermQueryBuilder> {

    @Override
    protected TermQueryBuilder createQueryBuilder(String fieldName, Object value) {
        return new TermQueryBuilder(fieldName, value);
    }
}
