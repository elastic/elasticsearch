/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.execution.search.extractor;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.ql.execution.search.extractor.BucketExtractor;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class CompositeKeyExtractor implements BucketExtractor {

    /**
     * Key or Composite extractor.
     */
    static final String NAME = "k";
    private final String key;
    private final boolean isDateTimeBased;

    /**
     * Constructs a new <code>CompositeKeyExtractor</code> instance.
     */
    public CompositeKeyExtractor(String key, boolean isDateTimeBased) {
        this.key = key;
        this.isDateTimeBased = isDateTimeBased;
    }

    CompositeKeyExtractor(StreamInput in) throws IOException {
        key = in.readString();
        isDateTimeBased = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(key);
        out.writeBoolean(isDateTimeBased);
    }

    public String key() {
        return key;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object extract(Bucket bucket) {
        // get the composite value
        Object m = bucket.getKey();

        if ((m instanceof Map) == false) {
            throw new EqlIllegalArgumentException("Unexpected bucket returned: {}", m);
        }

        Object object = ((Map<?, ?>) m).get(key);

        if (isDateTimeBased) {
            if (object == null) {
                return object;
            } else if (object instanceof Long) {
                // object = DateUtils.asDateTimeWithNanos(((Long) object).longValue(), zoneId);
                return object;
            } else {
                throw new EqlIllegalArgumentException("Invalid date key returned: {}", object);
            }
        }

        return object;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, isDateTimeBased);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        CompositeKeyExtractor other = (CompositeKeyExtractor) obj;
        return Objects.equals(key, other.key) && Objects.equals(isDateTimeBased, other.isDateTimeBased);
    }

    @Override
    public String toString() {
        return "|" + key + "|";
    }
}
