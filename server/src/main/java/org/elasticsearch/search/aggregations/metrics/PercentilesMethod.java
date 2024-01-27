/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;

import java.io.IOException;

/**
 * An enum representing the methods for calculating percentiles
 */
public enum PercentilesMethod implements Writeable {
    /**
     * The TDigest method for calculating percentiles
     */
    TDIGEST("tdigest", "TDigest", "TDIGEST") {
        @Override
        PercentilesConfig configFromStream(StreamInput in) throws IOException {
            return new PercentilesConfig.TDigest(in);
        }
    },
    /**
     * The HDRHistogram method of calculating percentiles
     */
    HDR("hdr", "HDR") {
        @Override
        PercentilesConfig configFromStream(StreamInput in) throws IOException {
            return new PercentilesConfig.Hdr(in);
        }
    };

    public static final ParseField COMPRESSION_FIELD = new ParseField("compression");

    public static final ParseField EXECUTION_HINT_FIELD = new ParseField("execution_hint");
    public static final ParseField NUMBER_SIGNIFICANT_DIGITS_FIELD = new ParseField("number_of_significant_value_digits");

    public static final ObjectParser<PercentilesConfig.TDigest, String> TDIGEST_PARSER;
    static {
        TDIGEST_PARSER = new ObjectParser<>(PercentilesMethod.TDIGEST.getParseField().getPreferredName(), PercentilesConfig.TDigest::new);
        TDIGEST_PARSER.declareDouble(PercentilesConfig.TDigest::setCompression, COMPRESSION_FIELD);
        TDIGEST_PARSER.declareString(PercentilesConfig.TDigest::parseExecutionHint, EXECUTION_HINT_FIELD);
    }

    public static final ObjectParser<PercentilesConfig.Hdr, String> HDR_PARSER;
    static {
        HDR_PARSER = new ObjectParser<>(PercentilesMethod.HDR.getParseField().getPreferredName(), PercentilesConfig.Hdr::new);
        HDR_PARSER.declareInt(PercentilesConfig.Hdr::setNumberOfSignificantValueDigits, NUMBER_SIGNIFICANT_DIGITS_FIELD);
    }

    private final ParseField parseField;

    PercentilesMethod(String name, String... deprecatedNames) {
        this.parseField = new ParseField(name, deprecatedNames);
    }

    abstract PercentilesConfig configFromStream(StreamInput in) throws IOException;

    /**
     * @return the name of the method
     */
    public ParseField getParseField() {
        return parseField;
    }

    public static PercentilesMethod readFromStream(StreamInput in) throws IOException {
        return in.readEnum(PercentilesMethod.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    @Override
    public String toString() {
        return parseField.getPreferredName();
    }

}
