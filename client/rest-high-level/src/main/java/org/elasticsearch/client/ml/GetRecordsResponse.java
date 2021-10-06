/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.results.AnomalyRecord;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A response containing the requested records
 */
public class GetRecordsResponse extends AbstractResultResponse<AnomalyRecord> {

    public static final ParseField RECORDS = new ParseField("records");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<GetRecordsResponse, Void> PARSER = new ConstructingObjectParser<>("get_records_response",
            true, a -> new GetRecordsResponse((List<AnomalyRecord>) a[0], (long) a[1]));

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), AnomalyRecord.PARSER, RECORDS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), COUNT);
    }

    public static GetRecordsResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    GetRecordsResponse(List<AnomalyRecord> records, long count) {
        super(RECORDS, records, count);
    }

    /**
     * The retrieved records
     * @return the retrieved records
     */
    public List<AnomalyRecord> records() {
        return results;
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, results);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GetRecordsResponse other = (GetRecordsResponse) obj;
        return count == other.count && Objects.equals(results, other.results);
    }
}
