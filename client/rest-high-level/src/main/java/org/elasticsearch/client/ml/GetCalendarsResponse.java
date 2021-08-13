/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.calendars.Calendar;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class GetCalendarsResponse extends AbstractResultResponse<Calendar> {

    public static final ParseField RESULTS_FIELD = new ParseField("calendars");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<GetCalendarsResponse, Void> PARSER =
            new ConstructingObjectParser<>("calendars_response", true,
                    a -> new GetCalendarsResponse((List<Calendar>) a[0], (long) a[1]));

    static {
        PARSER.declareObjectArray(constructorArg(), Calendar.PARSER, RESULTS_FIELD);
        PARSER.declareLong(constructorArg(), AbstractResultResponse.COUNT);
    }

    public static GetCalendarsResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    GetCalendarsResponse(List<Calendar> calendars, long count) {
        super(RESULTS_FIELD, calendars, count);
    }

    /**
     * The collection of {@link Calendar} objects found in the query
     */
    public List<Calendar> calendars() {
        return results;
    }

    @Override
    public int hashCode() {
        return Objects.hash(results, count);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        GetCalendarsResponse other = (GetCalendarsResponse) obj;
        return Objects.equals(results, other.results) && count == other.count;
    }

    @Override
    public final String toString() {
        return Strings.toString(this);
    }
}
