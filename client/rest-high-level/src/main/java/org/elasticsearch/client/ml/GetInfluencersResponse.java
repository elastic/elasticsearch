/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.results.Influencer;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A response containing the requested influencers
 */
public class GetInfluencersResponse extends AbstractResultResponse<Influencer> {

    public static final ParseField INFLUENCERS = new ParseField("influencers");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<GetInfluencersResponse, Void> PARSER = new ConstructingObjectParser<>(
            "get_influencers_response", true, a -> new GetInfluencersResponse((List<Influencer>) a[0], (long) a[1]));

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), Influencer.PARSER, INFLUENCERS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), COUNT);
    }

    public static GetInfluencersResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    GetInfluencersResponse(List<Influencer> influencers, long count) {
        super(INFLUENCERS, influencers, count);
    }

    /**
     * The retrieved influencers
     * @return the retrieved influencers
     */
    public List<Influencer> influencers() {
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
        GetInfluencersResponse other = (GetInfluencersResponse) obj;
        return count == other.count && Objects.equals(results, other.results);
    }
}
