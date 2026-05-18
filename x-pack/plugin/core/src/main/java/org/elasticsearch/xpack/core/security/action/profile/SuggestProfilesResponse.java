/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.profile;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class SuggestProfilesResponse extends ActionResponse implements ToXContentObject {

    private final ProfileHit[] profileHits;
    private final long tookInMillis;
    private final TotalHits totalHits;

    public SuggestProfilesResponse(ProfileHit[] profileHits, long tookInMillis, TotalHits totalHits) {
        this.profileHits = profileHits;
        this.tookInMillis = tookInMillis;
        this.totalHits = totalHits;
    }

    public ProfileHit[] getProfileHits() {
        return profileHits;
    }

    public long getTookInMillis() {
        return tookInMillis;
    }

    public TotalHits getTotalHits() {
        return totalHits;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(profileHits);
        out.writeVLong(tookInMillis);
        Lucene.writeTotalHits(out, totalHits);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("took", tookInMillis);
            builder.startObject("total");
            {
                builder.field("value", totalHits.value());
                builder.field("relation", totalHits.relation() == TotalHits.Relation.EQUAL_TO ? "eq" : "gte");
            }
            builder.endObject();
            builder.startArray("profiles");
            {
                for (ProfileHit profileHit : profileHits) {
                    profileHit.toXContent(builder, params);
                }
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    public record ProfileHit(Profile profile, float score) implements Writeable, ToXContentObject {

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            profile.writeTo(out);
            out.writeFloat(score);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("uid", profile.uid());
                profile.user().toXContent(builder, params);
                builder.field("labels", profile.labels());
                builder.field("data", profile.applicationData());
            }
            builder.endObject();
            return builder;
        }
    }
}
