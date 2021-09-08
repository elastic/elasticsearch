/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;


/**
 * A response acknowledging the deletion of expired data
 */
public class DeleteExpiredDataResponse implements ToXContentObject {

    private static final ParseField DELETED = new ParseField("deleted");

    public DeleteExpiredDataResponse(boolean deleted) {
        this.deleted = deleted;
    }

    public static final ConstructingObjectParser<DeleteExpiredDataResponse, Void> PARSER =
        new ConstructingObjectParser<>("delete_expired_data_response", true,
            a -> new DeleteExpiredDataResponse((Boolean) a[0]));

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), DELETED);
    }

    public static DeleteExpiredDataResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final Boolean deleted;

    public Boolean getDeleted() {
        return deleted;
    }

    @Override
    public int hashCode() {
        return Objects.hash(deleted);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (deleted != null) {
            builder.field(DELETED.getPreferredName(), deleted);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DeleteExpiredDataResponse response = (DeleteExpiredDataResponse) obj;
        return Objects.equals(deleted, response.deleted);
    }
}
