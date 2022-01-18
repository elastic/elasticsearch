/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class UpgradeTransformsResponse {

    public static final ParseField NO_ACTION = new ParseField("no_action");
    public static final ParseField UPDATED = new ParseField("updated");
    public static final ParseField NEEDS_UPDATE = new ParseField("needs_update");

    private static final ConstructingObjectParser<UpgradeTransformsResponse, Void> PARSER = new ConstructingObjectParser<>(
        "upgrade_transform",
        true,
        args -> {
            long updated = args[0] == null ? 0L : (Long) args[0];
            long noAction = args[1] == null ? 0L : (Long) args[1];
            long needsUpdate = args[2] == null ? 0L : (Long) args[2];

            return new UpgradeTransformsResponse(updated, noAction, needsUpdate);
        }
    );

    static {
        PARSER.declareLong(optionalConstructorArg(), UPDATED);
        PARSER.declareLong(optionalConstructorArg(), NO_ACTION);
        PARSER.declareLong(optionalConstructorArg(), NEEDS_UPDATE);
    }

    public static UpgradeTransformsResponse fromXContent(final XContentParser parser) {
        return UpgradeTransformsResponse.PARSER.apply(parser, null);
    }

    private final long updated;
    private final long noAction;
    private final long needsUpdate;

    public UpgradeTransformsResponse(long updated, long noAction, long needsUpdate) {
        this.updated = updated;
        this.noAction = noAction;
        this.needsUpdate = needsUpdate;
    }

    @Override
    public int hashCode() {
        return Objects.hash(updated, noAction, needsUpdate);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final UpgradeTransformsResponse that = (UpgradeTransformsResponse) other;
        return this.updated == that.updated && this.noAction == that.noAction && this.needsUpdate == that.needsUpdate;
    }

    public long getUpdated() {
        return updated;
    }

    public long getNoAction() {
        return noAction;
    }

    public long getNeedsUpdate() {
        return needsUpdate;
    }
}
