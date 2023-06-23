/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Holds the results of migrating a single feature. See also {@link FeatureMigrationResults}.
 */
public class SingleFeatureMigrationResult implements SimpleDiffable<SingleFeatureMigrationResult>, Writeable, ToXContentObject {
    private static final String NAME = "feature_migration_status";
    private static final ParseField SUCCESS_FIELD = new ParseField("successful");
    private static final ParseField FAILED_INDEX_NAME_FIELD = new ParseField("failed_index");
    private static final ParseField EXCEPTION_FIELD = new ParseField("exception");

    private final boolean successful;
    @Nullable
    private final String failedIndexName;
    @Nullable
    private final Exception exception;

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SingleFeatureMigrationResult, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        a -> new SingleFeatureMigrationResult((boolean) a[0], (String) a[1], (Exception) a[2])
    );

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), SUCCESS_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), FAILED_INDEX_NAME_FIELD);
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> ElasticsearchException.fromXContent(p),
            EXCEPTION_FIELD
        );
    }

    private SingleFeatureMigrationResult(boolean successful, String failedIndexName, Exception exception) {
        this.successful = successful;
        if (successful == false) {
            Objects.requireNonNull(failedIndexName, "failed index name must be present for failed feature migration statuses");
            Objects.requireNonNull(exception, "exception details must be present for failed feature migration statuses");
        }
        this.failedIndexName = failedIndexName;
        this.exception = exception;
    }

    SingleFeatureMigrationResult(StreamInput in) throws IOException {
        this.successful = in.readBoolean();
        if (this.successful == false) {
            this.failedIndexName = in.readString();
            this.exception = in.readException();
        } else {
            this.failedIndexName = null;
            this.exception = null;
        }
    }

    public static SingleFeatureMigrationResult fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /**
     * Creates a record indicating that migration succeeded.
     */
    public static SingleFeatureMigrationResult success() {
        return new SingleFeatureMigrationResult(true, null, null);
    }

    /**
     * Creates a record indicating that migration failed.
     * @param failedIndexName The name of the specific index whose migration failed.
     * @param exception The exception which caused the migration failure.
     */
    public static SingleFeatureMigrationResult failure(String failedIndexName, Exception exception) {
        return new SingleFeatureMigrationResult(false, failedIndexName, exception);
    }

    /**
     * Returns {@code true} if the migration of this feature's data succeeded, or {@code false} otherwise.
     */
    public boolean succeeded() {
        return successful;
    }

    /**
     * Gets the name of the specific index where the migration failure occurred, if the migration failed.
     */
    @Nullable
    public String getFailedIndexName() {
        return failedIndexName;
    }

    /**
     * Gets the exception that cause the migration failure, if the migration failed.
     */
    @Nullable
    public Exception getException() {
        return exception;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(this.successful);
        if (this.successful == false) {
            out.writeString(this.failedIndexName);
            out.writeException(this.exception);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(SUCCESS_FIELD.getPreferredName(), successful);
            if (successful == false) {
                builder.field(FAILED_INDEX_NAME_FIELD.getPreferredName(), failedIndexName);
                builder.startObject(EXCEPTION_FIELD.getPreferredName());
                {
                    ElasticsearchException.generateThrowableXContent(builder, params, exception);
                }
                builder.endObject();
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof SingleFeatureMigrationResult) == false) return false;
        SingleFeatureMigrationResult that = (SingleFeatureMigrationResult) o;
        // Exception is intentionally not checked here
        return successful == that.successful && Objects.equals(failedIndexName, that.failedIndexName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(successful, failedIndexName);
    }
}
