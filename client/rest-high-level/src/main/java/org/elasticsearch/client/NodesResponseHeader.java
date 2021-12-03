/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A utility class to parse the Nodes Header returned by
 * {@link RestActions#buildNodesHeader(XContentBuilder, ToXContent.Params, BaseNodesResponse)}.
 */
public final class NodesResponseHeader {

    public static final ParseField TOTAL = new ParseField("total");
    public static final ParseField SUCCESSFUL = new ParseField("successful");
    public static final ParseField FAILED = new ParseField("failed");
    public static final ParseField FAILURES = new ParseField("failures");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<NodesResponseHeader, Void> PARSER = new ConstructingObjectParser<>(
        "nodes_response_header",
        true,
        (a) -> {
            int i = 0;
            int total = (Integer) a[i++];
            int successful = (Integer) a[i++];
            int failed = (Integer) a[i++];
            List<ElasticsearchException> failures = (List<ElasticsearchException>) a[i++];
            return new NodesResponseHeader(total, successful, failed, failures);
        }
    );

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), TOTAL);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), SUCCESSFUL);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), FAILED);
        PARSER.declareObjectArray(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> ElasticsearchException.fromXContent(p),
            FAILURES
        );
    }

    private final int total;
    private final int successful;
    private final int failed;
    private final List<ElasticsearchException> failures;

    public NodesResponseHeader(int total, int successful, int failed, @Nullable List<ElasticsearchException> failures) {
        this.total = total;
        this.successful = successful;
        this.failed = failed;
        this.failures = failures == null ? Collections.emptyList() : failures;
    }

    public static NodesResponseHeader fromXContent(XContentParser parser, Void context) throws IOException {
        return PARSER.parse(parser, context);
    }

    /** the total number of nodes that the operation was carried on */
    public int getTotal() {
        return total;
    }

    /** the number of nodes that the operation has failed on */
    public int getFailed() {
        return failed;
    }

    /** the number of nodes that the operation was successful on */
    public int getSuccessful() {
        return successful;
    }

    /**
     * Get the failed node exceptions.
     *
     * @return Never {@code null}. Can be empty.
     */
    public List<ElasticsearchException> getFailures() {
        return failures;
    }

    /**
     * Determine if there are any node failures in {@link #failures}.
     *
     * @return {@code true} if {@link #failures} contains at least 1 exception.
     */
    public boolean hasFailures() {
        return failures.isEmpty() == false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodesResponseHeader that = (NodesResponseHeader) o;
        return total == that.total && successful == that.successful && failed == that.failed && Objects.equals(failures, that.failures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(total, successful, failed, failures);
    }

}
