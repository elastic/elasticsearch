/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Base class for autoscaling tier metrics
 */
public abstract class AbstractBaseTierMetrics implements Writeable, ToXContentObject {

    protected final String reason;
    protected final ElasticsearchException exception;

    public AbstractBaseTierMetrics() {
        this.reason = null;
        this.exception = null;
    }

    public AbstractBaseTierMetrics(String reason, ElasticsearchException exception) {
        this.reason = Objects.requireNonNull(reason, "[reason] must not be null.");
        this.exception = exception;
    }

    public AbstractBaseTierMetrics(StreamInput in) throws IOException {
        reason = in.readOptionalString();
        exception = in.readOptionalWriteable(ElasticsearchException::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(reason);
        out.writeOptionalWriteable(exception);
    }

    protected abstract XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException;

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (Strings.isNullOrEmpty(reason) == false) {
            builder.startObject("failure");
            builder.field("reason", reason);
            if (exception != null) {
                ElasticsearchException.generateFailureXContent(builder, params, exception, true);
            }
            builder.endObject();
        } else {
            // only allowed if no error occurred
            toInnerXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

}
